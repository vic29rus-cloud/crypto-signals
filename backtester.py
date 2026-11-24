#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Бэктестер для стратегии AMD + Sweep.
Прогоняет историю по всем доступным парам и ищет лучшие комбинации фильтров.
"""

import os
import json
import time
import requests
import pandas as pd
import numpy as np
import itertools

BASE_URL = "https://api.bybit.com"
SESSION = requests.Session()

# ==== НАСТРОЙКИ БЭКТЕСТА ====
TOP_N = 400                 # Берем максимум пар
MIN_TURNOVER_USDT = 100_000 # Снижено до $100k
MIN_PRICE = 0.00001
TIMEFRAME = "15"
CANDLE_LIMIT = 1000         # ~10 дней истории (максимум за 1 запрос)

# Параметры стратегии
AMD_ACCUM_LOOKBACK = 20
AMD_ACCUM_RANGE_ATR_MULT = 4.0
AMD_SWEEP_PENETRATION_ATR = 0.3
AMD_SWEEP_LOOKBACK = 10

SWEEP_STOP_BUFFER_ATR = 0.5
ATR_MULT_TP = 8.0
MIN_RR = 2.0

# Что перебираем
TEST_CVD = [True, False]
TEST_RSI = [(35, 72), (40, 80), (30, 80)]
TEST_ADX = [(15, 55), (20, 60), (10, 60)]
TEST_VOL = [1.0, 1.1, 1.2]
TEST_TREND_SCORE = [0, 1, 2]

# ==== API BYBIT ====
def api_get(path, params, retries=3):
    url = BASE_URL + path
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            j = r.json()
            if j.get("retCode") == 0:
                return j
        except Exception:
            time.sleep(1)
    return None

def fetch_klines(symbol, interval, limit):
    params = {"category": "spot", "symbol": symbol,
              "interval": interval, "limit": min(limit, 1000)}
    j = api_get("/v5/market/kline", params)
    if not j:
        return None
    rows = j.get("result", {}).get("list", [])
    if not rows:
        return None
    out = []
    for x in reversed(rows):
        try:
            out.append({
                "start": int(x[0]),
                "open": float(x[1]), "high": float(x[2]),
                "low": float(x[3]), "close": float(x[4]),
                "volume": float(x[5]),
            })
        except Exception:
            continue
    return pd.DataFrame(out) if out else None

def get_universe():
    j = api_get("/v5/market/tickers", {"category": "spot"})
    if not j:
        return []
    pairs = []
    for item in j.get("result", {}).get("list", []):
        symbol = item.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        try:
            price = float(item.get("lastPrice", 0))
            turnover = float(item.get("turnover24h", 0) or item.get("volume24h", 0))
        except Exception:
            continue
        if price >= MIN_PRICE and turnover >= MIN_TURNOVER_USDT:
            pairs.append((symbol, turnover))
    pairs.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in pairs[:TOP_N]]

# ==== ИНДИКАТОРЫ ====
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    tr = ranges.max(axis=1)
    return tr.rolling(period).mean()

def adx(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    tr = pd.concat([high - low, np.abs(high - close.shift()), np.abs(low - close.shift())], axis=1).max(axis=1)
    atr_val = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    return dx.rolling(period).mean()

def approx_cvd(df, lookback=20):
    if df is None or len(df) < lookback + 2:
        return 0.0
    d = df.tail(lookback).copy()
    body = d["close"] - d["open"]
    rng = (d["high"] - d["low"]).replace(0, np.nan)
    ratio = (body / rng).fillna(0.0).clip(-1, 1)
    delta = d["volume"] * ratio
    return float(delta.sum())

# ==== AMD / SWEEP ====
def detect_accumulation(df):
    if df is None or len(df) < AMD_ACCUM_LOOKBACK + 5:
        return None
    window = df.tail(AMD_ACCUM_LOOKBACK)
    high = float(window["high"].max())
    low = float(window["low"].min())
    close = float(window["close"].iloc[-1])
    if close <= 0 or high <= low:
        return None
    atr_val = float(atr(df).iloc[-1])
    if pd.isna(atr_val) or atr_val <= 0:
        return None
    range_atr_mult = (high - low) / atr_val
    if range_atr_mult > AMD_ACCUM_RANGE_ATR_MULT:
        return None
    return {"upper": high, "lower": low}

def detect_sweep(df, accum):
    if df is None or accum is None or len(df) < AMD_SWEEP_LOOKBACK + 2:
        return None
    atr_val = float(atr(df).iloc[-1])
    if pd.isna(atr_val) or atr_val <= 0:
        return None
    pen = AMD_SWEEP_PENETRATION_ATR * atr_val
    recent = df.tail(AMD_SWEEP_LOOKBACK + 1).iloc[:-1]
    for i in range(len(recent) - 1, -1, -1):
        row = recent.iloc[i]
        if row["low"] < accum["lower"] - pen and row["close"] > accum["lower"]:
            return {"sweep_low": float(row["low"])}
    return None

# ==== БЭКТЕСТ ОДНОЙ КОМБИНАЦИИ ====
def backtest_combo(pairs_data, cvd_enabled, rsi_range, adx_range, vol_mult, trend_score_min):
    total_trades = 0
    wins = 0
    total_pnl = 0.0
    
    for symbol, df in pairs_data.items():
        if df is None or len(df) < 100:
            continue
        
        df["atr"] = atr(df)
        df["rsi"] = rsi(df["close"])
        df["adx"] = adx(df)
        df["vol_sma"] = df["volume"].rolling(20).mean()
        df["vol_ratio"] = df["volume"] / df["vol_sma"].replace(0, np.nan)
        df["ema9"] = ema(df["close"], 9)
        df["ema21"] = ema(df["close"], 21)
        
        for i in range(100, len(df) - 5):
            past = df.iloc[:i+1].copy()
            accum = detect_accumulation(past)
            if not accum: continue
            sweep = detect_sweep(past, accum)
            if not sweep: continue
            
            row = past.iloc[-1]
            if pd.isna(row["atr"]) or row["atr"] <= 0: continue
            entry = float(row["close"])
            if entry <= 0: continue
            
            if not (rsi_range[0] <= row["rsi"] <= rsi_range[1]): continue
            if not (adx_range[0] <= row["adx"] <= adx_range[1]): continue
            if row["vol_ratio"] < vol_mult: continue
            
            if cvd_enabled:
                cvd_val = approx_cvd(past)
                if cvd_val <= 0: continue
            
            bull_4h = row["ema9"] > row["ema21"]
            t_score = 1 if bull_4h else 0
            if t_score < trend_score_min: continue
            
            stop = sweep["sweep_low"] - SWEEP_STOP_BUFFER_ATR * row["atr"]
            if stop >= entry: continue
            risk = entry - stop
            risk_pct = risk / entry * 100
            if risk_pct < 1.0: continue
            target = entry + risk * (ATR_MULT_TP / SWEEP_STOP_BUFFER_ATR)
            
            result = None
            for j in range(i+1, min(i+6, len(df))):
                future = df.iloc[j]
                if future["low"] <= stop:
                    result = -1.0
                    break
                if future["high"] >= target:
                    result = (target - entry) / risk
                    break
            if result is None: continue
            
            total_trades += 1
            total_pnl += result
            if result > 0: wins += 1
    
    if total_trades == 0: return None
    winrate = wins / total_trades * 100
    profit_factor = total_pnl / abs(total_trades - wins) if total_trades > wins else 999
    return {
        "trades": total_trades,
        "wins": wins,
        "winrate": winrate,
        "total_pnl_r": total_pnl,
        "profit_factor": profit_factor,
        "cvd": cvd_enabled,
        "rsi": rsi_range,
        "adx": adx_range,
        "vol": vol_mult,
        "trend_min": trend_score_min,
    }

# ==== ЗАПУСК ====
def main():
    print("🚀 Запуск бэктестера...")
    pairs = get_universe()
    print(f"📊 Загружено пар: {len(pairs)}")
    
    print(f"⏳ Скачиваем историю (по {CANDLE_LIMIT} свечей)...")
    pairs_data = {}
    for idx, symbol in enumerate(pairs):
        df = fetch_klines(symbol, TIMEFRAME, CANDLE_LIMIT)
        if df is not None and len(df) > 100:
            pairs_data[symbol] = df
        if idx % 50 == 0:
            print(f"   [{idx}/{len(pairs)}] Скачано...")
        time.sleep(0.05)
    
    print(f"✅ Готово. Пар для анализа: {len(pairs_data)}")
    
    results = []
    combos = list(itertools.product(TEST_CVD, TEST_RSI, TEST_ADX, TEST_VOL, TEST_TREND_SCORE))
    print(f"🔄 Перебираем {len(combos)} комбинаций...")
    
    for idx, (cvd, rsi_r, adx_r, vol, t_min) in enumerate(combos):
        res = backtest_combo(pairs_data, cvd, rsi_r, adx_r, vol, t_min)
        if res and res["trades"] >= 5:
            results.append(res)
        if (idx + 1) % 20 == 0:
            print(f"   [{idx+1}/{len(combos)}]...")
    
    results.sort(key=lambda x: (x["profit_factor"], x["total_pnl_r"]), reverse=True)
    
    print("\n" + "="*80)
    print("🏆 ТОП-10 ЛУЧШИХ КОМБИНАЦИЙ (по Profit Factor):")
    print("="*80)
    for i, r in enumerate(results[:10], 1):
        print(f"\n#{i} | PF: {r['profit_factor']:.2f} | Winrate: {r['winrate']:.1f}% "
              f"| Сделок: {r['trades']} | PnL: {r['total_pnl_r']:.1f}R")
        print(f"     CVD: {r['cvd']} | RSI: {r['rsi']} | ADX: {r['adx']} "
              f"| Vol: {r['vol']} | Trend: {r['trend_min']}")
    
    with open("backtest_results.json", "w", encoding="utf-8") as f:
        json.dump(results[:50], f, indent=2, default=str)
    
    print("\n💾 Результаты сохранены в backtest_results.json")
    print("✅ Готово!")

if __name__ == "__main__":
    main()
