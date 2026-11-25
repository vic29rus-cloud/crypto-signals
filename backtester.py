#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Бэктестер для стратегии AMD + Sweep (ИСПРАВЛЕННАЯ ЛОГИКА).
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

# ==== НАСТРОЙКИ ====
TOP_N = 400
MIN_TURNOVER_USDT = 100_000
MIN_PRICE = 0.00001
TIMEFRAME = "15"
CANDLE_LIMIT = 1000

AMD_ACCUM_LOOKBACK = 20
AMD_ACCUM_RANGE_ATR_MULT = 4.0
AMD_SWEEP_PENETRATION_ATR = 0.3
AMD_SWEEP_LOOKBACK = 10
SWEEP_STOP_BUFFER_ATR = 0.5
ATR_MULT_TP = 8.0

# Что перебираем
TEST_CVD = [True, False]
TEST_RSI = [(35, 72), (40, 80), (30, 80)]
TEST_ADX = [(15, 55), (20, 60), (10, 60)]
TEST_VOL = [1.0, 1.1, 1.2]
TEST_TREND_SCORE = [0, 1, 2]

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
    params = {"category": "spot", "symbol": symbol, "interval": interval, "limit": min(limit, 1000)}
    j = api_get("/v5/market/kline", params)
    if not j: return None
    rows = j.get("result", {}).get("list", [])
    if not rows: return None
    out = []
    for x in reversed(rows):
        try:
            out.append({"start": int(x[0]), "open": float(x[1]), "high": float(x[2]),
                        "low": float(x[3]), "close": float(x[4]), "volume": float(x[5])})
        except Exception:
            continue
    return pd.DataFrame(out) if out else None

def get_universe():
    j = api_get("/v5/market/tickers", {"category": "spot"})
    if not j: return []
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

def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def atr(df, period=14):
    tr = pd.concat([df["high"] - df["low"], 
                    np.abs(df["high"] - df["close"].shift()), 
                    np.abs(df["low"] - df["close"].shift())], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def adx(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm = high.diff().where((high.diff() > -low.diff()) & (high.diff() > 0), 0.0)
    minus_dm = -low.diff().where((-low.diff() > high.diff()) & (-low.diff() > 0), 0.0)
    tr = pd.concat([high - low, 
                    np.abs(high - close.shift()), 
                    np.abs(low - close.shift())], axis=1).max(axis=1)
    atr_val = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    return dx.rolling(period).mean()

def main():
    print("🚀 Запуск ИСПРАВЛЕННОГО бэктестера (фикс логики Sweep)...")
    pairs = get_universe()
    print(f"📊 Загружено пар: {len(pairs)}")
    
    pairs_data = {}
    for idx, symbol in enumerate(pairs):
        df = fetch_klines(symbol, TIMEFRAME, CANDLE_LIMIT)
        if df is not None and len(df) > 100:
            pairs_data[symbol] = df
        if idx % 50 == 0:
            print(f"   [{idx}/{len(pairs)}] Скачано...")
        time.sleep(0.12)
    
    print(f"✅ Скачивание завершено. Пар: {len(pairs_data)}")
    
    print("🔍 Этап 1: Поиск всех сетапов AMD+Sweep...")
    all_signals = []
    
    for symbol, df in pairs_data.items():
        df = df.copy()
        df["atr"] = atr(df)
        df["rsi"] = rsi(df["close"])
        df["adx"] = adx(df)
        df["vol_sma"] = df["volume"].rolling(20).mean()
        df["vol_ratio"] = df["volume"] / df["vol_sma"]
        df["ema9"] = ema(df["close"], 9)
        df["ema21"] = ema(df["close"], 21)
        
        body = df["close"] - df["open"]
        rng = (df["high"] - df["low"]).replace(0, np.nan)
        df["delta"] = (df["volume"] * (body / rng).fillna(0).clip(-1, 1)).rolling(20).sum()
        
        # ИСПРАВЛЕНИЕ: Боковик считаем за 20 свечей, которые закончились 10 свечей назад!
        df["high_20_past"] = df["high"].shift(10).rolling(20).max()
        df["low_20_past"] = df["low"].shift(10).rolling(20).min()
        df["range_atr"] = (df["high_20_past"] - df["low_20_past"]) / df["atr"]
        
        for i in range(40, len(df) - 5):
            row = df.iloc[i]
            if pd.isna(row["atr"]) or row["atr"] <= 0 or pd.isna(row["low_20_past"]):
                continue
            
            # 1. Проверка накопления (диапазон ДО прокола)
            if row["range_atr"] > AMD_ACCUM_RANGE_ATR_MULT:
                continue
                
            # 2. Проверка Sweep в последних 10 свечах
            past_10 = df.iloc[i-9 : i+1]
            min_low = past_10["low"].min()
            pen = AMD_SWEEP_PENETRATION_ATR * row["atr"]
            
            # Если пробили нижнюю границу накопления и закрылись ВЫШЕ неё
            if min_low < row["low_20_past"] - pen and row["close"] > row["low_20_past"]:
                sweep_low = min_low
            else:
                continue
            
            # Симуляция сделки
            entry = row["close"]
            stop = sweep_low - SWEEP_STOP_BUFFER_ATR * row["atr"]
            if stop >= entry:
                continue
            risk = entry - stop
            if (risk / entry * 100) < 1.0:
                continue
            target = entry + risk * (ATR_MULT_TP / SWEEP_STOP_BUFFER_ATR)
            
            result = None
            for j in range(i+1, min(i+6, len(df))):
                if df.iloc[j]["low"] <= stop:
                    result = -1.0
                    break
                if df.iloc[j]["high"] >= target:
                    result = (target - entry) / risk
                    break
            
            if result is not None:
                all_signals.append({
                    "symbol": symbol,
                    "rsi": float(row["rsi"]) if pd.notna(row["rsi"]) else 0.0,
                    "adx": float(row["adx"]) if pd.notna(row["adx"]) else 0.0,
                    "vol_ratio": float(row["vol_ratio"]) if pd.notna(row["vol_ratio"]) else 0.0,
                    "cvd": float(row["delta"]) if pd.notna(row["delta"]) else 0.0,
                    "trend": 1 if row["ema9"] > row["ema21"] else 0,
                    "result": float(result)
                })
    
    print(f"🎯 Найдено потенциальных сетапов: {len(all_signals)}")
    if not all_signals:
        print("❌ Все равно нет сетапов.")
        return
    
    print("🔄 Этап 2: Перебор комбинаций фильтров...")
    results = []
    combos = list(itertools.product(TEST_CVD, TEST_RSI, TEST_ADX, TEST_VOL, TEST_TREND_SCORE))
    
    for cvd, rsi_r, adx_r, vol, t_min in combos:
        trades = 0
        wins = 0
        pnl = 0.0
        for sig in all_signals:
            if cvd and sig["cvd"] <= 0: continue
            if not (rsi_r[0] <= sig["rsi"] <= rsi_r[1]): continue
            if not (adx_r[0] <= sig["adx"] <= adx_r[1]): continue
            if sig["vol_ratio"] < vol: continue
            if sig["trend"] < t_min: continue
            
            trades += 1
            pnl += sig["result"]
            if sig["result"] > 0: wins += 1
        
        if trades >= 5:
            winrate = wins / trades * 100
            pf = pnl / abs(trades - wins) if trades > wins else 999.0
            results.append({
                "trades": trades, "wins": wins, "winrate": winrate, "total_pnl_r": pnl,
                "profit_factor": pf, "cvd": cvd, "rsi": rsi_r, "adx": adx_r,
                "vol": vol, "trend_min": t_min
            })
    
    results.sort(key=lambda x: (x["profit_factor"], x["total_pnl_r"]), reverse=True)
    
    print("\n" + "="*80)
    print("🏆 ТОП-10 ЛУЧШИХ КОМБИНАЦИЙ:")
    print("="*80)
    
    for i, r in enumerate(results[:10], 1):
        print(f"\n#{i} | PF: {r['profit_factor']:.2f} | Winrate: {r['winrate']:.1f}% "
              f"| Сделок: {r['trades']} | PnL: {r['total_pnl_r']:.1f}R")
        print(f"     CVD: {r['cvd']} | RSI: {r['rsi']} | ADX: {r['adx']} "
              f"| Vol: {r['vol']} | Trend: {r['trend_min']}")
    
    with open("backtest_results.json", "w", encoding="utf-8") as f:
        json.dump(results[:50], f, indent=2)
    
    print("\n✅ Готово! Результаты сохранены в backtest_results.json")

if __name__ == "__main__":
    main()
