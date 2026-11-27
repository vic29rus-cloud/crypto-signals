#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Финальный бэктестер-оптимизатор для ШОРТОВ (Sweep Highs).
Перебирает стопы, тейки и время удержания.
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

# Будем перебирать эти параметры!
TEST_STOP_ATR = [1.0, 1.5, 2.0, 2.5]
TEST_TP_RR = [1.0, 1.5, 2.0, 2.5, 3.0]
TEST_HOLD_CANDLES = [10, 20, 40]

TEST_CVD = [False, True]
TEST_RSI = [(30, 80), (40, 80), (30, 90)]
TEST_ADX = [(10, 60), (15, 55), (20, 60)]
TEST_VOL = [1.0, 1.1, 1.2]
TEST_TREND_SCORE = [1]

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
        if not symbol.endswith("USDT"): continue
        try:
            price = float(item.get("lastPrice", 0))
            turnover = float(item.get("turnover24h", 0) or item.get("volume24h", 0))
        except Exception: continue
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
    tr = pd.concat([high - low, np.abs(high - close.shift()), np.abs(low - close.shift())], axis=1).max(axis=1)
    atr_val = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    return dx.rolling(period).mean()

def main():
    print("🚀 Запуск оптимизатора ШОРТОВ (поиск лучшего стопа и тейка)...")
    pairs = get_universe()
    
    pairs_data = {}
    for idx, symbol in enumerate(pairs):
        df = fetch_klines(symbol, TIMEFRAME, CANDLE_LIMIT)
        if df is not None and len(df) > 100:
            pairs_data[symbol] = df
        if idx % 50 == 0:
            print(f"   [{idx}/{len(pairs)}] Скачано...")
        time.sleep(0.12)
    
    print(f"✅ Пар: {len(pairs_data)}. Собираем сырые сетапы...")
    
    # ЭТАП 1: Сбор сетапов с сохранением будущих свечей для симуляции
    raw_signals = []
    max_future = max(TEST_HOLD_CANDLES)
    
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
        
        df["high_20_past"] = df["high"].shift(10).rolling(20).max()
        df["low_20_past"] = df["low"].shift(10).rolling(20).min()
        df["range_atr"] = (df["high_20_past"] - df["low_20_past"]) / df["atr"]
        
        for i in range(40, len(df) - max_future):
            row = df.iloc[i]
            if pd.isna(row["atr"]) or row["atr"] <= 0 or pd.isna(row["high_20_past"]): continue
            if row["range_atr"] > AMD_ACCUM_RANGE_ATR_MULT: continue
                
            past_10 = df.iloc[i-9 : i+1]
            max_high = past_10["high"].max()
            pen = AMD_SWEEP_PENETRATION_ATR * row["atr"]
            
            if max_high > row["high_20_past"] + pen and row["close"] < row["high_20_past"]:
                sweep_high = max_high
            else:
                continue
            
            entry = row["close"]
            
            # Сохраняем сырые данные
            future = df.iloc[i+1 : i+1+max_future][['high', 'low', 'close']].to_dict('records')
            raw_signals.append({
                "entry": entry,
                "sweep_high": sweep_high,
                "atr": row["atr"],
                "rsi": row["rsi"],
                "adx": row["adx"],
                "vol_ratio": row["vol_ratio"],
                "cvd": row["delta"],
                "trend": 1 if row["ema9"] < row["ema21"] else 0,
                "future": future
            })
    
    print(f"🎯 Найдено сырых сетапов: {len(raw_signals)}")
    
    print("🔄 Этап 2: Перебор фильтров и параметров риска...")
    results = []
    
    # Перебираем фильтры
    combos = list(itertools.product(TEST_CVD, TEST_RSI, TEST_ADX, TEST_VOL, TEST_TREND_SCORE))
    
    for cvd, rsi_r, adx_r, vol, t_min in combos:
        # Отфильтровываем сетапы по индикаторам (чтобы не гонять их каждый раз)
        filtered_signals = []
        for sig in raw_signals:
            if cvd and sig["cvd"] >= 0: continue
            if not (rsi_r[0] <= sig["rsi"] <= rsi_r[1]): continue
            if not (adx_r[0] <= sig["adx"] <= adx_r[1]): continue
            if sig["vol_ratio"] < vol: continue
            if sig["trend"] < t_min: continue
            filtered_signals.append(sig)
            
        if len(filtered_signals) < 10: continue
        
        # Для каждого набора фильтров перебираем параметры риска
        for stop_atr in TEST_STOP_ATR:
            for tp_rr in TEST_TP_RR:
                for max_hold in TEST_HOLD_CANDLES:
                    
                    trades = 0; wins = 0; pnl = 0.0; gp = 0.0; gl = 0.0
                    
                    for sig in filtered_signals:
                        entry = sig["entry"]
                        stop = sig["sweep_high"] + stop_atr * sig["atr"]
                        risk = stop - entry
                        if risk <= 0: continue
                        target = entry - risk * tp_rr
                        
                        result = None
                        for j in range(min(max_hold, len(sig["future"]))):
                            c = sig["future"][j]
                            if c["high"] >= stop: result = -1.0; break
                            if c["low"] <= target: result = tp_rr; break
                        
                        if result is None:
                            exit_price = sig["future"][min(max_hold-1, len(sig["future"])-1)]["close"]
                            result = (entry - exit_price) / risk
                            
                        trades += 1; pnl += result
                        if result > 0: wins += 1; gp += result
                        else: gl += abs(result)
                        
                    if trades >= 20:  # Требуем минимум 20 сделок для надежности
                        winrate = wins / trades * 100
                        pf = gp / gl if gl > 0 else 999.0
                        results.append({
                            "trades": trades, "winrate": winrate, "total_pnl_r": pnl, "pf": pf,
                            "cvd": cvd, "rsi": rsi_r, "adx": adx_r, "vol": vol, "t_min": t_min,
                            "stop_atr": stop_atr, "tp_rr": tp_rr, "max_hold": max_hold
                        })
    
    # Сортируем по Profit Factor
    results.sort(key=lambda x: (x["pf"], x["total_pnl_r"]), reverse=True)
    
    print("\n" + "="*80)
    print("🏆 ТОП-10 ЛУЧШИХ КОМБИНАЦИЙ (ШОРТ):")
    print("="*80)
    
    for i, r in enumerate(results[:10], 1):
        print(f"\n#{i} | PF: {r['pf']:.2f} | WR: {r['winrate']:.1f}% | Сделок: {r['trades']} | PnL: {r['total_pnl_r']:.1f}R")
        print(f"     Стоп: {r['stop_atr']} ATR | Тейк: {r['tp_rr']} R | Держать: {r['max_hold']} свечей")
        print(f"     CVD: {r['cvd']} | RSI: {r['rsi']} | ADX: {r['adx']} | Vol: {r['vol']} | Trend: {r['t_min']}")
        
    with open("backtest_results.json", "w", encoding="utf-8") as f:
        json.dump(results[:50], f, indent=2)
    print("\n✅ Результаты сохранены.")

if __name__ == "__main__":
    main()
