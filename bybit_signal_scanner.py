"""
Сканер сигналов по ВСЕМ ликвидным SPOT-парам Bybit.
Использует официальный публичный API Bybit v5 (ключ не нужен для рыночных данных).

УСТАНОВКА:
    pip install requests pandas numpy --break-system-packages

ЗАПУСК ВРУЧНУЮ (тест):
    python bybit_signal_scanner.py --timeframe 1d --top-n 100
"""

import argparse
import json
import os
import time
import requests
import pandas as pd
import numpy as np

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Bybit interval-коды: 1,3,5,15,30,60,120,240,360,720,D,M,W
TIMEFRAME_PARAMS = {
    "4h": {"bybit_interval": "240", "ema_fast": 21, "ema_slow": 55,  "atr_mult_sl": 1.5, "atr_mult_tp": 3.0, "min_bars": 120},
    "1d": {"bybit_interval": "D",   "ema_fast": 50, "ema_slow": 200, "atr_mult_sl": 2.0, "atr_mult_tp": 4.0, "min_bars": 220},
    "1w": {"bybit_interval": "W",   "ema_fast": 10, "ema_slow": 30,  "atr_mult_sl": 2.5, "atr_mult_tp": 5.0, "min_bars": 60},
}

BASE_URL = "https://bybit.nl"
STATE_FILE = "bybit_scanner_state.json"

EXCLUDE_BASE_SUBSTRINGS = ["UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S"]
STABLECOINS = {"USDC", "BUSD", "TUSD", "FDUSD", "DAI", "USDP", "PYUSD", "USDT", "EUR"}


def get_top_pairs(quote_coin: str, top_n: int) -> list:
    """Топ-N спот-пар Bybit по 24ч обороту в USDT (turnover24h)."""
    instr_resp = requests.get(
        f"{BASE_URL}/v5/market/instruments-info",
        params={"category": "spot"}, timeout=20
    )
    instr_resp.raise_for_status()
    instr_data = instr_resp.json()["result"]["list"]

    tradable = {
        i["symbol"] for i in instr_data
        if i["status"] == "Trading" and i["quoteCoin"] == quote_coin
    }

    tick_resp = requests.get(
        f"{BASE_URL}/v5/market/tickers",
        params={"category": "spot"}, timeout=20
    )
    tick_resp.raise_for_status()
    tickers = tick_resp.json()["result"]["list"]

    candidates = []
    for t in tickers:
        symbol = t["symbol"]
        if symbol not in tradable:
            continue
        base = symbol[:-len(quote_coin)] if symbol.endswith(quote_coin) else None
        if base is None:
            continue
        if any(x in base for x in EXCLUDE_BASE_SUBSTRINGS):
            continue
        if base in STABLECOINS:
            continue
        try:
            turnover = float(t.get("turnover24h", 0))
        except (TypeError, ValueError):
            turnover = 0.0
        candidates.append((symbol, turnover))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return [c[0] for c in candidates[:top_n]]


def fetch_klines(symbol: str, interval: str, min_bars: int) -> pd.DataFrame:
    """
    Bybit отдаёт максимум 200 свечей за запрос, поэтому пагинируем через end=.
    Возвращаем DataFrame отсортированный по времени по возрастанию.
    """
    all_rows = []
    end_ts = None
    remaining = min_bars

    while remaining > 0:
        params = {"category": "spot", "symbol": symbol, "interval": interval, "limit": min(200, remaining)}
        if end_ts:
            params["end"] = end_ts
        resp = requests.get(f"{BASE_URL}/v5/market/kline", params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {payload.get('retMsg')}")
        rows = payload["result"]["list"]  # новые сначала
        if not rows:
            break
        all_rows.extend(rows)
        oldest_start = int(rows[-1][0])
        end_ts = oldest_start - 1
        remaining -= len(rows)
        if len(rows) < 200:
            break  # больше истории нет

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=["start", "open", "high", "low", "close", "volume", "turnover"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df["start"] = df["start"].astype(np.int64)
    df = df.drop_duplicates(subset="start").sort_values("start").reset_index(drop=True)
    return df


def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()


def macd(series, fast=12, slow=26, signal=9):
    macd_line = ema(series, fast) - ema(series, slow)
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line


def atr(df, length=14):
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low, (high - prev_close).abs(), (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(length).mean()


def analyze(df: pd.DataFrame, params: dict) -> dict:
    if df.empty or len(df) < params["min_bars"]:
        return None

    df = df.copy()
    df["ema_fast"] = ema(df["close"], params["ema_fast"])
    df["ema_slow"] = ema(df["close"], params["ema_slow"])
    df["macd_line"], df["macd_signal"] = macd(df["close"])
    df["atr"] = atr(df)

    last = df.iloc[-2]   # последняя ЗАКРЫТАЯ свеча
    prev = df.iloc[-3]

    trend_up = last["ema_fast"] > last["ema_slow"]
    macd_cross_up = (prev["macd_line"] <= prev["macd_signal"]) and (last["macd_line"] > last["macd_signal"])
    macd_cross_down = (prev["macd_line"] >= prev["macd_signal"]) and (last["macd_line"] < last["macd_signal"])
    ema_cross_down = (prev["ema_fast"] >= prev["ema_slow"]) and (last["ema_fast"] < last["ema_slow"])

    return {
        "buy": bool(trend_up and macd_cross_up),
        "signal_exit": bool(ema_cross_down or macd_cross_down),
        "close": float(last["close"]),
        "atr": float(last["atr"]) if not np.isnan(last["atr"]) else 0.0,
        "bar_time": str(int(last["start"])),
    }


def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[NO TELEGRAM CONFIG]", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=15)
    r.raise_for_status()


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeframe", choices=list(TIMEFRAME_PARAMS.keys()), default="1d")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--quote-coin", default="USDT")
    parser.add_argument("--request-delay", type=float, default=0.15)
    args = parser.parse_args()

    params = TIMEFRAME_PARAMS[args.timeframe]
    state = load_state()

    pairs = get_top_pairs(args.quote_coin, args.top_n)
    print(f"Сканирую {len(pairs)} пар Bybit Spot на {args.timeframe}...")

    found_buy, found_sell = 0, 0

    for symbol in pairs:
        key = f"{symbol}_{args.timeframe}"
        try:
            df = fetch_klines(symbol, params["bybit_interval"], params["min_bars"] + 5)
            result = analyze(df, params)
        except Exception as e:
            print(f"[{symbol}] ошибка получения данных: {e}")
            time.sleep(args.request_delay)
            continue

        time.sleep(args.request_delay)

        if result is None:
            continue

        pos = state.get(key, {"position": "closed", "last_bar_processed": None})

        if pos.get("last_bar_processed") == result["bar_time"]:
            continue

        if pos["position"] == "closed" and result["buy"]:
            stop = result["close"] - result["atr"] * params["atr_mult_sl"]
            target = result["close"] + result["atr"] * params["atr_mult_tp"]
            text = (
                f"🟢 <b>ВХОД (BUY)</b>\n"
                f"Биржа: Bybit Spot\n"
                f"Пара: <b>{symbol}</b>\n"
                f"Таймфрейм: <b>{args.timeframe}</b>\n"
                f"Цена входа: <b>{result['close']:.6g}</b>\n"
                f"Stop-Loss: {stop:.6g}\n"
                f"Take-Profit: {target:.6g}"
            )
            send_telegram(text)
            print(text)
            state[key] = {
                "position": "open",
                "entry_price": result["close"],
                "stop": stop,
                "target": target,
                "last_bar_processed": result["bar_time"],
            }
            found_buy += 1

        elif pos["position"] == "open":
            hit_stop = result["close"] <= pos["stop"]
            hit_target = result["close"] >= pos["target"]
            exit_now = result["signal_exit"] or hit_stop or hit_target

            if exit_now:
                entry = pos["entry_price"]
                pnl_pct = (result["close"] - entry) / entry * 100
                reason = "Take-Profit" if hit_target else ("Stop-Loss" if hit_stop else "Сигнал разворота")
                text = (
                    f"🔴 <b>ВЫХОД (SELL)</b>\n"
                    f"Биржа: Bybit Spot\n"
                    f"Пара: <b>{symbol}</b>\n"
                    f"Таймфрейм: <b>{args.timeframe}</b>\n"
                    f"Цена выхода: <b>{result['close']:.6g}</b>\n"
                    f"Причина: {reason}\n"
                    f"Результат: <b>{pnl_pct:+.2f}%</b>"
                )
                send_telegram(text)
                print(text)
                state[key] = {"position": "closed", "last_bar_processed": result["bar_time"]}
                found_sell += 1
            else:
                pos["last_bar_processed"] = result["bar_time"]
                state[key] = pos
        else:
            state[key] = {"position": "closed", "last_bar_processed": result["bar_time"]}

    save_state(state)
    print(f"Готово. Новых входов: {found_buy}, выходов: {found_sell}")


if __name__ == "__main__":
    main()
