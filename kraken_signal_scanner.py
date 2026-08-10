"""
Сканер сигналов по ликвидным SPOT-парам Kraken (котировка в USD).
Использует официальный публичный API Kraken (ключ не нужен для рыночных данных).
Kraken официально доступен пользователям США (в отличие от Bybit).

УСТАНОВКА:
    pip install requests pandas numpy --break-system-packages

ЗАПУСК ВРУЧНУЮ (тест):
    python kraken_signal_scanner.py --timeframe 1d --top-n 100
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

# Kraken interval задаётся в минутах: 1,5,15,30,60,240,1440,10080,21600
TIMEFRAME_PARAMS = {
    "4h": {"kraken_interval": 240,   "ema_fast": 21, "ema_slow": 55,  "atr_mult_sl": 1.5, "atr_mult_tp": 3.0, "min_bars": 120},
    "1d": {"kraken_interval": 1440,  "ema_fast": 50, "ema_slow": 200, "atr_mult_sl": 2.0, "atr_mult_tp": 4.0, "min_bars": 220},
    "1w": {"kraken_interval": 10080, "ema_fast": 10, "ema_slow": 30,  "atr_mult_sl": 2.5, "atr_mult_tp": 5.0, "min_bars": 60},
}

BASE_URL = "https://api.kraken.com/0/public"
STATE_FILE = "kraken_scanner_state.json"

EXCLUDE_BASE_SUBSTRINGS = ["UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S"]
STABLECOINS = {"USDC", "USDT", "DAI", "USD", "EUR", "GBP", "PYUSD", "TUSD", "FDUSD"}


def get_top_pairs(quote_coin: str, top_n: int) -> list:
    """Топ-N спот-пар Kraken по 24ч обороту (в quote_coin), отсортированных по убыванию."""
    pairs_resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
    pairs_resp.raise_for_status()
    pairs_data = pairs_resp.json()
    if pairs_data.get("error"):
        raise RuntimeError(f"Kraken API error: {pairs_data['error']}")

    all_pairs = pairs_data["result"]

    candidates_names = []
    for kraken_name, info in all_pairs.items():
        wsname = info.get("wsname", "")
        if "/" not in wsname:
            continue
        base, quote = wsname.split("/")
        if quote != quote_coin:
            continue
        if any(x in base for x in EXCLUDE_BASE_SUBSTRINGS):
            continue
        if base in STABLECOINS:
            continue
        candidates_names.append(kraken_name)

    # Kraken Ticker принимает список пар через запятую, но с ограничением длины URL — бьём на чанки
    volumes = {}
    chunk_size = 50
    for i in range(0, len(candidates_names), chunk_size):
        chunk = candidates_names[i:i + chunk_size]
        tick_resp = requests.get(
            f"{BASE_URL}/Ticker", params={"pair": ",".join(chunk)}, timeout=20
        )
        tick_resp.raise_for_status()
        tick_data = tick_resp.json()
        if tick_data.get("error"):
            time.sleep(0.3)
            continue
        for pair_name, t in tick_data.get("result", {}).items():
            try:
                vwap_24h = float(t["p"][1])
                vol_24h = float(t["v"][1])
                volumes[pair_name] = vwap_24h * vol_24h
            except (KeyError, ValueError, TypeError):
                volumes[pair_name] = 0.0
        time.sleep(0.3)

    sorted_pairs = sorted(volumes.items(), key=lambda x: x[1], reverse=True)
    return [p[0] for p in sorted_pairs[:top_n]]


def fetch_klines(pair: str, interval_minutes: int, min_bars: int) -> pd.DataFrame:
    """
    Kraken отдаёт до 720 последних свечей за один запрос — этого хватает
    для всех наших таймфреймов без дополнительной пагинации.
    """
    resp = requests.get(
        f"{BASE_URL}/OHLC", params={"pair": pair, "interval": interval_minutes}, timeout=20
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("error"):
        raise RuntimeError(f"Kraken API error: {payload['error']}")

    result = payload["result"]
    # Ответ содержит ключ с реальным именем пары (может отличаться от запрошенного) + служебный "last"
    rows = None
    for key, val in result.items():
        if key != "last":
            rows = val
            break
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["start", "open", "high", "low", "close", "vwap", "volume", "count"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df["start"] = df["start"].astype(np.int64)
    df = df.drop_duplicates(subset="start").sort_values("start").reset_index(drop=True)
    return df.tail(min_bars + 10).reset_index(drop=True)


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

    last = df.iloc[-2]   # последняя ЗАКРЫТАЯ свеча (последняя строка Kraken — текущая незакрытая)
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
    parser.add_argument("--quote-coin", default="USD")
    parser.add_argument("--request-delay", type=float, default=0.3)
    args = parser.parse_args()

    params = TIMEFRAME_PARAMS[args.timeframe]
    state = load_state()

    pairs = get_top_pairs(args.quote_coin, args.top_n)
    print(f"Сканирую {len(pairs)} пар Kraken Spot на {args.timeframe}...")

    found_buy, found_sell = 0, 0

    for pair in pairs:
        key = f"{pair}_{args.timeframe}"
        try:
            df = fetch_klines(pair, params["kraken_interval"], params["min_bars"] + 5)
            result = analyze(df, params)
        except Exception as e:
            print(f"[{pair}] ошибка получения данных: {e}")
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
                f"Биржа: Kraken Spot\n"
                f"Пара: <b>{pair}</b>\n"
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
                    f"Биржа: Kraken Spot\n"
                    f"Пара: <b>{pair}</b>\n"
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
