"""
Сканер сигналов Kraken Spot с мультитаймфреймовым подтверждением (confluence).

ЛОГИКА ВХОДА:
    Сигнал BUY отправляется только если ОДНОВРЕМЕННО:
      - восходящий тренд (EMA fast > EMA slowна 4h, 1d И 1w
      - свежее пересечение MACD вверх на 4h (триггер входа)
      - RSI(14) на 4h в диапазоне [RSI_MIN, RSI_MAX] — не перекуплен/перепродан
      - ADX(14) на 1d >= ADX_MIN — тренд достаточно силён, не "боковик"

ВЫХОД:
    По 4h: разворот EMA/MACD, либо срабатывание Stop-Loss/Take-Profit (считаются от ATR дневного графика).

ОТЧЁТНОСТЬ:
    --mode report --period 3d    -> сводка за последние 3 дня
    --mode report --period month -> сводка за последние 30 дней

ПАРЫ:
    Каждый запуск заново ищет top-N САМЫХ ВОЛАТИЛЬНЫХ пар (по 24ч диапазону high/low в %),
    с фильтром минимальной ликвидности — список не фиксирован, отслеживание "плавающее".

ВАЖНО (честно): ни один набор фильтров не гарантирует прибыль. Ужесточение условий
обычно снижает число сделок и долю ложных входов, но не устраняет просадки полностью.
Это не является финансовой рекомендацией — только сигнал по вашей собственной логике.

УСТАНОВКА:
    pip install requests pandas numpy --break-system-packages
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import numpy as np

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")  # опционально: личный чат-админ

SUBSCRIBERS_FILE = "telegram_subscribers.json"
STATE_FILE = "kraken_scanner_state.json"
TRADES_LOG_FILE = "trades_log.json"

WELCOME_TEXT = (
    "✅ Вы подписались на сигналы Kraken Scanner.\n"
    "Входы отправляются только при совпадении тренда на 4h/1d/1w."
)

BASE_URL = "https://api.kraken.com/0/public"

# Kraken interval в минутах: 1,5,15,30,60,240,1440,10080,21600
TIMEFRAME_PARAMS = {
    "4h": {"kraken_interval": 240,   "ema_fast": 21, "ema_slow": 55, "min_bars": 120},
    "1d": {"kraken_interval": 1440,  "ema_fast": 50, "ema_slow": 100, "min_bars": 150},
    "1w": {"kraken_interval": 10080, "ema_fast": 8,  "ema_slow": 20, "min_bars": 40},
}
TIMEFRAME_ORDER = ["4h", "1d", "1w"]
TRIGGER_TF = "4h"          # на этом ТФ ищем свежий MACD-кросс как триггер входа
ADX_REF_TF = "1d"          # на этом ТФ проверяем силу тренда

RSI_LENGTH = 14
RSI_MIN, RSI_MAX = 40, 75  # входим не на перекупленности и не на дне без импульса
ADX_LENGTH = 14
ADX_MIN = 20                # ниже — считаем рынок "боковиком", сигнал игнорируем

ATR_MULT_SL = 2.0
ATR_MULT_TP = 4.0

EXCLUDE_BASE_SUBSTRINGS = ["UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S"]
STABLECOINS = {"USDC", "USDT", "DAI", "USD", "EUR", "GBP", "PYUSD", "TUSD", "FDUSD"}

MIN_VOLATILITY_PCT = 3.0        # мин. дневной диапазон (high-low)/low *100, чтобы пара считалась волатильной
MIN_TURNOVER_USD = 300_000      # фильтр ликвидности, чтобы не ловить "мёртвые" пары


# ---------------------------------------------------------------------------
# Поиск волатильных пар (пересчитывается КАЖДЫЙ запуск, список не фиксирован)
# ---------------------------------------------------------------------------

def get_volatile_pairs(quote_coin: str, top_n: int) -> list:
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

    scored = []
    chunk_size = 50
    for i in range(0, len(candidates_names), chunk_size):
        chunk = candidates_names[i:i + chunk_size]
        try:
            tick_resp = requests.get(f"{BASE_URL}/Ticker", params={"pair": ",".join(chunk)}, timeout=20)
            tick_resp.raise_for_status()
            tick_data = tick_resp.json()
        except Exception:
            time.sleep(0.3)
            continue
        if tick_data.get("error"):
            time.sleep(0.3)
            continue
        for pair_name, t in tick_data.get("result", {}).items():
            try:
                high_24h = float(t["h"][1])
                low_24h = float(t["l"][1])
                last = float(t["c"][0])
                vwap_24h = float(t["p"][1])
                vol_24h = float(t["v"][1])
                turnover = vwap_24h * vol_24h
                if turnover < MIN_TURNOVER_USD or low_24h <= 0:
                    continue
                volatility_pct = (high_24h - low_24h) / low_24h * 100
                if volatility_pct < MIN_VOLATILITY_PCT:
                    continue
                scored.append((pair_name, volatility_pct, turnover))
            except (KeyError, ValueError, TypeError, ZeroDivisionError):
                continue
        time.sleep(0.3)

    scored.sort(key=lambda x: x[1], reverse=True)
    return [s[0] for s in scored[:top_n]]


def fetch_klines(pair: str, interval_minutes: int, min_bars: int) -> pd.DataFrame:
    resp = requests.get(f"{BASE_URL}/OHLC", params={"pair": pair, "interval": interval_minutes}, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("error"):
        raise RuntimeError(f"Kraken API error: {payload['error']}")

    result = payload["result"]
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


# ---------------------------------------------------------------------------
# Индикаторы
# ---------------------------------------------------------------------------

def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()


def macd(series, fast=12, slow=26, signal=9):
    macd_line = ema(series, fast) - ema(series, slow)
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line


def atr(df, length=14):
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(length).mean()


def rsi(series, length=RSI_LENGTH):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    result = 100 - (100 / (1 + rs))
    return result.fillna(50)


def adx(df, length=ADX_LENGTH):
    high, low, close = df["high"], df["low"], df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr_w = tr.ewm(alpha=1 / length, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / length, adjust=False).mean() / atr_w.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / length, adjust=False).mean() / atr_w.replace(0, np.nan)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    adx_val = dx.ewm(alpha=1 / length, adjust=False).mean()
    return adx_val.fillna(0)


def analyze_timeframe(df: pd.DataFrame, params: dict) -> dict:
    if df.empty or len(df) < params["min_bars"]:
        return None

    df = df.copy()
    df["ema_fast"] = ema(df["close"], params["ema_fast"])
    df["ema_slow"] = ema(df["close"], params["ema_slow"])
    df["macd_line"], df["macd_signal"] = macd(df["close"])
    df["atr"] = atr(df)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df)

    last = df.iloc[-2]   # последняя ЗАКРЫТАЯ свеча
    prev = df.iloc[-3]

    trend_up = bool(last["ema_fast"] > last["ema_slow"])
    macd_cross_up = bool((prev["macd_line"] <= prev["macd_signal"]) and (last["macd_line"] > last["macd_signal"]))
    macd_cross_down = bool((prev["macd_line"] >= prev["macd_signal"]) and (last["macd_line"] < last["macd_signal"]))
    ema_cross_down = bool((prev["ema_fast"] >= prev["ema_slow"]) and (last["ema_fast"] < last["ema_slow"]))

    return {
        "trend_up": trend_up,
        "macd_cross_up": macd_cross_up,
        "macd_cross_down": macd_cross_down,
        "ema_cross_down": ema_cross_down,
        "rsi": float(last["rsi"]),
        "adx": float(last["adx"]),
        "close": float(last["close"]),
        "atr": float(last["atr"]) if not np.isnan(last["atr"]) else 0.0,
        "bar_time": str(int(last["start"])),
    }


def check_confluence_entry(results: dict) -> bool:
    if any(results.get(tf) is None for tf in TIMEFRAME_ORDER):
        return False
    trend_all_up = all(results[tf]["trend_up"] for tf in TIMEFRAME_ORDER)
    trigger = results[TRIGGER_TF]["macd_cross_up"]
    rsi_ok = RSI_MIN <= results[TRIGGER_TF]["rsi"] <= RSI_MAX
    adx_ok = results[ADX_REF_TF]["adx"] >= ADX_MIN
    return trend_all_up and trigger and rsi_ok and adx_ok


def check_exit(results: dict, pos: dict) -> tuple:
    """Возвращает (exit_now: bool, reason: str)."""
    r4h = results.get(TRIGGER_TF)
    if r4h is None:
        return False, ""
    if r4h["close"] <= pos["stop"]:
        return True, "Stop-Loss"
    if r4h["close"] >= pos["target"]:
        return True, "Take-Profit"
    if r4h["ema_cross_down"] or r4h["macd_cross_down"]:
        return True, "Сигнал разворота (4h)"
    return False, ""


# ---------------------------------------------------------------------------
# Telegram: подписчики и рассылка
# ---------------------------------------------------------------------------

def load_subscribers() -> dict:
    if os.path.exists(SUBSCRIBERS_FILE):
        with open(SUBSCRIBERS_FILE) as f:
            return json.load(f)
    return {"offset": 0, "chat_ids": []}


def save_subscribers(data: dict):
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def poll_new_subscribers():
    if not TELEGRAM_BOT_TOKEN:
        return
    data = load_subscribers()
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = requests.get(url, params={"offset": data["offset"] + 1, "timeout": 0}, timeout=15)
        resp.raise_for_status()
        updates = resp.json().get("result", [])
    except Exception as e:
        print(f"Не удалось получить обновления Telegram: {e}")
        return

    for update in updates:
        data["offset"] = max(data["offset"], update.get("update_id", data["offset"]))
        msg = update.get("message") or update.get("channel_post")
        if not msg:
            continue
        chat_id = msg["chat"]["id"]
        text = (msg.get("text") or "").strip().lower()
        if text.startswith("/start") and chat_id not in data["chat_ids"]:
            data["chat_ids"].append(chat_id)
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              json={"chat_id": chat_id, "text": WELCOME_TEXT}, timeout=15)
            except Exception as e:
                print(f"Не удалось отправить приветствие {chat_id}: {e}")
        if text.startswith("/stop") and chat_id in data["chat_ids"]:
            data["chat_ids"].remove(chat_id)

    save_subscribers(data)
    print(f"Подписчиков: {len(data['chat_ids'])}")


def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN:
        print("[NO TELEGRAM CONFIG]", text)
        return
    data = load_subscribers()
    chat_ids = set(data.get("chat_ids", []))
    if TELEGRAM_CHAT_ID:
        chat_ids.add(TELEGRAM_CHAT_ID)
    if not chat_ids:
        print("[NO SUBSCRIBERS]", text)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    still_active = []
    for chat_id in chat_ids:
        try:
            r = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=15)
            if r.status_code == 403:
                continue
            r.raise_for_status()
            still_active.append(chat_id)
        except Exception as e:
            print(f"Не удалось отправить сообщение {chat_id}: {e}")
            still_active.append(chat_id)

    data["chat_ids"] = [c for c in data.get("chat_ids", []) if c in still_active or c == TELEGRAM_CHAT_ID]
    save_subscribers(data)


# ---------------------------------------------------------------------------
# Состояние открытых позиций и журнал сделок
# ---------------------------------------------------------------------------

def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def log_trade(symbol, entry_price, exit_price, entry_time, exit_time, reason):
    trades = load_json(TRADES_LOG_FILE, [])
    pnl_pct = (exit_price - entry_price) / entry_price * 100
    trades.append({
        "symbol": symbol,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "pnl_pct": round(pnl_pct, 2),
        "result": "win" if pnl_pct > 0 else "loss",
        "reason": reason,
    })
    save_json(TRADES_LOG_FILE, trades)
    return pnl_pct


# ---------------------------------------------------------------------------
# Режим SCAN: вход по confluence + проверка выходов
# ---------------------------------------------------------------------------

def run_scan(args):
    state = load_json(STATE_FILE, {})
    poll_new_subscribers()

    pairs = get_volatile_pairs(args.quote_coin, args.top_n)
    print(f"Отслеживаю {len(pairs)} самых волатильных пар Kraken Spot (мин. волатильность {MIN_VOLATILITY_PCT}%)...")

    found_buy, found_sell = 0, 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for pair in pairs:
        results = {}
        try:
            for tf in TIMEFRAME_ORDER:
                params = TIMEFRAME_PARAMS[tf]
                df = fetch_klines(pair, params["kraken_interval"], params["min_bars"] + 5)
                results[tf] = analyze_timeframe(df, params)
                time.sleep(args.request_delay)
        except Exception as e:
            print(f"[{pair}] ошибка получения данных: {e}")
            continue

        pos = state.get(pair, {"position": "closed"})

        if pos["position"] == "open":
            exit_now, reason = check_exit(results, pos)
            if exit_now:
                exit_price = results[TRIGGER_TF]["close"]
                pnl_pct = log_trade(pair, pos["entry_price"], exit_price, pos["entry_time"], now_iso, reason)
                text = (
                    f"🔴 <b>ВЫХОД (SELL)</b>\n"
                    f"Пара: <b>{pair}</b>\n"
                    f"Цена выхода: <b>{exit_price:.6g}</b>\n"
                    f"Причина: {reason}\n"
                    f"Результат: <b>{pnl_pct:+.2f}%</b>"
                )
                send_telegram(text)
                print(text)
                state[pair] = {"position": "closed"}
                found_sell += 1
        else:
            if check_confluence_entry(results):
                close = results[TRIGGER_TF]["close"]
                daily_atr = results["1d"]["atr"]
                stop = close - daily_atr * ATR_MULT_SL
                target = close + daily_atr * ATR_MULT_TP
                text = (
                    f"🟢 <b>ВХОД (BUY) — подтверждён на 4h/1d/1w</b>\n"
                    f"Пара: <b>{pair}</b>\n"
                    f"Цена входа: <b>{close:.6g}</b>\n"
                    f"RSI(4h): {results['4h']['rsi']:.1f}  ADX(1d): {results['1d']['adx']:.1f}\n"
                    f"Stop-Loss: {stop:.6g}\n"
                    f"Take-Profit: {target:.6g}"
                )
                send_telegram(text)
                print(text)
                state[pair] = {
                    "position": "open",
                    "entry_price": close,
                    "entry_time": now_iso,
                    "stop": stop,
                    "target": target,
                }
                found_buy += 1

    save_json(STATE_FILE, state)
    print(f"Готово. Новых входов: {found_buy}, выходов: {found_sell}")


# ---------------------------------------------------------------------------
# Режим REPORT: сводка за 3 дня / месяц
# ---------------------------------------------------------------------------

def run_report(args):
    poll_new_subscribers()
    trades = load_json(TRADES_LOG_FILE, [])

    days = 3 if args.period == "3d" else 30
    since = datetime.now(timezone.utc) - timedelta(days=days)

    period_trades = [t for t in trades if datetime.fromisoformat(t["exit_time"]) >= since]

    if not period_trades:
        text = f"📊 <b>Отчёт за {'3 дня' if args.period == '3d' else '30 дней'}</b>\nЗакрытых сделок не было."
        send_telegram(text)
        print(text)
        return

    wins = [t for t in period_trades if t["result"] == "win"]
    losses = [t for t in period_trades if t["result"] == "loss"]
    win_rate = len(wins) / len(period_trades) * 100
    total_pnl = sum(t["pnl_pct"] for t in period_trades)
    avg_win = sum(t["pnl_pct"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_pct"] for t in losses) / len(losses) if losses else 0

    text = (
        f"📊 <b>Отчёт за {'3 дня' if args.period == '3d' else '30 дней'}</b>\n"
        f"Всего сделок: {len(period_trades)}\n"
        f"✅ Прибыльных: {len(wins)} ({win_rate:.1f}%)\n"
        f"❌ Убыточных: {len(losses)} ({100 - win_rate:.1f}%)\n"
        f"Средняя прибыль: {avg_win:+.2f}%\n"
        f"Средний убыток: {avg_loss:+.2f}%\n"
        f"Суммарный результат: <b>{total_pnl:+.2f}%</b>"
    )
    send_telegram(text)
    print(text)


# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["scan", "report"], default="scan")
    parser.add_argument("--period", choices=["3d", "month"], default="3d")
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--quote-coin", default="USD")
    parser.add_argument("--request-delay", type=float, default=0.3)
    args = parser.parse_args()

    if args.mode == "scan":
        run_scan(args)
    else:
        run_report(args)


if __name__ == "__main__":
    main()
