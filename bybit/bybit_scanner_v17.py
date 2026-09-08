#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
BYBIT SCANNER v17.3 «HYBRID+» — ВСЯ КРИПТА (ГИБРИДНЫЙ ФИЛЬТР)
WebSocket (wss://stream.bybit.com/v5/public/spot) + REST (api.bybit.com/v5)
Бумажная торговля: сделки -> bybit_trades.json, алерты -> Telegram

ИСПРАВЛЕНО В v17.3:
 - Гибридный фильтр is_crypto(): больше НЕ использует белый список,
   а исключает только явные не-криптоактивы (фиат, стейблы, акции).
 - Теперь бот видит ВСЕ криптовалюты Bybit (~500+ пар),
   а не только 80 монет из CRYPTO_WHITELIST.
==============================================================================
"""

import json
import os
import signal
import time
import logging
import logging.handlers
import threading
import requests
import pandas as pd
import numpy as np
import websocket
from collections import deque
from datetime import datetime, timezone

# ==================== КОНФИГУРАЦИЯ (BYBIT) ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
BASE_URL = "https://api.bybit.com/v5/market"
WS_URL = "wss://stream.bybit.com/v5/public/spot"

TOP_N = 200
TOTAL_PAIRS = 700
SCAN_INTERVAL_SECONDS = 7200
MIN_TURNOVER_USD = 50_000

TIMEFRAME = "15"
MIN_BARS = 80
TRIGGER_TF = "4h"
TIME_STOP_DAYS = 7
TIME_STOP_MIN_R = 1.0

MAX_OPEN_POSITIONS = 8
MAX_TRADES_PER_HOUR = 15
ENTRY_COOLDOWN_SECONDS = 1800

MIN_STOP_DISTANCE_PCT = 0.5
MAX_BREAKOUT_DISTANCE_PCT = 4.5
BREAKOUT_MIN_TRIGGER_PCT = 0.5
BREAKOUT_MAX_ADX = 30
CONSOLIDATION_MAX_RANGE_PCT = 18.0
CONSOLIDATION_MAX_ADX = 22

FEE_PCT = 0.25
SLIPPAGE_PCT = 0.05

ATR_MULT_SL = 3.0
ATR_MULT_TP = 6.0
PARTIAL_TP_ATR = 3.0
PARTIAL_TP_FRACTION = 0.5
BREAKEVEN_TRIGGER_ATR = 1.0
TRAILING_TRIGGER_ATR = 2.0
TRAILING_STEP_ATR = 1.5

MIN_SIGNAL_SCORE = 5
FRESH_CROSS_WINDOW = 3
RSI_MIN, RSI_MAX = 35, 80
ADX_MIN, ADX_MAX = 15, 60
MIN_VOL_MULT = 1.2
PULLBACK_ENABLED = True
PULLBACK_VOL_MULT = 1.1
PULLBACK_RSI_MAX = 68
PULLBACK_SL_ATR = 2.5
PULLBACK_TP_ATR = 5.0

TREND_CACHE_REFRESH_SECONDS = 1800
TREND_CACHE_INITIAL_LIMIT = 60
BREAKOUT_CACHE_SIZE = 30
CLEANUP_AFTER_DAYS = 14
WS_SILENCE_TIMEOUT = 90

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(WORK_DIR, "bybit_state.json")
TRADES_LOG_FILE = os.path.join(WORK_DIR, "bybit_trades.json")
LOG_FILE = os.path.join(WORK_DIR, "bybit_scanner.log")
MAX_STATUS_PAIRS = 20

# ==================== ФИЛЬТРЫ (ГИБРИДНЫЙ, v17.3) ====================
FIAT_BASES = {"AUD", "GBP", "EUR", "CAD", "CHF", "JPY", "USD",
              "BRL", "MXN", "TRY", "ZAR", "INR", "SGD", "HKD"}

# Известные стейблкоины
STABLECOINS = {
    "USDC", "USDT", "DAI", "PYUSD", "TUSD", "FDUSD", "AUSD", "EURR",
    "USDR", "FRNT", "EURQ", "USDPT", "BRL1", "EUROP", "USDTB",
    "USD1", "RLUSD", "EURT", "GUSD", "FRAX", "LUSD", "SUSD", "USDP",
}

# Токены акций (Bybit их почти не листит, но на всякий случай)
STOCK_TOKENS = {"AAPLX", "AMZNX", "GOOGLX", "MCDX", "TSLAX", "COINX"}

# Подозрительные/мёртвые монеты (ручные исключения)
MANUAL_EXCLUDED = set()   # добавляй сюда по мере необходимости

EXCLUDED = FIAT_BASES | STABLECOINS | STOCK_TOKENS | MANUAL_EXCLUDED

# Подстроки-маркеры стейблов (для новых листингов)
STABLE_SUBSTRINGS = ("USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "BRL", "MXN")

def is_crypto(base):
    """Гибридный фильтр: исключает очевидные не-крипта, остальное пропускает."""
    if not base:
        return False
    # 1) Фиат-базы (USD/AUD/EUR и т.д.) — точно не крипта
    if base in FIAT_BASES:
        return False
    # 2) Токены акций
    if base.endswith("X") or base in STOCK_TOKENS:
        return False
    # 3) Известные стейблы
    if base in STABLECOINS:
        return False
    # 4) Ручные исключения
    if base in MANUAL_EXCLUDED:
        return False
    # 5) Новые стейблы по подстроке в имени
    if any(s in base for s in STABLE_SUBSTRINGS):
        return False
    # 6) Всё остальное — СЧИТАЕМ КРИПТОЙ (новые листинги, меми, L2 и т.д.)
    return True

# ==================== ЛОГИРОВАНИЕ ====================
os.makedirs(WORK_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5_000_000,
                                             backupCount=3, encoding="utf-8"),
    ],
)
logger = logging.getLogger("bybit-scanner")

# ==================== ГЛОБАЛЬНОЕ СОСТОЯНИЕ ====================
state = {}
state_lock = threading.RLock()
trade_times = []
ohlc_buffers = {}
last_processed_closed = {}

trend_cache = {}
trend_cache_lock = threading.RLock()
LAST_FILTERED_PAIRS = []

breakout_cache = {}
breakout_cache_lock = threading.RLock()

last_ws_msg_ts = time.time()
WS_APP = None
PAIRS_WS = []

SESSION = requests.Session()

# ==================== HTTP-СЛОЙ ====================
def http_get(url, params=None, timeout=20, retries=3):
    """GET с ретраями и бэкоффом на 429/5xx."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = 2 ** attempt
                logger.warning("HTTP %s от Bybit, повтор через %d c", resp.status_code, wait)
                time.sleep(wait)
                continue
            return resp.json()
        except Exception as e:
            logger.warning("Сетевая ошибка %s (попытка %d): %s", url, attempt + 1, e)
            time.sleep(2 ** attempt)
    return None

def api_ok(data) -> bool:
    return bool(data) and data.get("retCode") == 0

# ==================== СЛУЖЕБНЫЕ ФУНКЦИИ ====================
def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for i in range(0, len(text), 4000):
        chunk = text[i:i + 4000]
        try:
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID,
                                     "text": chunk, "parse_mode": "HTML"},
                          timeout=10)
        except Exception as e:
            logger.error("Ошибка отправки Telegram: %s", e)

def save_state(state_data):
    with state_lock:
        try:
            tmp_file = STATE_FILE + ".tmp"
            with open(tmp_file, "w") as f:
                json.dump(state_data, f, indent=2)
            os.replace(tmp_file, STATE_FILE)
        except Exception as e:
            logger.error("Ошибка сохранения state: %s", e)

def load_state():
    with state_lock:
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.critical("Не удалось загрузить state: %s", e)
            try:
                os.replace(STATE_FILE, STATE_FILE + ".corrupt")
            except OSError:
                pass
            return {}

def log_trade(symbol, entry, exit_price, reason, strategy,
              entry_time=None, size_fraction=1.0):
    with state_lock:
        trades = []
        if os.path.exists(TRADES_LOG_FILE):
            try:
                with open(TRADES_LOG_FILE, "r") as f:
                    trades = json.load(f)
            except Exception:
                trades = []
        now_iso = datetime.now(timezone.utc).isoformat()
        if not entry_time:
            entry_time = now_iso
        raw_pnl = (exit_price - entry) / entry * 100 if entry else 0.0
        costs = (FEE_PCT * 2 + SLIPPAGE_PCT) * size_fraction
        net_pnl = raw_pnl * size_fraction - costs
        trades.append({
            "symbol": symbol, "entry_price": entry, "exit_price": exit_price,
            "entry_time": entry_time, "exit_time": now_iso,
            "size_fraction": round(size_fraction, 2),
            "raw_pnl_pct": round(raw_pnl, 2),
            "net_pnl_pct": round(net_pnl, 2),
            "pnl_pct": round(net_pnl, 2),
            "result": "win" if net_pnl > 0 else "loss",
            "reason": reason, "strategy": strategy,
        })
        with open(TRADES_LOG_FILE, "w") as f:
            json.dump(trades, f, indent=2)
        return round(net_pnl, 2)

# ==================== ИНДИКАТОРЫ ====================
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
    true_range = ranges.max(axis=1)
    return true_range.rolling(period).mean()

def adx(df, period=14):
    up = df["high"].diff()
    down = -df["low"].diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    tr = atr(df, 1).replace(0, np.nan)
    plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False).mean() / tr)
    minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False).mean() / tr)
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * np.abs((plus_di - minus_di) / di_sum)
    return dx.ewm(alpha=1 / period, adjust=False).mean()

def analyze_timeframe(df, params):
    if df.empty or len(df) < params["min_bars"]:
        return None
    df = df.copy()
    df["ema_fast"] = ema(df["close"], params["ema_fast"])
    df["ema_slow"] = ema(df["close"], params["ema_slow"])
    df["macd_line"] = ema(df["close"], 12) - ema(df["close"], 26)
    df["macd_signal"] = ema(df["macd_line"], 9)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df)
    df["atr"] = atr(df)
    df["vol_sma"] = df["volume"].rolling(20).mean()
    if len(df) < 6:
        return None
    last = df.iloc[-2]
    prev = df.iloc[-3]
    if any(pd.isna([last["ema_fast"], last["ema_slow"], last["rsi"],
                    last["adx"], last["macd_line"], last["macd_signal"]])):
        return None
    adx_ago = df["adx"].iloc[-5]
    adx_slope_up = bool(last["adx"] > adx_ago) if not pd.isna(adx_ago) else False
    vol_sma = last["vol_sma"]
    vol_ratio = float(last["volume"] / vol_sma) if not pd.isna(vol_sma) and vol_sma > 0 else 0.0
    return {
        "trend_up": bool(last["ema_fast"] > last["ema_slow"]),
        "macd_cross_up": bool(prev["macd_line"] <= prev["macd_signal"]
                              and last["macd_line"] > last["macd_signal"]),
        "ema_cross_down": bool(prev["ema_fast"] >= prev["ema_slow"]
                               and last["ema_fast"] < last["ema_slow"]),
        "ema_cross_up": bool(prev["ema_fast"] <= prev["ema_slow"]
                             and last["ema_fast"] > last["ema_slow"]),
        "rsi": float(last["rsi"]),
        "adx": float(last["adx"]),
        "adx_slope_up": adx_slope_up,
        "vol_ratio": vol_ratio,
        "close": float(last["close"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "open": float(last["open"]),
        "atr": float(last["atr"]) if not pd.isna(last["atr"]) else 0.0,
        "macd_line": float(last["macd_line"]),
        "macd_signal": float(last["macd_signal"]),
        "ema_fast": float(last["ema_fast"]),
        "ema_slow": float(last["ema_slow"]),
    }

# ==================== СТРАТЕГИИ ====================
def detect_consolidation(df_daily):
    closed = df_daily.iloc[:-1]
    if len(closed) < 60:
        return None
    window = closed.tail(30)
    high = window["high"].max()
    low = window["low"].min()
    mean = window["close"].mean()
    if mean <= 0:
        return None
    range_pct = (high - low) / mean * 100
    adx_val = adx(closed).iloc[-1]
    if range_pct <= CONSOLIDATION_MAX_RANGE_PCT and adx_val < CONSOLIDATION_MAX_ADX:
        return {"days": len(window), "range_pct": range_pct, "adx": adx_val,
                "upper_level": high, "lower_level": low}
    return None

def check_breakout(df_daily, current_price):
    closed = df_daily.iloc[:-1]
    if len(closed) < 60:
        return None
    window = closed.tail(30)
    high = window["high"].max()
    low = window["low"].min()
    mean = window["close"].mean()
    if mean <= 0:
        return None
    range_pct = (high - low) / mean * 100
    if range_pct > CONSOLIDATION_MAX_RANGE_PCT:
        return None
    if current_price < high * (1 + BREAKOUT_MIN_TRIGGER_PCT / 100):
        return None
    if current_price > high * (1 + MAX_BREAKOUT_DISTANCE_PCT / 100):
        return None
    if len(closed) < 21:
        return None
    vol_avg = closed["volume"].rolling(20).mean().iloc[-2]
    last_closed_volume = closed["volume"].iloc[-1]
    if not (vol_avg > 0 and last_closed_volume > vol_avg * 1.8):
        return None
    adx_val = adx(closed).iloc[-1]
    atr_val = atr(closed).iloc[-1]
    if pd.isna(adx_val) or adx_val >= BREAKOUT_MAX_ADX:
        return None
    if pd.isna(atr_val) or atr_val <= 0:
        return None
    return {"days": len(window), "range_pct": range_pct, "adx": adx_val,
            "stop": current_price - atr_val * ATR_MULT_SL,
            "target": current_price + atr_val * ATR_MULT_TP}

# ==================== УПРАВЛЕНИЕ ПОЗИЦИЕЙ ====================
def check_exit(results, pos):
    if not results.get(TRIGGER_TF) or not results.get("1d"):
        return False, "", 0.0
    r4h = results[TRIGGER_TF]
    r1d = results["1d"]
    atr1d = r1d["atr"] if r1d["atr"] > 0 else pos.get("atr_ref", 0.0)
    entry = pos.get("entry_price") or 0.0

    if r4h["low"] <= pos["stop"]:
        return True, "Stop-Loss", min(r4h["open"], pos["stop"])
    if r4h["high"] >= pos["target"]:
        return True, "Take-Profit", pos["target"]

    if entry > 0 and atr1d > 0:
        if not pos.get("partial_done") and r4h["high"] >= entry + PARTIAL_TP_ATR * atr1d:
            pos["partial_done"] = True
            pos["stop"] = max(pos["stop"], entry * 1.001)
            return False, "partial", entry + PARTIAL_TP_ATR * atr1d
        if (not pos.get("breakeven_moved")
                and r4h["close"] >= entry + BREAKEVEN_TRIGGER_ATR * atr1d):
            pos["breakeven_moved"] = True
            pos["stop"] = max(pos["stop"], entry * 1.001)
        if r4h["close"] >= entry + TRAILING_TRIGGER_ATR * atr1d:
            pos["highest_close"] = max(pos.get("highest_close", r4h["close"]),
                                       r4h["close"])
            new_stop = pos["highest_close"] - TRAILING_STEP_ATR * atr1d
            floor = entry * 1.001 if (pos.get("partial_done")
                                      or pos.get("breakeven_moved")) else pos["stop"]
            if new_stop > pos["stop"]:
                pos["stop"] = max(new_stop, floor)

    if r4h.get("ema_cross_down"):
        return True, "Разворот", r4h["close"]
    return False, "", 0.0

def can_enter(pair):
    now = time.time()
    trade_times[:] = [t for t in trade_times if now - t < 3600]
    old_state = state.get(pair, {})
    if old_state.get("position") == "open":
        return False
    if now - float(old_state.get("last_exit_ts", 0) or 0) < ENTRY_COOLDOWN_SECONDS:
        return False
    open_positions = sum(1 for p in state.values() if p.get("position") == "open")
    if open_positions >= MAX_OPEN_POSITIONS:
        return False
    if len([t for t in trade_times if now - t < 3600]) >= MAX_TRADES_PER_HOUR:
        return False
    return True

# ==================== ТРЕНД-КЭШ ====================
def get_trend(symbol):
    with trend_cache_lock:
        return trend_cache.get(symbol)

def compute_tf_trend(df, params):
    if df is None or df.empty or len(df) < 40:
        return False
    f = ema(df["close"], params["ema_fast"])
    s = ema(df["close"], params["ema_slow"])
    if len(f) < 6 or pd.isna(f.iloc[-2]) or pd.isna(s.iloc[-2]) or pd.isna(f.iloc[-5]):
        return False
    return bool(f.iloc[-2] > s.iloc[-2] and f.iloc[-2] > f.iloc[-5])

def refresh_trend_for(pair):
    res = {}
    for tf in ("4h", "1d", "1w"):
        p = TIMEFRAME_PARAMS[tf]
        df = pd.DataFrame(fetch_klines(pair, p["bybit_interval"], p["min_bars"]))
        res[tf] = compute_tf_trend(df, p)
        time.sleep(0.15)
    score = sum(1 for v in res.values() if v)
    with trend_cache_lock:
        trend_cache[pair] = {"score": score, **res}
    return score

def refresh_all_trends():
    with state_lock:
        open_pairs = [p for p, v in state.items() if v.get("position") == "open"]
    watch = list(dict.fromkeys(LAST_FILTERED_PAIRS + open_pairs))
    q3 = 0
    for pair in watch:
        try:
            if refresh_trend_for(pair) == 3:
                q3 += 1
        except Exception as e:
            logger.debug("trend %s: %s", pair, e)
    logger.info("Тренд-кэш обновлён: %d пар, подтверждённый тренд 3/3 = %d",
                len(watch), q3)

def trend_cache_loop():
    first = True
    while True:
        if not first:
            time.sleep(TREND_CACHE_REFRESH_SECONDS)
        first = False
        try:
            refresh_all_trends()
        except Exception as e:
            logger.error("Поток тренд-кэша: %s", e)

# ==================== ВСЕЛЕННАЯ ПАР ====================
def fetch_spot_instruments():
    items = []
    cursor = ""
    while True:
        params = {"category": "spot", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = http_get(f"{BASE_URL}/instruments-info", params=params)
        if not api_ok(data):
            break
        result = data.get("result") or {}
        batch = result.get("list") or []
        items.extend(batch)
        cursor = result.get("nextPageCursor") or ""
        if not cursor or not batch:
            break
    return items

def fetch_spot_tickers():
    data = http_get(f"{BASE_URL}/tickers", params={"category": "spot"})
    if not api_ok(data):
        return {}
    out = {}
    for t in (data.get("result") or {}).get("list") or []:
        try:
            out[t["symbol"]] = {
                "last": float(t["lastPrice"]),
                "turnover": float(t.get("turnover24h", 0)),
                "high": float(t.get("highPrice24h", 0)),
                "low": float(t.get("lowPrice24h", 0)),
            }
        except (KeyError, TypeError, ValueError):
            continue
    return out

def _spot_candidates():
    return [
        it["symbol"] for it in fetch_spot_instruments()
        if it.get("status") == "Trading"
        and it.get("quoteCoin") == "USDT"
        and is_crypto(it.get("baseCoin", ""))
    ]

def get_filtered_pairs(top_n):
    global LAST_FILTERED_PAIRS
    try:
        candidates = _spot_candidates()
        if not candidates:
            return []
        tickers = fetch_spot_tickers()
        scored = []
        for sym in candidates:
            t = tickers.get(sym)
            if not t or t["turnover"] < MIN_TURNOVER_USD or t["low"] <= 0:
                continue
            scored.append((sym, (t["high"] - t["low"]) / t["low"] * 100))
        scored.sort(key=lambda x: x[1], reverse=True)
        LAST_FILTERED_PAIRS = [s[0] for s in scored[:top_n]]
        return LAST_FILTERED_PAIRS
    except Exception as e:
        logger.error("get_filtered_pairs: %s", e)
        return []

def get_all_available_pairs(max_pairs):
    try:
        candidates = _spot_candidates()
        if not candidates:
            return []
        tickers = fetch_spot_tickers()
        ranked = [
            (sym, tickers[sym]["turnover"])
            for sym in candidates
            if sym in tickers and tickers[sym]["turnover"] >= MIN_TURNOVER_USD
        ]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return [s[0] for s in ranked[:max_pairs]]
    except Exception as e:
        logger.error("get_all_available_pairs: %s", e)
        return []

# ==================== ЗАГРУЗКА ДАННЫХ ====================
def fetch_klines(pair, interval, min_bars):
    data = http_get(f"{BASE_URL}/kline", params={
        "category": "spot", "symbol": pair,
        "interval": interval, "limit": min(min_bars, 1000),
    })
    if not api_ok(data):
        return []
    rows = (data.get("result") or {}).get("list") or []
    if not rows:
        return []
    rows = list(reversed(rows))
    out = []
    for r in rows:
        try:
            out.append({
                "start": int(r[0]) // 1000,
                "open": float(r[1]), "high": float(r[2]),
                "low": float(r[3]), "close": float(r[4]),
                "volume": float(r[5]),
            })
        except (TypeError, ValueError, IndexError):
            continue
    return out[-min_bars:]

def fetch_current_prices(pairs):
    want = set(p for p in pairs if isinstance(p, str))
    prices = {}
    for sym, t in fetch_spot_tickers().items():
        if sym in want:
            prices[sym] = t["last"]
    return prices

# ==================== СКОРИНГ ВХОДА (WS, 15m) ====================
def find_fresh_cross(df, window=FRESH_CROSS_WINDOW):
    ml, ms = df["macd_line"], df["macd_signal"]
    for ago in range(0, window):
        i = len(df) - 2 - ago
        if i < 1:
            return None
        if ml.iloc[i - 1] <= ms.iloc[i - 1] and ml.iloc[i] > ms.iloc[i]:
            return ago
    return None

def evaluate_ws_entry(df, symbol):
    if len(df) < MIN_BARS + 1:
        return None
    sig = df.iloc[-2]
    if (pd.isna(sig["ema_fast"]) or pd.isna(sig["rsi"])
            or pd.isna(sig["atr"]) or sig["atr"] <= 0):
        return None
    if not (RSI_MIN <= sig["rsi"] <= RSI_MAX):
        return None
    if not (ADX_MIN <= sig["adx"] <= ADX_MAX):
        return None
    if not (sig["ema_fast"] > sig["ema_slow"]):
        return None
    ti = get_trend(symbol)
    if ti is None or ti["score"] < 2:
        return None

    score = 0
    parts = []
    cross_ago = find_fresh_cross(df)
    if cross_ago == 0:
        score += 3
        parts.append("MACD-кросс +3")
    elif cross_ago in (1, 2):
        hist_now = sig["macd_line"] - sig["macd_signal"]
        hist_prev = df.iloc[-3]["macd_line"] - df.iloc[-3]["macd_signal"]
        if hist_now > hist_prev:
            score += 2
            parts.append(f"MACD-кросс ({cross_ago + 1} св. назад) +2")
        else:
            return None
    else:
        return None

    score += 1
    parts.append("EMA9>21 +1")
    if bool(sig["ema_cross_up"]):
        score += 1
        parts.append("EMA-кросс +1")
    score += 1
    parts.append("RSI в зоне +1")
    if bool(sig["adx_slope_up"]):
        score += 1
        parts.append("ADX растёт +1")
    vol_ratio = float(sig["vol_ratio"]) if not pd.isna(sig["vol_ratio"]) else 0.0
    if vol_ratio >= MIN_VOL_MULT:
        score += 1
        parts.append(f"Объём {vol_ratio:.1f}× +1")
    if ti["score"] == 3:
        score += 2
        parts.append("Тренд 3/3 +2")
    else:
        score += 1
        parts.append("Тренд 2/3 +1")

    if score < MIN_SIGNAL_SCORE:
        return None

    entry = float(df.iloc[-1]["close"])
    atr_value = float(sig["atr"])
    stop = entry - atr_value * ATR_MULT_SL
    target = entry + atr_value * ATR_MULT_TP
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    return {"entry": entry, "stop": stop, "target": target, "atr": atr_value,
            "score": score, "parts": parts, "strategy": "ws_15m"}

def evaluate_ws_pullback(df, symbol):
    if not PULLBACK_ENABLED or len(df) < MIN_BARS + 1:
        return None
    ti = get_trend(symbol)
    if ti is None or ti["score"] < 3:
        return None
    if find_fresh_cross(df) is not None:
        return None
    sig = df.iloc[-2]
    if pd.isna(sig["ema_fast"]) or pd.isna(sig["ema_slow"]) or pd.isna(sig["atr"]):
        return None
    if sig["atr"] <= 0 or not (sig["ema_fast"] > sig["ema_slow"]):
        return None
    touched = sig["low"] <= sig["ema_slow"] * 1.002
    recovered = sig["close"] >= sig["ema_fast"]
    if not (touched and recovered):
        return None
    if not (RSI_MIN <= sig["rsi"] <= PULLBACK_RSI_MAX):
        return None
    if not (ADX_MIN <= sig["adx"] <= ADX_MAX):
        return None
    vol_ratio = float(sig["vol_ratio"]) if not pd.isna(sig["vol_ratio"]) else 0.0
    if vol_ratio < PULLBACK_VOL_MULT:
        return None
    entry = float(df.iloc[-1]["close"])
    atr_value = float(sig["atr"])
    stop = entry - atr_value * PULLBACK_SL_ATR
    target = entry + atr_value * PULLBACK_TP_ATR
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    return {"entry": entry, "stop": stop, "target": target, "atr": atr_value,
            "score": "PB",
            "parts": [f"Откат к EMA21 · тренд 3/3 · объём {vol_ratio:.1f}×"],
            "strategy": "ws_pullback"}

def open_position(symbol, sig, closed_start):
    if not can_enter(symbol):
        return None
    old_state = state.get(symbol, {})
    if old_state.get("last_signal_candle") == closed_start:
        return None
    new_state = old_state.copy()
    new_state.update({
        "position": "open",
        "entry_price": sig["entry"],
        "stop": sig["stop"],
        "target": sig["target"],
       
