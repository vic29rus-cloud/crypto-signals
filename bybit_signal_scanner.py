#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BYBIT SCANNER v22.4 OBSERVE

Режим наблюдения:
- size = 1.0 всегда (100% депо на сделку)
- НЕТ частичных закрытий (partial TP отключён)
- BE при +0.5R вместо partial TP
- PnL показывается ТОЛЬКО по цене (Вход→Выход), без вклада в счёт

Из v22.3 сохранено:
- Confluence / Pullback / Breakout
- BTC/ETH/stables off
- Alt Breadth (medRSI<38 → блок)
- Score≥6, RR≥2.0, 1 вход/30мин, 4/час
- force_close fix
- Кликабельные ссылки TradingView
"""

import os
import json
import time
import logging
import logging.handlers
import threading
import signal
from collections import deque
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np
import websocket


TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


WORK_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(WORK_DIR, "bybit_scanner_v224.log")
STATE_FILE = os.path.join(WORK_DIR, "bybit_state_v224.json")
TRADES_FILE = os.path.join(WORK_DIR, "bybit_trades_v224.json")
CB_FILE = os.path.join(WORK_DIR, "bybit_cb_v224.json")
SUBSCRIBERS_FILE = os.path.join(WORK_DIR, "subscribers_v224.json")


BASE_URL = "https://api.bybit.com/v5/market"
WS_URL = "wss://stream.bybit.com/v5/public/spot"


# ==================== СКАН / WS ====================
TOP_N = 150
MIN_TURNOVER_USDT = 3_000_000
MIN_PRICE = 0.0001

SCAN_INTERVAL_SECONDS = 1800
STATUS_INTERVAL_SECONDS = 7200
MANAGE_SLEEP_SECONDS = 10

WS_KLINE_BUFFER = 180
WS_TICKER_MAX = 50
WS_SUBSCRIBE_CHUNK = 10
WS_SUBSCRIBE_PAUSE = 0.15
WS_SILENCE_TIMEOUT = 150


# ==================== РИСК (v22.4 OBSERVE) ====================
MAX_OPEN_POSITIONS = 5
MAX_TRADES_PER_HOUR = 4
MAX_TRADES_PER_30MIN = 1

ENTRY_COOLDOWN_SECONDS = 3600
PAIR_LOSS_COOLDOWN_SECONDS = 86400

MIN_SIGNAL_SCORE = 6
MIN_STOP_DISTANCE_PCT = 1.0
MIN_RR = 2.0

ATR_MULT_SL = 4.0
ATR_MULT_TP = 8.0
PARTIAL_TP_ATR = 3.5
TRAILING_STEP_ATR = 2.5
TIME_STOP_HOURS = 72

# v22.4: режим наблюдения
NO_PARTIAL_TP = True
BREAKEVEN_AT_R = 0.5

RISK_PER_TRADE_PCT = 0.75
MAX_PORTFOLIO_RISK_PCT = 4.0


# ==================== CB ====================
CB_CONSEC_LOSSES = 3
CB_DAILY_LOSS_PCT = -2.0
CB_PAUSE_SECONDS = 12 * 3600


# ==================== BTC ====================
BTC_DROP_6H_PCT = -3.5
BTC_ADX_BLOCK = 50


# ==================== ALT BREADTH ====================
ALT_BREADTH_ENABLED = True
ALT_BREADTH_MEDIAN_RSI = 38.0
ALT_BREADTH_LOW_PCT = 60.0
ALT_BREADTH_MIN_SAMPLES = 20


# ==================== ФИЛЬТРЫ ====================
RSI_MIN, RSI_MAX = 35, 72
ADX_MIN, ADX_MAX = 15, 55
MIN_VOL_MULT = 1.1

BREAKOUT_VOL_MULT = 1.4
BREAKOUT_RT_VOL_MULT = 1.6

CONSOL_DAYS = 20
CONSOL_MAX_RANGE_PCT = 25.0
CONSOL_MIN_RANGE_PCT = 1.5

ARM_EXPIRY_CONFLUENCE = 3600
ARM_EXPIRY_PULLBACK = 3600
ARM_EXPIRY_BREAKOUT = 1800

ENTRY_TOLERANCE_LOW = 0.992
ENTRY_TOLERANCE_HIGH = 1.008

BREAKOUT_ENTRY_LOW = 0.998
BREAKOUT_ENTRY_HIGH = 1.015

STATUS_RSI_MIN, STATUS_RSI_MAX = 35, 75
STATUS_ADX_MIN, STATUS_ADX_MAX = 18, 55

TG_MSG_LIMIT = 3900


# ==================== СТЕЙБЛ-ФИЛЬТР ====================
STABLE_BASES = {
    "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "TRY", "BRL", "MXN",
    "INR", "SGD", "HKD", "ZAR", "NZD", "NOK", "SEK",
    "USDC", "DAI", "TUSD", "FDUSD", "PYUSD", "USDD", "USDE",
    "USDS", "RLUSD", "USDX", "USD1", "GUSD", "BUSD", "USDP",
    "USTC", "USDY", "SUSD", "LUSD", "FRAX", "WUSD", "XUSD",
    "DUSD", "AUSD", "USDF", "USD0", "EURT", "EURS", "EURI",
    "USDR", "USDTB", "EURQ", "EUROP", "FRNT", "BRL1",
    "XAUT", "PAXG",
    "BTC", "ETH",
}
# ==================== ЛОГИ ====================
logger = logging.getLogger("bybit-scanner-v224")


def setup_logging():
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)


# ==================== ГЛОБАЛЬНОЕ ====================
SESSION = requests.Session()

state = {}
cb = {"consec": 0, "day_pnl": 0.0, "day": "", "paused_until": 0.0}
subscribers = set()

state_lock = threading.RLock()
cb_lock = threading.RLock()
file_lock = threading.RLock()
sub_lock = threading.RLock()
trade_times_lock = threading.RLock()

trade_times = []
entry_30 = []

PAIRS = []
pairs_lock = threading.RLock()

OHLC = {}
ohlc_lock = threading.RLock()

LAST_PROCESSED = {}
CURRENT_PRICE = {}
price_lock = threading.RLock()

CONS_LEVEL = {}
cons_lock = threading.RLock()

CANDIDATES = []
CONSOLIDATIONS = []
scan_lock = threading.RLock()
LAST_SCAN_TS = 0.0

ARMED = {}
armed_lock = threading.RLock()

LAST_ANALYSIS = {}
analysis_lock = threading.RLock()

TREND_CACHE = {}
trend_lock = threading.RLock()

BTC_CACHE = {"ts": 0.0, "ok": True, "reason": "init"}
btc_lock = threading.RLock()

BREADTH_CACHE = {"ts": 0.0, "ok": True, "reason": "init"}
breadth_lock = threading.RLock()

WS_KLINE_APP = None
WS_TICKER_APP = None
WS_KLINE_CONNECTED = threading.Event()
WS_TICKERS_CONNECTED = threading.Event()

WS_TICKER_PAIRS = set()
ws_ticker_lock = threading.RLock()

LAST_WS_KLINE_TS = 0.0
LAST_WS_TICKER_TS = 0.0
ws_ts_lock = threading.RLock()

STOP_EVENT = threading.Event()


# ==================== HTML-HELPERS ====================
def tv_link(symbol):
    url = f"https://www.tradingview.com/chart/?symbol=BYBIT:{symbol}"
    return f'<a href="{url}">📈 {symbol}</a>'


def esc(text):
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


# v22.4.1: HTML-sanitize вне известных тегов
import re as _re

_TAG_RE = _re.compile(
    r'<(/?)(a|b|i|u|s|code|pre|blockquote|em|strong|tg-spoiler)(\s[^<>]*)?>',
    _re.IGNORECASE,
)


def _escape_non_tags(text):
    """Эскейпит & < > только вне разрешённых Telegram-тегов."""
    if not text:
        return text
    text = text.replace("&lt;", "\x00LT\x00")
    text = text.replace("&gt;", "\x00GT\x00")
    text = text.replace("&amp;", "\x00AMP\x00")

    out = []
    pos = 0
    for m in _TAG_RE.finditer(text):
        chunk = text[pos:m.start()]
        chunk = (chunk.replace("&", "&amp;")
                      .replace("<", "&lt;")
                      .replace(">", "&gt;"))
        out.append(chunk)
        out.append(m.group(0))
        pos = m.end()
    tail = text[pos:]
    tail = (tail.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;"))
    out.append(tail)
    result = "".join(out)

    result = result.replace("\x00LT\x00", "&lt;")
    result = result.replace("\x00GT\x00", "&gt;")
    result = result.replace("\x00AMP\x00", "&amp;")
    return result


def _strip_tags(text):
    if not text:
        return text
    return _re.sub(r'<[^>]+>', '', text)


# ==================== TELEGRAM ====================
def _post_telegram(chat_id, text, reply_markup=None):
    if not TELEGRAM_BOT_TOKEN:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    # v22.4.1: HTML с escape вне тегов, при ошибке — plain со strip
    for mode in ("HTML", "PLAIN"):
        if mode == "HTML":
            payload = {
                "chat_id": chat_id,
                "text": _escape_non_tags(text),
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
        else:
            payload = {
                "chat_id": chat_id,
                "text": _strip_tags(text),
                "disable_web_page_preview": True,
            }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        try:
            r = SESSION.post(url, data=payload, timeout=15)
            j = r.json()
            if j.get("ok"):
                return True
            desc = str(j.get("description", ""))
            logger.error("Telegram отклонил (mode=%s, chat=%s): %s",
                         mode, chat_id, desc)
            low = desc.lower()
            if any(k in low for k in ("blocked", "chat not found",
                                       "deactivated", "kicked")):
                remove_subscriber(int(chat_id))
                return False
        except Exception as e:
            logger.warning("Telegram send error chat=%s: %s", chat_id, e)
    return False


def split_text(text, limit=TG_MSG_LIMIT):
    chunks, cur = [], ""
    for line in text.split("\n"):
        if cur and len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = ""
        cur = (cur + "\n" + line) if cur else line
    if cur:
        chunks.append(cur)
    return chunks or [""]


def send_telegram(text, reply_markup=None):
    if not TELEGRAM_BOT_TOKEN:
        return
    with sub_lock:
        targets = list(subscribers)
    if not targets and TELEGRAM_CHAT_ID:
        targets = [TELEGRAM_CHAT_ID]
    for chunk in split_text(text):
        for cid in targets:
            _post_telegram(cid, chunk, reply_markup=reply_markup)


def load_subscribers():
    global subscribers
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                subscribers = set(int(x) for x in data.get("ids", []))
        except Exception as e:
            logger.error("load_subscribers error: %s", e)
            subscribers = set()
    if TELEGRAM_CHAT_ID:
        try:
            subscribers.add(int(TELEGRAM_CHAT_ID))
        except Exception:
            pass
    save_subscribers()


def save_subscribers():
    try:
        with sub_lock:
            data = {"ids": sorted(list(subscribers))}
        tmp = SUBSCRIBERS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, SUBSCRIBERS_FILE)
    except Exception as e:
        logger.error("save_subscribers error: %s", e)


def add_subscriber(chat_id):
    with sub_lock:
        if chat_id in subscribers:
            return False
        subscribers.add(int(chat_id))
    save_subscribers()
    return True


def remove_subscriber(chat_id):
    with sub_lock:
        if int(chat_id) not in subscribers:
            return False
        subscribers.discard(int(chat_id))
    save_subscribers()
    return True


# ==================== REST BYBIT ====================
def api_get(path, params, timeout=15, retries=3):
    url = "https://api.bybit.com" + path
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            j = r.json()
            if j.get("retCode") == 0:
                return j
            logger.debug("API retCode %s: %s",
                         j.get("retCode"), j.get("retMsg"))
            time.sleep(1)
        except Exception as e:
            logger.debug("API error %s: %s", path, e)
            time.sleep(1)
    return None


def fetch_klines(symbol, interval, limit=200):
    params = {"category": "spot", "symbol": symbol,
              "interval": interval, "limit": min(max(limit, 1), 1000)}
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
    if not out:
        return None
    return pd.DataFrame(out)


def get_price(symbol):
    j = api_get("/v5/market/tickers",
                {"category": "spot", "symbol": symbol})
    if not j:
        return None
    rows = j.get("result", {}).get("list", [])
    if not rows:
        return None
    try:
        return float(rows[0].get("lastPrice", 0))
    except Exception:
        return None


def get_universe():
    j = api_get("/v5/market/tickers", {"category": "spot"})
    if not j:
        return []
    pairs = []
    skipped_stable = 0
    for item in j.get("result", {}).get("list", []):
        symbol = item.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        if base in STABLE_BASES:
            skipped_stable += 1
            continue
        if base.endswith("USD"):
            skipped_stable += 1
            continue
        try:
            price = float(item.get("lastPrice", 0))
            turnover = float(item.get("turnover24h", 0)
                              or item.get("volume24h", 0))
        except Exception:
            continue
        if price >= MIN_PRICE and turnover >= MIN_TURNOVER_USDT:
            pairs.append((symbol, turnover))
    pairs.sort(key=lambda x: x[1], reverse=True)
    logger.info("Universe: %d pairs, skipped %d stable-like",
                min(len(pairs), TOP_N), skipped_stable)
    return [s for s, _ in pairs[:TOP_N]]


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
    tr = ranges.max(axis=1)
    return tr.rolling(period).mean()


def adx(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    tr = pd.concat([high - low, np.abs(high - close.shift()),
                    np.abs(low - close.shift())], axis=1).max(axis=1)
    atr_val = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    return dx.rolling(period).mean()


def analyze_frame(df, drop_last=False):
    if df is None or len(df) < 60:
        return None
    d = df.iloc[:-1] if drop_last and len(df) > 1 else df
    if len(d) < 50:
        return None
    c = d["close"]
    e9 = ema(c, 9).iloc[-1]
    e21 = ema(c, 21).iloc[-1]
    e20 = ema(c, 20).iloc[-1]
    e50 = ema(c, 50).iloc[-1]
    macd_line = ema(c, 12) - ema(c, 26)
    macd_signal = ema(macd_line, 9)
    hist = macd_line - macd_signal
    cross_fresh = False
    if len(hist) >= 4:
        for i in range(1, 4):
            if hist.iloc[-i - 1] <= 0 and hist.iloc[-i] > 0:
                cross_fresh = True
                break
    rsi_val = rsi(c).iloc[-1]
    adx_val = adx(d).iloc[-1]
    atr_val = atr(d).iloc[-1]
    vol_sma = d["volume"].rolling(20).mean().iloc[-1]
    vol_last = float(d["volume"].iloc[-1])
    if pd.isna(vol_sma) or vol_sma <= 0:
        vol_ratio = 0.0
    else:
        vol_ratio = vol_last / float(vol_sma)
    last = d.iloc[-1]
    values = [e9, e21, e20, e50, rsi_val, adx_val, atr_val, vol_ratio]
    for v in values:
        if pd.isna(v):
            return None
    return {
        "close": float(last["close"]), "high": float(last["high"]),
        "low": float(last["low"]),
        "ema9": float(e9), "ema21": float(e21),
        "ema20": float(e20), "ema50": float(e50),
        "rsi": float(rsi_val), "adx": float(adx_val),
        "atr": float(atr_val), "vol_ratio": float(vol_ratio),
        "macd_cross_fresh": bool(cross_fresh),
    }


def confirmed_df(symbol):
    with ohlc_lock:
        buf = OHLC.get(symbol)
        if not buf:
            return None
        rows = [dict(c) for c in buf if c.get("confirm")]
    if len(rows) < 60:
        return None
    df = pd.DataFrame(rows)
    df = df.drop_duplicates("start").sort_values("start").reset_index(drop=True)
    return df
    # ==================== ТРЕНД / BTC / BREADTH ====================
def get_trend(symbol, force=False):
    now = time.time()
    with trend_lock:
        item = TREND_CACHE.get(symbol)
        if not force and item and now - item.get("ts", 0) < 1800:
            return item
    df4 = fetch_klines(symbol, "240", 120)
    dfd = fetch_klines(symbol, "D", 120)
    a4 = analyze_frame(df4, drop_last=True)
    ad = analyze_frame(dfd, drop_last=True)
    score = 0
    if a4 and a4["ema9"] > a4["ema21"]:
        score += 1
    bull_1d = False
    if ad and ad["ema20"] > ad["ema50"]:
        score += 1
        bull_1d = True
    item = {
        "ts": now, "score": score,
        "adx_4h": float(a4["adx"]) if a4 else 0.0,
        "adx_1d": float(ad["adx"]) if ad else 0.0,
        "bull_1d": bull_1d,
    }
    with trend_lock:
        TREND_CACHE[symbol] = item
    return item


def btc_allows_longs():
    now = time.time()
    with btc_lock:
        if now - BTC_CACHE.get("ts", 0) < 300:
            return BTC_CACHE["ok"], BTC_CACHE["reason"]
    df = fetch_klines("BTCUSDT", "60", 20)
    a = analyze_frame(df, drop_last=True)
    ok, reason = True, "OK"
    try:
        closes = df.iloc[:-1]["close"]
        if len(closes) >= 7:
            chg_6h = (float(closes.iloc[-1]) / float(closes.iloc[-7]) - 1.0) * 100.0
        else:
            chg_6h = 0.0
    except Exception:
        chg_6h = 0.0
    if chg_6h <= BTC_DROP_6H_PCT:
        ok, reason = False, f"BTC {chg_6h:.1f}%/6h"
    elif a and a["ema9"] < a["ema21"] and a["adx"] > BTC_ADX_BLOCK:
        ok, reason = False, f"BTC dt ADX{a['adx']:.0f}"
    with btc_lock:
        BTC_CACHE["ts"] = now
        BTC_CACHE["ok"] = ok
        BTC_CACHE["reason"] = reason
    return ok, reason


def alt_breadth_allows_longs():
    if not ALT_BREADTH_ENABLED:
        return True, "off"
    now = time.time()
    with breadth_lock:
        if now - BREADTH_CACHE.get("ts", 0) < 120:
            return BREADTH_CACHE["ok"], BREADTH_CACHE["reason"]
    rsis = []
    with analysis_lock:
        for sym, item in LAST_ANALYSIS.items():
            a = item.get("analysis")
            if a and a.get("rsi"):
                rsis.append(float(a["rsi"]))
    if len(rsis) < ALT_BREADTH_MIN_SAMPLES:
        with breadth_lock:
            BREADTH_CACHE["ts"] = now
            BREADTH_CACHE["ok"] = True
            BREADTH_CACHE["reason"] = f"n/a ({len(rsis)})"
        return True, f"n/a ({len(rsis)})"
    median_rsi = float(np.median(rsis))
    low_pct = sum(1 for r in rsis if r < 40) / len(rsis) * 100.0
    ok = True
    reason = f"OK medRSI={median_rsi:.0f} low={low_pct:.0f}%"
    if median_rsi < ALT_BREADTH_MEDIAN_RSI:
        ok = False
        reason = f"alt panic medRSI={median_rsi:.0f}"
    elif low_pct > ALT_BREADTH_LOW_PCT:
        ok = False
        reason = f"alt panic low={low_pct:.0f}%"
    with breadth_lock:
        BREADTH_CACHE["ts"] = now
        BREADTH_CACHE["ok"] = ok
        BREADTH_CACHE["reason"] = reason
    return ok, reason


# ==================== СОСТОЯНИЕ ====================
def save_state():
    try:
        with state_lock:
            data = dict(state)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logger.error("save_state error: %s", e)


def load_state():
    global state
    if not os.path.exists(STATE_FILE):
        state = {}
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        logger.critical("load_state error: %s", e)
        state = {}
    now = time.time()
    with state_lock:
        for sym, pos in list(state.items()):
            if pos.get("position") == "open":
                if float(pos.get("atr_ref", 0)) <= 0:
                    entry = float(pos.get("entry_price", 0))
                    stop = float(pos.get("stop", 0))
                    if entry > 0 and stop > 0 and entry > stop:
                        pos["atr_ref"] = (entry - stop) / ATR_MULT_SL
                    else:
                        pos["position"] = "closed"
                        pos["last_exit_ts"] = now
                        pos["last_exit_reason"] = "invalid_state"


def save_cb():
    try:
        with cb_lock:
            data = dict(cb)
        tmp = CB_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, CB_FILE)
    except Exception as e:
        logger.error("save_cb error: %s", e)


def load_cb():
    if not os.path.exists(CB_FILE):
        return
    try:
        with open(CB_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if loaded.get("day") != today:
            loaded["day"] = today
            loaded["day_pnl"] = 0.0
            loaded["consec"] = 0
        with cb_lock:
            cb.update(loaded)
    except Exception as e:
        logger.warning("load_cb error: %s", e)


def append_trade(trade):
    try:
        with file_lock:
            trades = []
            if os.path.exists(TRADES_FILE):
                try:
                    with open(TRADES_FILE, "r", encoding="utf-8") as f:
                        trades = json.load(f)
                except Exception:
                    trades = []
            trades.append(trade)
            trades = trades[-2000:]
            tmp = TRADES_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(trades, f, indent=2)
            os.replace(tmp, TRADES_FILE)
    except Exception as e:
        logger.error("append_trade error: %s", e)


def cb_can_trade():
    with cb_lock:
        return time.time() >= float(cb.get("paused_until", 0.0))


def can_enter(symbol):
    now = time.time()
    if not cb_can_trade():
        return False
    with state_lock:
        open_count = sum(1 for p in state.values()
                         if p.get("position") == "open")
        if open_count >= MAX_OPEN_POSITIONS:
            return False
        old = state.get(symbol, {})
        if old.get("position") == "open":
            return False
        if now - float(old.get("last_entry_ts", 0.0)) < ENTRY_COOLDOWN_SECONDS:
            return False
        if now - float(old.get("last_loss_ts", 0.0)) < PAIR_LOSS_COOLDOWN_SECONDS:
            return False
    with trade_times_lock:
        trade_times[:] = [t for t in trade_times if now - t < 3600]
        if len(trade_times) >= MAX_TRADES_PER_HOUR:
            return False
        entry_30[:] = [t for t in entry_30 if now - t < 1800]
        if len(entry_30) >= MAX_TRADES_PER_30MIN:
            return False
    return True


# ==================== СИГНАЛЫ ====================
def make_signal(symbol, strategy, entry, atr_value, score, parts,
                entry_low=None, entry_high=None, expiry_sec=1800):
    if entry <= 0 or atr_value <= 0:
        return None
    stop = entry - atr_value * ATR_MULT_SL
    target = entry + atr_value * ATR_MULT_TP
    risk_pct = (entry - stop) / entry * 100.0
    if risk_pct < MIN_STOP_DISTANCE_PCT:
        stop = entry * (1.0 - MIN_STOP_DISTANCE_PCT / 100.0)
        risk_pct = MIN_STOP_DISTANCE_PCT
        target = entry + (entry - stop) * (ATR_MULT_TP / ATR_MULT_SL)
    if stop >= entry or target <= entry:
        return None
    rr = (target - entry) / (entry - stop)
    if rr < MIN_RR:
        return None
    if entry_low is None:
        entry_low = entry * ENTRY_TOLERANCE_LOW
    if entry_high is None:
        entry_high = entry * ENTRY_TOLERANCE_HIGH
    return {
        "symbol": symbol, "strategy": strategy,
        "entry": float(entry), "stop": float(stop),
        "target": float(target), "atr": float(atr_value),
        "score": int(score), "parts": list(parts),
        "risk_pct": float(risk_pct),
        "rr": float(rr),
        "entry_low": float(entry_low), "entry_high": float(entry_high),
        "expires": time.time() + float(expiry_sec),
    }


def arm_signal(symbol, sig):
    if not sig:
        return
    now = time.time()
    with state_lock:
        pos = state.get(symbol, {})
        if pos.get("position") == "open":
            return
        if now - float(pos.get("last_entry_ts", 0.0)) < ENTRY_COOLDOWN_SECONDS:
            return
        if now - float(pos.get("last_loss_ts", 0.0)) < PAIR_LOSS_COOLDOWN_SECONDS:
            return
    with armed_lock:
        ARMED[symbol] = sig
    logger.info("ARM %s %s score=%s zone=%.8g-%.8f",
                symbol, sig["strategy"], sig["score"],
                sig["entry_low"], sig["entry_high"])


def expire_armed():
    now = time.time()
    with armed_lock:
        for sym, sig in list(ARMED.items()):
            if now > float(sig.get("expires", 0)):
                ARMED.pop(sym, None)
                # ==================== ОТКРЫТИЕ ПОЗИЦИИ ====================
def open_position(sig):
    symbol = sig["symbol"]
    if not can_enter(symbol):
        return False
    now = time.time()
    with state_lock:
        state[symbol] = {
            "position": "open",
            "strategy": sig["strategy"],
            "entry_price": sig["entry"],
            "stop": sig["stop"],
            "target": sig["target"],
            "atr_ref": sig["atr"],
            "score": sig["score"],
            "risk_pct": sig["risk_pct"],
            "rr": sig["rr"],
            "entry_ts": now,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "last_entry_ts": now,
            "last_loss_ts": 0.0,
            "last_exit_ts": 0.0,
            "partial_done": False,       # технический флаг trailing
            "breakeven_moved": False,    # v22.4: отдельный флаг BE
            "highest": sig["entry"],
            "last_reversal_check": 0.0,
        }
        save_state()
    with trade_times_lock:
        trade_times.append(now)
        entry_30.append(now)
    with armed_lock:
        ARMED.pop(symbol, None)
    parts_txt = esc(" / ".join(sig["parts"]))
    sl_pct = (sig["stop"] - sig["entry"]) / sig["entry"] * 100
    tp_pct = (sig["target"] - sig["entry"]) / sig["entry"] * 100
    send_telegram(
        f"🟢 <b>ВХОД {sig['strategy'].upper()}</b>\n"
        f"Пара: {tv_link(symbol)}\n"
        f"Цена: {sig['entry']:.8f}\n"
        f"SL: {sig['stop']:.8f} ({sl_pct:+.2f}%)\n"
        f"TP: {sig['target']:.8f} ({tp_pct:+.2f}%)\n"
        f"Score: {sig['score']} · RR: {sig['rr']:.2f}\n"
        f"<i>{parts_txt}</i>"
    )
    logger.info("OPEN %s %s entry=%.8f sl=%.8f tp=%.8f score=%s",
                symbol, sig["strategy"], sig["entry"],
                sig["stop"], sig["target"], sig["score"])
    return True


# ==================== RECORD TRADE ====================
def record_trade(symbol, pos, exit_price, reason, force_close=False):
    """
    v22.4: fraction всегда 1.0 (без частичных).
    contribution_pct = pnl_pct (size=1.0).
    """
    entry = float(pos.get("entry_price", 0.0))
    if entry <= 0:
        return None
    pnl_pct = (float(exit_price) - entry) / entry * 100.0
    contribution_pct = pnl_pct  # size=1.0
    now = time.time()
    trade = {
        "time": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "strategy": pos.get("strategy", "?"),
        "entry": entry,
        "exit": float(exit_price),
        "reason": reason,
        "fraction": 1.0,
        "pnl_pct": pnl_pct,
        "contribution_pct": contribution_pct,
    }
    append_trade(trade)
    with cb_lock:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if cb.get("day") != today:
            cb["day"] = today
            cb["day_pnl"] = 0.0
            cb["consec"] = 0
        cb["day_pnl"] = float(cb.get("day_pnl", 0.0)) + contribution_pct
        if pnl_pct < 0:
            cb["consec"] = int(cb.get("consec", 0)) + 1
            pos["last_loss_ts"] = now
        else:
            cb["consec"] = 0
        if cb["consec"] >= CB_CONSEC_LOSSES or cb["day_pnl"] <= CB_DAILY_LOSS_PCT:
            cb["paused_until"] = now + CB_PAUSE_SECONDS
            logger.warning(
                "CIRCUIT BREAKER: consec=%s day_pnl=%.2f%% pause=%s min",
                cb["consec"], cb["day_pnl"], CB_PAUSE_SECONDS // 60)
        save_cb()
    if force_close:
        pos["last_exit_ts"] = now
        pos["position"] = "closed"
    return pnl_pct


# ==================== ЗАКРЫТИЕ ====================
def close_position(symbol, price, reason, force_close=True):
    """
    v22.4: без fraction, всегда полное закрытие.
    Формат: только PnL по цене.
    """
    with state_lock:
        pos = state.get(symbol)
        if not pos or pos.get("position") != "open":
            return
        if pos.get("closing_lock_ts"):
            if time.time() - float(pos["closing_lock_ts"]) < 5:
                return
        pos["closing_lock_ts"] = time.time()
        result = record_trade(symbol, pos, price, reason,
                              force_close=force_close)
        if result is None:
            pos.pop("closing_lock_ts", None)
            state[symbol] = pos
            return
        pnl_pct = result
        entry = float(pos.get("entry_price", 0.0))
        state[symbol] = pos
        save_state()
    icon = "🟢" if pnl_pct > 0 else "🔴"
    send_telegram(
        f"{icon} <b>ВЫХОД · {reason}</b>\n"
        f"Пара: {tv_link(symbol)}\n"
        f"Вход: {entry:.8f} → Выход: {float(price):.8f}\n"
        f"PnL по цене: <b>{pnl_pct:+.2f}%</b>"
    )
    logger.info("CLOSE %s %s price=%.8f pnl=%.2f%%",
                symbol, reason, float(price), pnl_pct)


# ==================== ПРОВЕРКА ЦЕНЫ ====================
def check_position_price(symbol, price):
    if not price or price <= 0:
        return
    with state_lock:
        pos = state.get(symbol)
        if not pos or pos.get("position") != "open":
            return
        entry = float(pos.get("entry_price", 0.0))
        atr_ref = float(pos.get("atr_ref", 0.0))
        stop = float(pos.get("stop", 0.0))
        target = float(pos.get("target", 0.0))
        be_moved = bool(pos.get("breakeven_moved", False))
        highest = max(float(pos.get("highest", entry)), float(price))
        if entry <= 0 or atr_ref <= 0:
            return
        pos["highest"] = highest
        state[symbol] = pos

    # Стоп (полное закрытие)
    if price <= stop:
        reason = "BE/Trailing Stop" if be_moved else "Stop-Loss"
        close_position(symbol, stop, reason, force_close=True)
        return

    # Тейк (полное закрытие)
    if price >= target:
        close_position(symbol, target, "Take-Profit", force_close=True)
        return

    # v22.4: BE при +0.5R вместо partial TP
    if (not be_moved
            and price >= entry + BREAKEVEN_AT_R * atr_ref):
        with state_lock:
            pos = state.get(symbol)
            if not pos or pos.get("breakeven_moved"):
                return
            old_stop = float(pos.get("stop", 0))
            new_stop = max(old_stop, entry * 1.001)
            if new_stop > old_stop:
                pos["stop"] = new_stop
                pos["breakeven_moved"] = True
                pos["partial_done"] = True  # для совместимости (trailing)
                state[symbol] = pos
                save_state()
                pnl_now = (price - entry) / entry * 100
                send_telegram(
                    f"🔒 <b>СТОП В БЕЗУБЫТОК</b>\n"
                    f"Пара: {tv_link(symbol)}\n"
                    f"Вход: {entry:.8f} → текущая: {float(price):.8f}\n"
                    f"PnL сейчас: <b>{pnl_now:+.2f}%</b>\n"
                    f"Новый стоп: {new_stop:.8f} (безубыток)"
                )
                logger.info("BE moved %s entry=%.8f new_stop=%.8f",
                            symbol, entry, new_stop)
        return

    # Trailing после BE
    if be_moved:
        new_stop = highest - TRAILING_STEP_ATR * atr_ref
        floor = entry * 1.001
        with state_lock:
            pos = state.get(symbol)
            if pos and pos.get("position") == "open":
                if new_stop > float(pos.get("stop", 0)):
                    pos["stop"] = max(new_stop, floor)
                    state[symbol] = pos
                    save_state()


def check_armed_price(symbol, price):
    if not price or price <= 0:
        return
    now = time.time()
    with armed_lock:
        sig = ARMED.get(symbol)
        if not sig:
            return
        if now > float(sig.get("expires", 0)):
            ARMED.pop(symbol, None)
            return
        if price < float(sig.get("entry_low", 0)):
            return
        if price > float(sig.get("entry_high", 0)):
            return
        new_sig = make_signal(
            symbol, sig["strategy"], price, sig["atr"],
            sig["score"], sig["parts"], expiry_sec=60)
    if new_sig:
        open_position(new_sig)


def try_realtime_breakout(symbol, price):
    if not price or price <= 0:
        return
    with cons_lock:
        level = CONS_LEVEL.get(symbol)
    if not level or price <= level * 1.001:
        return
    with state_lock:
        pos = state.get(symbol, {})
        if pos.get("position") == "open":
            return
    with armed_lock:
        if symbol in ARMED:
            return
    with analysis_lock:
        ana = LAST_ANALYSIS.get(symbol)
    if not ana or time.time() - float(ana.get("ts", 0)) > 3600:
        return
    atr_value = float(ana.get("atr", 0))
    if atr_value <= 0:
        return
    with ohlc_lock:
        buf = list(OHLC.get(symbol, []))
    if len(buf) < 21:
        return
    current = buf[-1]
    confirmed = [c for c in buf[:-1] if c.get("confirm")]
    if len(confirmed) < 20:
        return
    try:
        avg_vol = float(np.mean([float(c.get("volume", 0))
                                 for c in confirmed[-20:]]))
        cur_vol = float(current.get("volume", 0))
    except Exception:
        return
    if avg_vol <= 0:
        return
    vol_ratio = cur_vol / avg_vol
    if vol_ratio < BREAKOUT_RT_VOL_MULT:
        return
    trend = get_trend(symbol)
    if trend["score"] < 2 or not trend["bull_1d"]:
        return
    parts = [
        f"Realtime breakout level={level:.8g}",
        f"Vol {vol_ratio:.1f}x",
        f"Trend {trend['score']}/2",
    ]
    sig = make_signal(symbol, "breakout_rt", price, atr_value, 8,
                      parts, expiry_sec=60)
    if sig:
        open_position(sig)
        # ==================== СТРАТЕГИИ ====================
def evaluate_signal(symbol, a, trend, cons_upper=None):
    if not a or a["atr"] <= 0:
        return None
    if trend["score"] < 2:
        return None
    entry = float(a["close"])
    atr_value = float(a["atr"])
    best = None

    # ---------- CONFLUENCE ----------
    score = 0
    parts = []
    if trend["score"] == 2:
        score += 2
        parts.append("Trend 4h+1d +2")
    if a["macd_cross_fresh"]:
        score += 2
        parts.append("MACD cross fresh +2")
    if a["ema9"] > a["ema21"]:
        score += 1
        parts.append("EMA9>21 +1")
    if RSI_MIN <= a["rsi"] <= RSI_MAX:
        score += 1
        parts.append(f"RSI {a['rsi']:.0f} +1")
    if ADX_MIN <= trend["adx_4h"] <= ADX_MAX:
        score += 1
        parts.append(f"ADX4h {trend['adx_4h']:.0f} +1")
    if a["vol_ratio"] >= MIN_VOL_MULT:
        score += 1
        parts.append(f"Vol {a['vol_ratio']:.1f}x +1")
    if score >= MIN_SIGNAL_SCORE:
        best = make_signal(
            symbol, "confluence", entry, atr_value, score, parts,
            entry_low=entry * ENTRY_TOLERANCE_LOW,
            entry_high=entry * ENTRY_TOLERANCE_HIGH,
            expiry_sec=ARM_EXPIRY_CONFLUENCE)

    # ---------- PULLBACK ----------
    pb_score = 0
    pb_parts = []
    if trend["score"] == 2:
        pb_score += 2
        pb_parts.append("Trend 4h+1d +2")
    touched_ema21 = a["low"] <= a["ema21"] * 1.004
    recovered = a["close"] > a["ema9"]
    if touched_ema21 and recovered:
        pb_score += 2
        pb_parts.append("Pullback EMA21 + recovery +2")
    if 35 <= a["rsi"] <= 62:
        pb_score += 1
        pb_parts.append(f"RSI {a['rsi']:.0f} +1")
    if trend["adx_4h"] >= 18:
        pb_score += 1
        pb_parts.append(f"ADX4h {trend['adx_4h']:.0f} +1")
    if a["vol_ratio"] >= 1.0:
        pb_score += 1
        pb_parts.append(f"Vol {a['vol_ratio']:.1f}x +1")
    if pb_score >= MIN_SIGNAL_SCORE:
        sig = make_signal(
            symbol, "pullback", entry, atr_value, pb_score, pb_parts,
            entry_low=entry * ENTRY_TOLERANCE_LOW,
            entry_high=entry * ENTRY_TOLERANCE_HIGH,
            expiry_sec=ARM_EXPIRY_PULLBACK)
        if sig and (best is None or sig["score"] > best["score"]):
            best = sig

    # ---------- BREAKOUT ----------
    if cons_upper and entry > cons_upper and a["vol_ratio"] >= BREAKOUT_VOL_MULT:
        bo_score = 8
        bo_parts = [
            f"Breakout {CONSOL_DAYS}d +5",
            f"Vol {a['vol_ratio']:.1f}x +2",
            "1D bull +1",
        ]
        sig = make_signal(
            symbol, "breakout", entry, atr_value, bo_score, bo_parts,
            entry_low=cons_upper * BREAKOUT_ENTRY_LOW,
            entry_high=cons_upper * BREAKOUT_ENTRY_HIGH,
            expiry_sec=ARM_EXPIRY_BREAKOUT)
        if sig and (best is None or sig["score"] >= best["score"]):
            best = sig

    return best


def process_ws_closed(symbol):
    df = confirmed_df(symbol)
    if df is None or len(df) < 80:
        return
    a = analyze_frame(df, drop_last=False)
    if not a:
        return
    with analysis_lock:
        LAST_ANALYSIS[symbol] = {
            "ts": time.time(),
            "atr": a["atr"],
            "analysis": a,
        }
    trend = get_trend(symbol)
    with cons_lock:
        cons_upper = CONS_LEVEL.get(symbol)
    sig = evaluate_signal(symbol, a, trend, cons_upper)
    if sig:
        arm_signal(symbol, sig)


def detect_consolidation(symbol):
    dfd = fetch_klines(symbol, "D", 80)
    if dfd is None or len(dfd) < CONSOL_DAYS + 5:
        return None
    d = dfd.iloc[:-1]
    if len(d) < CONSOL_DAYS:
        return None
    window = d.tail(CONSOL_DAYS)
    try:
        upper = float(window["high"].max())
        lower = float(window["low"].min())
        adx_val = float(adx(d).iloc[-1])
    except Exception:
        return None
    if lower <= 0 or upper <= lower:
        return None
    range_pct = (upper - lower) / lower * 100.0
    if range_pct > CONSOL_MAX_RANGE_PCT:
        return None
    if range_pct < CONSOL_MIN_RANGE_PCT:
        return None
    if pd.isna(adx_val):
        adx_val = 0.0
    return {
        "upper": upper, "lower": lower,
        "days": CONSOL_DAYS, "range_pct": range_pct,
        "adx": adx_val,
    }


def scan_cycle():
    global CANDIDATES, CONSOLIDATIONS, LAST_SCAN_TS
    btc_ok, btc_reason = btc_allows_longs()
    breadth_ok, breadth_reason = alt_breadth_allows_longs()
    with pairs_lock:
        pairs = list(PAIRS)
    candidates = []
    consolidations = []
    logger.info("Scan started: pairs=%d btc=%s breadth=%s",
                len(pairs), btc_reason, breadth_reason)

    for symbol in pairs:
        try:
            df15 = fetch_klines(symbol, "15", 180)
            a15 = analyze_frame(df15, drop_last=True)
            trend = get_trend(symbol)
            cons = detect_consolidation(symbol)
            cons_upper = None
            if cons:
                cons_upper = cons["upper"]
                with cons_lock:
                    CONS_LEVEL[symbol] = cons_upper
                try:
                    cur_price = CURRENT_PRICE.get(symbol) or (a15["close"] if a15 else 0)
                    dist_pct = ((cons_upper - cur_price) / cur_price * 100.0
                                if cur_price > 0 else 999)
                except Exception:
                    dist_pct = 999
                consolidations.append({
                    "symbol": symbol,
                    "upper": cons_upper,
                    "lower": cons["lower"],
                    "days": cons["days"],
                    "range_pct": cons["range_pct"],
                    "adx": cons["adx"],
                    "price": cur_price,
                    "dist_pct": dist_pct,
                })
            if a15:
                sig = evaluate_signal(symbol, a15, trend, cons_upper)
                if sig and btc_ok and breadth_ok:
                    arm_signal(symbol, sig)
                candidates.append({
                    "symbol": symbol,
                    "price": a15["close"],
                    "score": max(2 if trend["score"] == 2 else trend["score"], 0),
                    "trend": trend["score"],
                    "rsi": a15["rsi"],
                    "adx_4h": trend["adx_4h"],
                    "vol": a15["vol_ratio"],
                    "atr": a15["atr"],
                    "strategy": sig["strategy"] if sig else "watch",
                    "armed": symbol in ARMED,
                })
            time.sleep(0.12)
        except Exception as e:
            logger.debug("scan %s error: %s", symbol, e)

    candidates.sort(key=lambda x: (-x.get("score", 0), x.get("rsi", 100)))
    consolidations.sort(key=lambda x: x.get("dist_pct", 999))
    with scan_lock:
        CANDIDATES = candidates[:30]
        CONSOLIDATIONS = consolidations[:30]
    LAST_SCAN_TS = time.time()
    logger.info("Scan done: candidates=%d cons=%d btc=%s breadth=%s",
                len(CANDIDATES), len(CONSOLIDATIONS),
                btc_reason, breadth_reason)
    update_ticker_subscription()


# ==================== ПЕРИОДИЧЕСКИЕ ВЫХОДЫ ====================
def periodic_exit_checks():
    now = time.time()
    with state_lock:
        open_items = [(s, dict(p)) for s, p in state.items()
                      if p.get("position") == "open"]
    for symbol, pos in open_items:
        price = CURRENT_PRICE.get(symbol)
        if not price:
            price = get_price(symbol)
        if not price or price <= 0:
            continue
        entry = float(pos.get("entry_price", 0))
        atr_ref = float(pos.get("atr_ref", 0))
        entry_ts = float(pos.get("entry_ts", now))
        if entry <= 0 or atr_ref <= 0:
            continue
        age_hours = (now - entry_ts) / 3600.0
        if age_hours >= TIME_STOP_HOURS:
            r_multiple = (price - entry) / atr_ref
            if r_multiple < 1.0:
                close_position(symbol, price, "Time Stop", force_close=True)
                continue
        if now - float(pos.get("last_reversal_check", 0)) > 900:
            with state_lock:
                cur = state.get(symbol)
                if cur and cur.get("position") == "open":
                    cur["last_reversal_check"] = now
                    state[symbol] = cur
                    save_state()
            df4 = fetch_klines(symbol, "240", 80)
            a4 = analyze_frame(df4, drop_last=True)
            if a4 and a4["ema9"] < a4["ema21"] and price > entry * 1.005:
                close_position(symbol, price, "EMA reversal 4h",
                               force_close=True)
                # ==================== СТАТУС ====================
def send_status():
    with state_lock:
        opens = [(s, dict(p)) for s, p in state.items()
                 if p.get("position") == "open"]
    with cb_lock:
        now = time.time()
        paused_until = float(cb.get("paused_until", 0.0))
        paused = now < paused_until
        pause_min = max(0, int((paused_until - now) / 60)) if paused else 0
        day_pnl = float(cb.get("day_pnl", 0.0))
        consec = int(cb.get("consec", 0))
    btc_ok, btc_reason = btc_allows_longs()
    breadth_ok, breadth_reason = alt_breadth_allows_longs()
    with scan_lock:
        cands = list(CANDIDATES)
        cons = list(CONSOLIDATIONS)
        scan_age = int(now - LAST_SCAN_TS) if LAST_SCAN_TS else 0
    with armed_lock:
        armed_count = len(ARMED)
    with ws_ticker_lock:
        ticker_count = len(WS_TICKER_PAIRS)
    with pairs_lock:
        kline_count = len(PAIRS)

    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")

    lines = []
    lines.append(f"📡 <b>СТАТУС v22.4 OBSERVE</b> | <i>{now_str} UTC</i>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🔹 Пар WS kline: <b>{kline_count}</b> · "
                 f"WS tickers: <b>{ticker_count}/{WS_TICKER_MAX}</b>")
    lines.append(f"🔹 BTC: <b>{esc(btc_reason)}</b>")
    lines.append(f"🔹 Breadth: <b>{esc(breadth_reason)}</b>")
    cb_str = f"⏸ пауза {pause_min}м" if paused else "OK"
    lines.append(
        f"🔹 CB: <b>{cb_str}</b> · сегодня {day_pnl:+.2f}% · "
        f"серия {consec}")
    lines.append(
        f"🔹 Позиций: <b>{len(opens)}/{MAX_OPEN_POSITIONS}</b> · "
        f"armed: <b>{armed_count}</b> · scan: {scan_age}s")
    lines.append(
        f"🔹 Лимиты: {MAX_TRADES_PER_30MIN}/30м · "
        f"{MAX_TRADES_PER_HOUR}/час · score≥{MIN_SIGNAL_SCORE} · "
        f"RR≥{MIN_RR:.1f}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━")

    if opens:
        lines.append("💰 <b>ОТКРЫТЫЕ ПОЗИЦИИ</b>")
        for s, p in opens:
            entry = float(p.get("entry_price", 0))
            cur = CURRENT_PRICE.get(s, entry)
            pnl_live = (cur - entry) / entry * 100 if entry > 0 else 0
            pnl_icon = "🟢" if pnl_live > 0 else "🔴"
            tag = "🎯" if p.get("strategy") == "confluence" else (
                  "🌊" if p.get("strategy") == "pullback" else "🚀")
            be = " · 🔒BE" if p.get("breakeven_moved") else ""
            lines.append(
                f"{tag} {tv_link(s)} "
                f"{entry:.6g} → {cur:.6g} · "
                f"🛑{float(p.get('stop', 0)):.6g} "
                f"🎯{float(p.get('target', 0)):.6g} · "
                f"{pnl_icon}{pnl_live:+.2f}%{be}")
    else:
        lines.append("💰 Позиций нет")

    lines.append("")
    lines.append("🔵🔵🔵 <b>ТОП КАНДИДАТОВ</b> 🔵🔵🔵")
    lines.append("<blockquote expandable>")
    if cands:
        for i, c in enumerate(cands[:10], 1):
            armed = " ⏳" if c.get("armed") else ""
            score = c.get("score", 0)
            mark = "🟢" if score >= 6 else ("🟡" if score >= 4 else "")
            lines.append(
                f"{mark}{i}.{tv_link(c['symbol'])} "
                f"<b>{c.get('strategy', 'watch')}</b> "
                f"T{c.get('trend', 0)} "
                f"ADX{c.get('adx_4h', 0):.0f} "
                f"RSI{c.get('rsi', 0):.0f} "
                f"V{c.get('vol', 0):.1f}x "
                f"💰{c.get('price', 0):.6g}"
                f"{armed}")
    else:
        lines.append("— нет кандидатов")
    lines.append("</blockquote>")

    lines.append("")
    lines.append("🟡🟡🟡 <b>МОНЕТЫ В БОКОВИКЕ (20 ДНЕЙ)</b> 🟡🟡🟡")
    lines.append("<blockquote expandable>")
    if cons:
        for i, c in enumerate(cons[:10], 1):
            dist = c.get("dist_pct", 999)
            if dist <= 1:
                mark = "🔥"
            elif dist <= 3:
                mark = "⚡"
            elif dist <= 5:
                mark = "🟢"
            else:
                mark = ""
            lines.append(
                f"{mark}{i}.{tv_link(c['symbol'])} "
                f"{c.get('days', 0)}д "
                f"{c.get('range_pct', 0):.1f}% "
                f"ADX{c.get('adx', 0):.0f} "
                f"🚀{c.get('upper', 0):.6g} "
                f"💰{c.get('price', 0):.6g} ({dist:.1f}%)")
    else:
        lines.append("— нет боковиков")
    lines.append("</blockquote>")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🛡 v22.4 OBSERVE: size 100% · без partial · BE +0.5R")
    lines.append("🎯 Confluence · Pullback · Breakout")
    lines.append("🔧 BTC/ETH/stables off · Alt Breadth блок при medRSI<38")
    lines.append("/status · /help · /start · /stop")

    send_telegram("\n".join(lines))
# ==================== WEBSOCKET KLINE ====================
def touch_ws_ts(kind):
    global LAST_WS_KLINE_TS, LAST_WS_TICKER_TS
    now = time.time()
    with ws_ts_lock:
        if kind == "kline":
            LAST_WS_KLINE_TS = now
        else:
            LAST_WS_TICKER_TS = now


def ws_ping(ws, name):
    while not STOP_EVENT.is_set():
        time.sleep(20)
        try:
            ws.send(json.dumps({"op": "ping"}))
        except Exception:
            logger.info("WS %s ping stopped", name)
            return


def on_kline_open(ws):
    logger.info("WS kline connected")
    WS_KLINE_CONNECTED.set()
    touch_ws_ts("kline")
    with pairs_lock:
        pairs = list(PAIRS)
    if not pairs:
        logger.warning("WS kline: нет пар для подписки")
        return
    args = [f"kline.15.{p}" for p in pairs]
    for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
        chunk = args[i:i + WS_SUBSCRIBE_CHUNK]
        try:
            ws.send(json.dumps({"op": "subscribe", "args": chunk}))
        except Exception as e:
            logger.warning("WS kline subscribe error: %s", e)
            return
        time.sleep(WS_SUBSCRIBE_PAUSE)
    threading.Thread(target=ws_ping, args=(ws, "kline"), daemon=True).start()


def on_kline_message(ws, message):
    touch_ws_ts("kline")
    try:
        data = json.loads(message)
        topic = data.get("topic", "")
        if not topic.startswith("kline."):
            return
        symbol = topic.split(".")[-1]
        rows = data.get("data") or []
        for k in rows:
            try:
                start = int(k["start"])
                item = {
                    "start": start,
                    "open": float(k["open"]),
                    "high": float(k["high"]),
                    "low": float(k["low"]),
                    "close": float(k["close"]),
                    "volume": float(k["volume"]),
                    "confirm": bool(k.get("confirm", False)),
                }
            except Exception:
                continue
            with ohlc_lock:
                buf = OHLC.get(symbol)
                if buf is None:
                    buf = deque(maxlen=WS_KLINE_BUFFER)
                    OHLC[symbol] = buf
                if buf and buf[-1]["start"] == start:
                    buf[-1] = item
                else:
                    buf.append(item)
            with price_lock:
                CURRENT_PRICE[symbol] = item["close"]
            if item["confirm"] and LAST_PROCESSED.get(symbol) != start:
                LAST_PROCESSED[symbol] = start
                process_ws_closed(symbol)
    except Exception as e:
        logger.debug("WS kline message error: %s", e)


def on_kline_error(ws, error):
    logger.warning("WS kline error: %s", error)


def on_kline_close(ws, close_status_code, close_msg):
    logger.warning("WS kline closed: %s %s", close_status_code, close_msg)
    WS_KLINE_CONNECTED.clear()


def run_kline_ws():
    global WS_KLINE_APP
    while not STOP_EVENT.is_set():
        with pairs_lock:
            has_pairs = bool(PAIRS)
        if not has_pairs:
            time.sleep(5)
            continue
        try:
            WS_KLINE_APP = websocket.WebSocketApp(
                WS_URL,
                on_open=on_kline_open,
                on_message=on_kline_message,
                on_error=on_kline_error,
                on_close=on_kline_close,
            )
            WS_KLINE_APP.run_forever(ping_interval=0, ping_timeout=None)
        except Exception as e:
            logger.error("WS kline critical error: %s", e)
        WS_KLINE_CONNECTED.clear()
        time.sleep(5)


# ==================== WEBSOCKET TICKERS ====================
def desired_ticker_pairs():
    pairs = []
    with state_lock:
        pairs.extend([s for s, p in state.items()
                      if p.get("position") == "open"])
    with armed_lock:
        pairs.extend(list(ARMED.keys()))
    with scan_lock:
        pairs.extend([c["symbol"] for c in CANDIDATES[:20]])
        pairs.extend([c["symbol"] for c in CONSOLIDATIONS[:20]])
    unique = []
    seen = set()
    for s in pairs:
        if s and s not in seen:
            seen.add(s)
            unique.append(s)
    return unique[:WS_TICKER_MAX]


def subscribe_tickers(ws, symbols):
    if not symbols:
        return
    args = [f"tickers.{s}" for s in symbols]
    for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
        chunk = args[i:i + WS_SUBSCRIBE_CHUNK]
        try:
            ws.send(json.dumps({"op": "subscribe", "args": chunk}))
        except Exception as e:
            logger.warning("WS tickers subscribe error: %s", e)
            return
        time.sleep(WS_SUBSCRIBE_PAUSE)


def unsubscribe_tickers(ws, symbols):
    if not symbols:
        return
    args = [f"tickers.{s}" for s in symbols]
    for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
        chunk = args[i:i + WS_SUBSCRIBE_CHUNK]
        try:
            ws.send(json.dumps({"op": "unsubscribe", "args": chunk}))
        except Exception as e:
            logger.warning("WS tickers unsubscribe error: %s", e)
            return
        time.sleep(0.05)


def update_ticker_subscription():
    desired = desired_ticker_pairs()
    with ws_ticker_lock:
        current = set(WS_TICKER_PAIRS)
        new = [s for s in desired if s not in current]
        gone = [s for s in current if s not in desired]
        WS_TICKER_PAIRS.clear()
        WS_TICKER_PAIRS.update(desired)
    if WS_TICKER_APP and WS_TICKERS_CONNECTED.is_set():
        if gone:
            unsubscribe_tickers(WS_TICKER_APP, gone)
        if new:
            subscribe_tickers(WS_TICKER_APP, new)


def on_tickers_open(ws):
    logger.info("WS tickers connected")
    WS_TICKERS_CONNECTED.set()
    touch_ws_ts("ticker")
    with ws_ticker_lock:
        pairs = list(WS_TICKER_PAIRS)
    if not pairs:
        pairs = desired_ticker_pairs()
        with ws_ticker_lock:
            WS_TICKER_PAIRS.clear()
            WS_TICKER_PAIRS.update(pairs)
    subscribe_tickers(ws, pairs)
    threading.Thread(target=ws_ping, args=(ws, "tickers"), daemon=True).start()


def on_tickers_message(ws, message):
    touch_ws_ts("ticker")
    try:
        data = json.loads(message)
        topic = data.get("topic", "")
        if not topic.startswith("tickers."):
            return
        symbol = topic.split(".")[-1]
        d = data.get("data") or {}
        try:
            price = float(d.get("lastPrice", 0))
        except Exception:
            return
        if price <= 0:
            return
        with price_lock:
            CURRENT_PRICE[symbol] = price
        check_position_price(symbol, price)
        check_armed_price(symbol, price)
        try_realtime_breakout(symbol, price)
    except Exception as e:
        logger.debug("WS tickers message error: %s", e)


def on_tickers_error(ws, error):
    logger.warning("WS tickers error: %s", error)


def on_tickers_close(ws, close_status_code, close_msg):
    logger.warning("WS tickers closed: %s %s", close_status_code, close_msg)
    WS_TICKERS_CONNECTED.clear()


def run_tickers_ws():
    global WS_TICKER_APP
    while not STOP_EVENT.is_set():
        try:
            WS_TICKER_APP = websocket.WebSocketApp(
                WS_URL,
                on_open=on_tickers_open,
                on_message=on_tickers_message,
                on_error=on_tickers_error,
                on_close=on_tickers_close,
            )
            WS_TICKER_APP.run_forever(ping_interval=0, ping_timeout=None)
        except Exception as e:
            logger.error("WS tickers critical error: %s", e)
        WS_TICKERS_CONNECTED.clear()
        time.sleep(5)


def watchdog_loop():
    while not STOP_EVENT.is_set():
        time.sleep(30)
        now = time.time()
        with ws_ts_lock:
            kline_silence = now - LAST_WS_KLINE_TS if LAST_WS_KLINE_TS else 9999
            ticker_silence = now - LAST_WS_TICKER_TS if LAST_WS_TICKER_TS else 9999
        if kline_silence > WS_SILENCE_TIMEOUT and WS_KLINE_APP is not None:
            logger.warning("Watchdog kline: тишина %.0fс — перезапуск", kline_silence)
            try:
                WS_KLINE_APP.close()
            except Exception:
                pass
        if ticker_silence > WS_SILENCE_TIMEOUT and WS_TICKER_APP is not None:
            logger.warning("Watchdog tickers: тишина %.0fс — перезапуск", ticker_silence)
            try:
                WS_TICKER_APP.close()
            except Exception:
                pass


# ==================== POLLING TELEGRAM ====================
def polling_loop():
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("Polling не запущен: нет TELEGRAM_BOT_TOKEN")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = 0
    logger.info("Polling запущен")
    while not STOP_EVENT.is_set():
        try:
            r = SESSION.get(url, params={
                "offset": offset, "timeout": 30,
                "allowed_updates": json.dumps(["message"]),
            }, timeout=40)
            j = r.json()
            if not j.get("ok"):
                desc = str(j.get("description", "")).lower()
                if "conflict" in desc:
                    logger.warning("Telegram polling conflict, sleep 30")
                    time.sleep(30)
                else:
                    time.sleep(5)
                continue
            for upd in j.get("result", []):
                offset = max(offset, int(upd.get("update_id", 0)) + 1)
                msg = upd.get("message") or {}
                cid = (msg.get("chat") or {}).get("id")
                text = (msg.get("text") or "").strip().lower()
                if not cid or not text:
                    continue
                if text == "/start":
                    if add_subscriber(int(cid)):
                        _post_telegram(cid,
                            "🟢 <b>Подписка оформлена</b>\n"
                            "v22.4 OBSERVE запущен.\n"
                            "/stop — отписаться, /help — справка.")
                    else:
                        _post_telegram(cid, "✅ Вы уже подписаны.")
                elif text == "/stop":
                    if remove_subscriber(int(cid)):
                        _post_telegram(cid, "🔴 Вы отписаны.\n/start — подписаться снова.")
                    else:
                        _post_telegram(cid, "Вы и так не подписаны.")
                elif text == "/help":
                    _post_telegram(cid,
                        "📡 <b>Bybit Scanner v22.4 OBSERVE</b>\n"
                        "Стратегии: Confluence, Pullback, Breakout.\n"
                        "BTC/ETH/stables off.\n"
                        "Alt Breadth: medRSI&lt;38 → блок.\n"
                        "Score≥6 · RR≥2.0 · 1 вход/30мин.\n"
                        "Size = 100% депо (наблюдение).\n"
                        "Без partial TP, BE при +0.5R.\n"
                        "Команды: /start, /stop, /help, /status.")
                elif text == "/status":
                    threading.Thread(target=send_status, daemon=True).start()
        except Exception as e:
            logger.warning("Polling error: %s", e)
            time.sleep(5)


# ==================== MAIN ====================
def manage_loop():
    now = time.time()
    next_scan = now + 5
    next_status = now + 120
    next_ticker_refresh = now + 30
    next_periodic = now + 10
    while not STOP_EVENT.is_set():
        now = time.time()
        try:
            if now >= next_scan:
                scan_cycle()
                next_scan = now + SCAN_INTERVAL_SECONDS
            if now >= next_status:
                send_status()
                next_status = now + STATUS_INTERVAL_SECONDS
            if now >= next_ticker_refresh:
                update_ticker_subscription()
                next_ticker_refresh = now + 300
            if now >= next_periodic:
                periodic_exit_checks()
                next_periodic = now + 20
            expire_armed()
        except Exception as e:
            logger.exception("Manage loop error: %s", e)
        time.sleep(MANAGE_SLEEP_SECONDS)


def handle_stop(signum, frame):
    logger.info("Получен сигнал %s — останавливаюсь", signum)
    STOP_EVENT.set()
    save_state()
    raise SystemExit(0)


def main():
    setup_logging()
    load_state()
    load_cb()
    load_subscribers()
    logger.info("Bybit Scanner v22.4 OBSERVE starting")
    pairs = get_universe()
    if not pairs:
        logger.critical("Не удалось получить вселенную пар")
        raise SystemExit(1)
    with pairs_lock:
        PAIRS[:] = pairs
    logger.info("Universe loaded: %d pairs (BTC/ETH/stables excluded)",
                len(PAIRS))
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    threading.Thread(target=run_kline_ws, daemon=True).start()
    threading.Thread(target=run_tickers_ws, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()
    threading.Thread(target=polling_loop, daemon=True).start()
    send_telegram(
        "🟢 <b>Bybit Scanner v22.4 OBSERVE запущен</b>\n"
        "WS kline 15m + WS tickers.\n"
        "Стратегии: Confluence / Pullback / Breakout.\n"
        "BTC/ETH/stables — off.\n"
        "Size = 100% депо (режим наблюдения).\n"
        "БЕЗ partial TP. BE при +0.5R.\n"
        "Score≥6 · RR≥2.0 · 1 вход/30мин.\n"
        "Alt Breadth: medRSI<38 → блок."
    )
    manage_loop()


if __name__ == "__main__":
    main()
