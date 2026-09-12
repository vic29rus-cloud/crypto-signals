#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
BYBIT SCANNER v20.1 «WS-REALTIME-HOT-X» — ЕДИНЫЙ ФАЙЛ ДЛЯ LINUX VPS
WebSocket (wss://stream.bybit.com/v5/public/spot) + REST (api.bybit.com/v5)
Бумажная торговля: сделки -> bybit_trades.json, алерты -> Telegram

НОВОЕ В v20.1 (относительно v20.0):
• Раздельные квоты WS-tickers: 20 кандидатов + 20 боковиков (X-режим)
• Расширенный пул «горячих»: не топ-10, а все близкие к кроссу/пробою
• Пороги в константах: WS_HOT_NEAR_CROSS_PCT, WS_HOT_NEAR_BREAKOUT_PCT
• Мягкая пересборка пула раз в 15 мин из ohlc_buffers (без REST)
• Приоритет слотов: cand → cons → добор из недобора
• Всё остальное как в v20.0: PEAK-GUARD, BTC-адаптив, WS kline+tickers
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

# ==================== КОНФИГУРАЦИЯ ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
BASE_URL = "https://api.bybit.com/v5/market"
WS_URL = "wss://stream.bybit.com/v5/public/spot"

TOP_N = 200
TOTAL_PAIRS = 700
SCAN_INTERVAL_SECONDS = 7200
MIN_TURNOVER_USD = 50_000
CONSOLIDATION_MIN_TURNOVER_USD = 20_000

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
BREAKOUT_VOL_ACCUM_MULT = 1.0

CONSOLIDATION_WINDOWS = (30, 45, 60)
CONSOLIDATION_MAX_RANGE_PCT = 24.0
CONSOLIDATION_MAX_ADX = 25

WS_EXTRA_SYMBOLS = 50

RETEST_ENABLED = True
RETEST_TOUCH_TOLERANCE_PCT = 0.8
RETEST_SL_ATR = 1.5
RETEST_TP_ATR = 4.0

# --- BTC-ФИЛЬТР ---
BTC_CONTEXT_ENABLED = True
BTC_CONTEXT_TTL = 300
BTC_DROP_6H_PCT = -5.0
BTC_ADX_BLOCK_THRESHOLD = 35
BTC_ALLOW_STRONG_WHEN_BLOCKED = True
BTC_STRONG_MIN_TREND_SCORE = 3
BTC_STRONG_MIN_SCORE = 8
BTC_STRONG_SIZE_MULT = 0.5

CB_MAX_CONSEC_LOSSES = 4
CB_PAUSE_SECONDS = 6 * 3600
CB_DAILY_LOSS_PCT = 3.0

MAX_SECTOR_POSITIONS = 2
SECTOR_MAP = {
    "SOL": "L1", "AVAX": "L1", "NEAR": "L1", "APT": "L1", "ATOM": "L1",
    "FTM": "L1", "ALGO": "L1", "DOT": "L1", "ADA": "L1", "XRP": "L1",
    "SUI": "L1", "SEI": "L1", "TIA": "L1", "TON": "L1", "TRX": "L1",
    "ARB": "L2", "OP": "L2", "MATIC": "L2", "IMX": "L2", "STRK": "L2",
    "UNI": "DeFi", "AAVE": "DeFi", "MKR": "DeFi", "COMP": "DeFi",
    "SUSHI": "DeFi", "CRV": "DeFi", "SNX": "DeFi", "LDO": "DeFi",
    "DYDX": "DEX", "GMX": "DEX", "JUP": "DEX",
    "DOGE": "Meme", "SHIB": "Meme", "PEPE": "Meme", "FLOKI": "Meme",
    "WIF": "Meme", "BONK": "Meme", "MEME": "Meme",
    "FET": "AI", "RNDR": "AI", "AGIX": "AI", "TAO": "AI", "ARKM": "AI",
    "SAND": "Gaming", "MANA": "Gaming", "AXS": "Gaming", "GALA": "Gaming",
    "FIL": "Storage", "AR": "Storage",
}

RISK_PER_TRADE_PCT = 1.0
MAX_PORTFOLIO_RISK_PCT = 6.0
SESSION_FILTER_ENABLED = True

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
PULLBACK_MAX_DEPTH_PCT = 0.05
PULLBACK_MIN_PRIOR_MOVE_PCT = 0.03

STATUS_RSI_MIN, STATUS_RSI_MAX = 35, 75
STATUS_ADX_MIN, STATUS_ADX_MAX = 18, 55
STATUS_MIN_PRICE = 0.0001
STATUS_READY_SCORE = 3

# --- 🛡 PEAK-GUARD ---
PEAK_GUARD_ENABLED = True
PEAK_LOOKBACK_CANDLES = 20
PEAK_MAX_POSITION_PCT = 85
PEAK_MIN_DIST_TO_MAX_PCT = 1.5
PEAK_MAX_RSI = 70
PEAK_MAX_DRIFT_PCT = 1.0

# --- ПОРОГИ «ГОРЯЧЕСТИ» БОКОВИКОВ (для отображения) ---
HOT_DIST_PCT_1 = 1.0
HOT_DIST_PCT_2 = 3.0
HOT_DIST_PCT_3 = 5.0

MAX_SHOW_CANDIDATES = 10
MAX_SHOW_CONSOLIDATIONS = 10

# --- ⚡ WS-REALTIME-HOT (v20.1, X-режим) ---
WS_TICKERS_ENABLED = True
WS_TICKERS_MAX_PAIRS = 40               # общий максимум
WS_TICKERS_QUOTA_CAND = 20              # квота кандидатов
WS_TICKERS_QUOTA_CONS = 20              # квота боковиков
WS_TICKERS_REFRESH_SEC = 900            # мягкая пересборка пула (15 мин)
WS_HOT_COOLDOWN_SEC = 60                # cooldown на попытку входа по одной паре
WS_HOT_MIN_RR = 1.5
WS_HOT_MIN_VOL = 1.2
WS_HOT_BREAKOUT_VOL = 1.8
WS_HOT_MAX_DRIFT_PCT = 1.0

# --- Пороги «близости» для попадания в пул (v20.1) ---
WS_HOT_NEAR_CROSS_PCT = 0.5             # |macd_gap_pct| <= 0.5% → кандидат «на грани»
WS_HOT_NEAR_BREAKOUT_PCT = 5.0          # % до пробоя <= 5% → боковик «почти готов»

TREND_CACHE_REFRESH_SECONDS = 1800
TREND_CACHE_INITIAL_LIMIT = 60
BREAKOUT_CACHE_SIZE = 60
CLEANUP_AFTER_DAYS = 14
WS_SILENCE_TIMEOUT = 90

TG_MSG_LIMIT = 4096
TG_SAFE_LIMIT = 4000

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(WORK_DIR, "bybit_state.json")
TRADES_LOG_FILE = os.path.join(WORK_DIR, "bybit_trades.json")
SUBSCRIBERS_FILE = os.path.join(WORK_DIR, "subscribers.json")
LOG_FILE = os.path.join(WORK_DIR, "bybit_scanner.log")

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
retest_memory = {}
last_ws_msg_ts = time.time()
WS_APP = None
PAIRS_WS = []
SESSION = requests.Session()

market_context = {"ok": True, "reason": "", "ts": 0.0}
market_context_lock = threading.RLock()
cb_state = {"consec": 0, "paused_until": 0.0, "day": "", "day_pnl": 0.0}
cb_lock = threading.Lock()

SUBSCRIBERS = set()
subscribers_lock = threading.RLock()
MAIN_CHAT_ID = None

# ⚡ Расширенные пулы «горячих» пар (v20.1, X-режим)
# В каждом пуле — список dict'ов, отсортированных по «близости».
HOT_PAIRS_CACHE = {
    "candidates_pool": [],      # все кандидаты с |macd_gap| <= порога
    "consolidations_pool": [],  # все боковики с % до пробоя <= порога
    "candidates": [],           # топ-10 для отображения в статусе
    "consolidations": [],       # топ-10 для отображения в статусе
    "ts": 0.0,
}
HOT_PAIRS_LOCK = threading.RLock()
HOT_LAST_CHECK = {}   # symbol -> ts последней попытки входа

# ⚡ WebSocket tickers — текущие подписки
WS_TICKER_PAIRS = set()
WS_TICKER_PAIRS_LOCK = threading.RLock()
WS_TICKERS_APP = None
WS_TICKERS_CONNECTED = threading.Event()

# 🛡 PEAK-GUARD: кэш последних проверок (для дедупликации алертов)
LAST_PEAK_ALERT = {}  # symbol -> ts

_http_failures = 0
_http_open_until = 0.0

# ==================== HTTP-СЛОЙ ====================
def http_get(url, params=None, timeout=20, retries=3):
    global _http_failures, _http_open_until
    if time.time() < _http_open_until:
        return None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = 2 ** attempt
                logger.warning("HTTP %s от Bybit, повтор через %d c", resp.status_code, wait)
                time.sleep(wait)
                continue
            _http_failures = 0
            return resp.json()
        except Exception as e:
            logger.warning("Сетевая ошибка %s (попытка %d): %s", url, attempt + 1, e)
            time.sleep(2 ** attempt)
    _http_failures += 1
    if _http_failures >= 5:
        _http_open_until = time.time() + 60
        _http_failures = 0
        logger.error("HTTP circuit breaker: пауза 60 c")
    return None

def api_ok(data):
    return bool(data) and data.get("retCode") == 0

# ==================== CIRCUIT BREAKER ====================
def cb_register(net_pnl):
    with cb_lock:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if cb_state["day"] != today:
            cb_state["day"], cb_state["day_pnl"] = today, 0.0
        cb_state["day_pnl"] += net_pnl
        if net_pnl < 0:
            cb_state["consec"] += 1
            if cb_state["consec"] >= CB_MAX_CONSEC_LOSSES:
                cb_state["paused_until"] = time.time() + CB_PAUSE_SECONDS
                logger.warning("Circuit Breaker: %d убытков подряд — пауза %d ч",
                               cb_state["consec"], CB_PAUSE_SECONDS // 3600)
        else:
            cb_state["consec"] = 0
        if cb_state["day_pnl"] <= -CB_DAILY_LOSS_PCT:
            midnight = (datetime.now(timezone.utc).replace(hour=0, minute=0,
                        second=0, microsecond=0) + pd.Timedelta(days=1))
            cb_state["paused_until"] = max(cb_state["paused_until"], midnight.timestamp())
            logger.error("Circuit Breaker: дневной лимит убытка %.1f%%", cb_state["day_pnl"])

def cb_can_trade():
    with cb_lock:
        return time.time() >= cb_state["paused_until"]

# ==================== BTC-КОНТЕКСТ ====================
def refresh_market_context():
    ok, reason = True, ""
    if BTC_CONTEXT_ENABLED:
        try:
            candles = fetch_klines("BTCUSDT", "60", 48)
            if candles and len(candles) >= 10:
                closes = [c["close"] for c in candles]
                chg_6h = (closes[-2] / closes[-8] - 1.0) * 100 if len(closes) > 8 else 0.0
                df = pd.DataFrame(candles)
                ef = ema(df["close"], 9).iloc[-2]
                es = ema(df["close"], 21).iloc[-2]
                adx_val = adx(df).iloc[-2]
                if chg_6h <= BTC_DROP_6H_PCT:
                    ok, reason = False, f"BTC {chg_6h:.1f}% за 6ч"
                elif ef < es and not pd.isna(adx_val) and adx_val > BTC_ADX_BLOCK_THRESHOLD:
                    ok, reason = False, f"BTC нисходящий тренд (ADX>{BTC_ADX_BLOCK_THRESHOLD:.0f})"
        except Exception as e:
            logger.debug("market context: %s", e)
    with market_context_lock:
        market_context.update(ok=ok, reason=reason, ts=time.time())
    if not ok:
        logger.info("BTC-контекст: лонги заблокированы (%s)", reason)

def market_context_loop():
    while True:
        refresh_market_context()
        time.sleep(BTC_CONTEXT_TTL)

def market_allows_longs():
    if not BTC_CONTEXT_ENABLED:
        return True
    with market_context_lock:
        return market_context["ok"]

def is_strong_signal_for_blocked_market(signal):
    if not BTC_ALLOW_STRONG_WHEN_BLOCKED:
        return False
    trend_score = signal.get("trend_score", 0)
    score = signal.get("score", 0)
    if isinstance(score, str):
        return False
    return (trend_score >= BTC_STRONG_MIN_TREND_SCORE
            and score >= BTC_STRONG_MIN_SCORE)

# ==================== 🛡 PEAK-GUARD ====================
def check_peak_guard(df, entry_price):
    if not PEAK_GUARD_ENABLED:
        return True, "", {}
    try:
        if df is None or len(df) < PEAK_LOOKBACK_CANDLES:
            return True, "", {}
        window = df.tail(PEAK_LOOKBACK_CANDLES)
        local_high = float(window["high"].max())
        local_low = float(window["low"].min())
        if local_high <= local_low or entry_price <= 0:
            return True, "", {}
        position_pct = (entry_price - local_low) / (local_high - local_low) * 100
        dist_to_max_pct = (local_high - entry_price) / entry_price * 100
        metrics = {"position_pct": round(position_pct, 1),
                   "dist_to_max_pct": round(dist_to_max_pct, 2),
                   "local_high": local_high, "local_low": local_low}
        if position_pct > PEAK_MAX_POSITION_PCT:
            return False, f"цена в верхних {100 - PEAK_MAX_POSITION_PCT:.0f}% ({position_pct:.0f}%)", metrics
        if dist_to_max_pct < PEAK_MIN_DIST_TO_MAX_PCT:
            return False, f"слишком близко к максимуму ({dist_to_max_pct:.2f}%)", metrics
        return True, "", metrics
    except Exception as e:
        logger.debug("peak_guard error: %s", e)
        return True, "", {}

def check_peak_guard_rsi(rsi_val):
    if not PEAK_GUARD_ENABLED:
        return True, ""
    if rsi_val is None or pd.isna(rsi_val):
        return True, ""
    if rsi_val > PEAK_MAX_RSI:
        return False, f"RSI {rsi_val:.0f} > {PEAK_MAX_RSI}"
    return True, ""

def check_peak_guard_drift(current_price, entry_price):
    if not PEAK_GUARD_ENABLED:
        return True, ""
    if entry_price <= 0 or current_price <= 0:
        return True, ""
    drift_pct = abs(current_price - entry_price) / entry_price * 100
    if drift_pct > PEAK_MAX_DRIFT_PCT:
        return False, f"цена ушла на {drift_pct:.1f}%"
    return True, ""

# ==================== СЕКТОРА И РАЗМЕР ====================
def sector_of(symbol):
    base = symbol.replace("USDT", "")
    return SECTOR_MAP.get(base)

def sector_slot_free(symbol):
    sec = sector_of(symbol)
    if sec is None:
        return True
    cnt = sum(1 for p, v in state.items()
              if v.get("position") == "open" and sector_of(p) == sec)
    return cnt < MAX_SECTOR_POSITIONS

def position_size_fraction(score, rr, atr_pct, portfolio_risk_pct, stop_dist_pct,
                            btc_blocked=False):
    base = RISK_PER_TRADE_PCT / 100.0
    conf = (0.5 + (score / 10.0) * 0.5) if isinstance(score, int) else 0.7
    vol_mult = 0.5 if atr_pct > 5 else 0.7 if atr_pct > 3 else 0.85 if atr_pct > 1.5 else 1.0
    rr_mult = 1.0 if rr >= 3 else 0.9 if rr >= 2 else 0.7 if rr >= 1.5 else 0.5
    size = base * conf * vol_mult * rr_mult
    if btc_blocked:
        size *= BTC_STRONG_SIZE_MULT
    remaining = MAX_PORTFOLIO_RISK_PCT - portfolio_risk_pct
    max_add = remaining / stop_dist_pct if stop_dist_pct > 0 else 0.0
    size = min(size, max(max_add, 0.0))
    return round(max(size, 0.01), 3) if max_add >= 0.01 else 0.01

def portfolio_risk_used():
    total = 0.0
    for v in state.values():
        if v.get("position") == "open" and v.get("entry_price"):
            dist = (v["entry_price"] - v.get("stop", 0)) / v["entry_price"] * 100
            total += dist * v.get("size_fraction", 1.0)
    return total

# ==================== TELEGRAM: ПОДПИСЧИКИ ====================
def _load_subscribers():
    global SUBSCRIBERS, MAIN_CHAT_ID
    ids = set()
    if TELEGRAM_CHAT_ID:
        try:
            MAIN_CHAT_ID = int(TELEGRAM_CHAT_ID)
            ids.add(MAIN_CHAT_ID)
        except (TypeError, ValueError):
            pass
    try:
        if os.path.exists(SUBSCRIBERS_FILE):
            with open(SUBSCRIBERS_FILE, "r") as f:
                ids.update(int(x) for x in json.load(f).get("ids", []))
    except Exception as e:
        logger.error("Ошибка загрузки подписчиков: %s", e)
    with subscribers_lock:
        SUBSCRIBERS = ids
    logger.info("Подписчиков загружено: %d", len(ids))

def _save_subscribers():
    with subscribers_lock:
        extras = sorted(x for x in SUBSCRIBERS if x != MAIN_CHAT_ID)
    try:
        tmp = SUBSCRIBERS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"ids": extras}, f)
        os.replace(tmp, SUBSCRIBERS_FILE)
    except Exception as e:
        logger.error("Ошибка сохранения подписчиков: %s", e)

def add_subscriber(cid):
    with subscribers_lock:
        if cid in SUBSCRIBERS:
            return False
        SUBSCRIBERS.add(cid)
    _save_subscribers()
    logger.info("➕ Новый подписчик: %s (всего %d)", cid, len(SUBSCRIBERS))
    return True

def remove_subscriber(cid):
    with subscribers_lock:
        if cid not in SUBSCRIBERS or cid == MAIN_CHAT_ID:
            return False
        SUBSCRIBERS.discard(cid)
    _save_subscribers()
    logger.info("➖ Отписка: %s (осталось %d)", cid, len(SUBSCRIBERS))
    return True

def _post_telegram(chat_id, text):
    if not TELEGRAM_BOT_TOKEN:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for parse_mode in ("HTML", None):
        payload = {"chat_id": chat_id, "text": text,
                   "disable_web_page_preview": True}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        try:
            r = requests.post(url, data=payload, timeout=10)
            resp = r.json()
            if resp.get("ok"):
                return True
            desc = resp.get("description", "")
            logger.error("Telegram отклонил (parse=%s, chat=%s): %s",
                         parse_mode, chat_id, desc)
            low = desc.lower()
            if any(k in low for k in ("blocked", "chat not found",
                                       "deactivated", "kicked")):
                remove_subscriber(chat_id)
                return False
        except Exception as e:
            logger.error("Ошибка отправки Telegram chat=%s: %s", chat_id, e)
    return False

def _split_html_safe(text, limit=TG_SAFE_LIMIT):
    chunks, cur = [], ""
    for line in text.split("\n"):
        if cur and len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = ""
        cur = (cur + "\n" + line) if cur else line
    if cur:
        chunks.append(cur)
    return chunks or [""]

def _send_to_all_one(text):
    if len(text) > TG_MSG_LIMIT:
        cut = TG_MSG_LIMIT - 10
        text = text[:cut].rstrip()
        if "<blockquote" in text and "</blockquote>" not in text.rsplit("<blockquote", 1)[1]:
            text += "\n</blockquote>"
        text += " …"
    with subscribers_lock:
        targets = list(SUBSCRIBERS)
    for cid in targets:
        _post_telegram(cid, text)

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN:
        return
    for chunk in _split_html_safe(text):
        with subscribers_lock:
            targets = list(SUBSCRIBERS)
        for cid in targets:
            _post_telegram(cid, chunk)

def tv_link(symbol: str) -> str:
    url = f"https://www.tradingview.com/chart/?symbol=BYBIT:{symbol}"
    return f'<a href="{url}">📈 {symbol}</a>'

HELP_TEXT = (
    "📡 <b>Bybit Scanner v20.1 — справка</b>\n"
    "Бот шлёт: входы/выходы, частичные TP и ОДИН статус каждые 2 часа.\n"
    "⚡ WS-REALTIME-HOT-X: 20 кандидатов + 20 боковиков в реальном времени.\n"
    "🛡 PEAK-GUARD: не входим на пике (RSI≤70, дрейф≤1%).\n"
    "🔥≤1% ⚡≤3% 🟢≤5% — сортировка боковиков по % до пробоя.\n"
    "💰 текущая · 🎯~ вход · 🚀 пробой · ✨ готов · 🥀 объём↓ · ⛔ пик\n"
    "Команды: /stop — отписаться, /help — справка."
)

def polling_loop():
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("Polling не запущен: нет TELEGRAM_BOT_TOKEN")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = 0
    logger.info("Polling /start запущен")
    while True:
        try:
            r = requests.get(url, params={"offset": offset, "timeout": 30,
                                          "allowed_updates": '["message"]'},
                             timeout=40)
            data = r.json()
            if not data.get("ok"):
                desc = data.get("description", "")
                logger.error("getUpdates ошибка: %s", desc)
                if "conflict" in desc.lower():
                    logger.error("Конфликт с webhook — polling остановлен")
                    return
                time.sleep(5)
                continue
            for u in data.get("result", []):
                offset = max(offset, u.get("update_id", 0) + 1)
                msg = u.get("message") or {}
                cid = (msg.get("chat") or {}).get("id")
                text = (msg.get("text") or "").strip().lower()
                if not cid or not text:
                    continue
                if text == "/start":
                    if add_subscriber(int(cid)):
                        _post_telegram(cid,
                            "🟢 <b>Подписка оформлена!</b>\n"
                            "Бот присылает входы/выходы и статус каждые 2 часа.\n"
                            "/stop — отписаться, /help — справка.")
                    else:
                        _post_telegram(cid, "✅ Вы уже подписаны.")
                elif text == "/stop":
                    if remove_subscriber(int(cid)):
                        _post_telegram(cid, "🔴 Вы отписаны.\n/start — подписаться снова.")
                    else:
                        _post_telegram(cid, "Вы и так не подписаны.")
                elif text == "/help":
                    _post_telegram(cid, HELP_TEXT)
        except Exception as e:
            logger.warning("Polling ошибка: %s", e)
            time.sleep(3)

# ==================== СОСТОЯНИЕ И СДЕЛКИ ====================
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
              entry_time=None, size_fraction=1.0, traded_fraction=1.0):
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
        costs = (FEE_PCT * 2 + SLIPPAGE_PCT) * traded_fraction
        net_pnl = raw_pnl * traded_fraction - costs
        trades.append({
            "symbol": symbol, "entry_price": entry, "exit_price": exit_price,
            "entry_time": entry_time, "exit_time": now_iso,
            "size_fraction": round(size_fraction, 3),
            "raw_pnl_pct": round(raw_pnl, 2),
            "net_pnl_pct": round(net_pnl, 2),
            "pnl_pct": round(net_pnl, 2),
            "result": "win" if net_pnl > 0 else "loss",
            "reason": reason, "strategy": strategy,
        })
        with open(TRADES_LOG_FILE, "w") as f:
            json.dump(trades, f, indent=2)
        if traded_fraction >= 1.0:
            cb_register(net_pnl)
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

# ==================== БОКОВИКИ ====================
def _find_cons_window(closed):
    for days in CONSOLIDATION_WINDOWS:
        if len(closed) < days:
            continue
        window = closed.tail(days)
        mean = window["close"].mean()
        if mean <= 0:
            continue
        range_pct = (window["close"].max() - window["close"].min()) / mean * 100
        if range_pct <= CONSOLIDATION_MAX_RANGE_PCT:
            return window, range_pct
    return None, None

def detect_consolidation(df_daily):
    closed = df_daily.iloc[:-1]
    if len(closed) < 60:
        return None
    adx_val = adx(closed).iloc[-1]
    if pd.isna(adx_val) or adx_val >= CONSOLIDATION_MAX_ADX:
        return None
    window, range_pct = _find_cons_window(closed)
    if window is None:
        return None
    recent = closed["volume"].tail(5).mean()
    older = closed["volume"].iloc[-25:-5].mean()
    vol_trend = float(recent / older) if older and older > 0 else 1.0
    return {"days": len(window), "range_pct": range_pct, "adx": float(adx_val),
            "upper_level": float(window["high"].max()),
            "lower_level": float(window["low"].min()),
            "vol_trend": round(vol_trend, 2)}

def check_breakout(df_daily, current_price):
    closed = df_daily.iloc[:-1]
    if len(closed) < 60:
        return None
    adx_val = adx(closed).iloc[-1]
    if pd.isna(adx_val) or adx_val >= BREAKOUT_MAX_ADX:
        return None
    window, range_pct = _find_cons_window(closed)
    if window is None:
        return None
    high = window["high"].max()
    if current_price < high * (1 + BREAKOUT_MIN_TRIGGER_PCT / 100):
        return None
    if current_price > high * (1 + MAX_BREAKOUT_DISTANCE_PCT / 100):
        return None
    vol_base = closed["volume"].rolling(20).mean()
    vol_avg = vol_base.iloc[-2]
    last_vol = closed["volume"].iloc[-1]
    if not (vol_avg > 0 and last_vol > vol_avg * 1.8):
        return None
    accum = closed["volume"].tail(3).mean()
    base_old = vol_base.iloc[-4] if not pd.isna(vol_base.iloc[-4]) else vol_avg
    if not (base_old > 0 and accum > base_old * BREAKOUT_VOL_ACCUM_MULT):
        return None
    atr_val = atr(closed).iloc[-1]
    if pd.isna(atr_val) or atr_val <= 0:
        return None
    return {"days": len(window), "range_pct": range_pct, "adx": float(adx_val),
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

def can_enter(pair, signal=None, signal_risk_pct=None):
    now = time.time()
    trade_times[:] = [t for t in trade_times if now - t < 3600]
    if not cb_can_trade():
        return False
    if not market_allows_longs():
        if signal is None or not is_strong_signal_for_blocked_market(signal):
            return False
    old_state = state.get(pair, {})
    if old_state.get("position") == "open":
        return False
    if now - float(old_state.get("last_exit_ts", 0) or 0) < ENTRY_COOLDOWN_SECONDS:
        return False
    open_positions = sum(1 for p in state.values() if p.get("position") == "open")
    if open_positions >= MAX_OPEN_POSITIONS:
        return False
    if len(trade_times) >= MAX_TRADES_PER_HOUR:
        return False
    if not sector_slot_free(pair):
        return False
    if signal_risk_pct is not None:
        if portfolio_risk_used() + signal_risk_pct > MAX_PORTFOLIO_RISK_PCT:
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
FIAT_BASES = {"AUD", "GBP", "EUR", "CAD", "CHF", "JPY", "USD",
              "BRL", "MXN", "TRY", "ZAR", "INR", "SGD", "HKD"}
STABLECOINS = {"USDC", "USDT", "DAI", "PYUSD", "TUSD", "FDUSD", "AUSD", "EURR",
               "USDR", "FRNT", "EURQ", "USDPT", "BRL1", "EUROP", "USDTB",
               "USD1", "RLUSD", "EURT", "GUSD", "FRAX", "LUSD", "SUSD", "USDP"}
STOCK_TOKENS = {"AAPLX", "AMZNX", "GOOGLX", "MCDX", "TSLAX", "COINX"}
STABLE_SUBSTRINGS = ("USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "BRL", "MXN")

def is_crypto(base):
    if not base:
        return False
    if base in FIAT_BASES:
        return False
    if base.endswith("X") or base in STOCK_TOKENS:
        return False
    if base in STABLECOINS:
        return False
    if any(s in base for s in STABLE_SUBSTRINGS):
        return False
    return True

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
    return [it["symbol"] for it in fetch_spot_instruments()
            if it.get("status") == "Trading"
            and it.get("quoteCoin") == "USDT"
            and is_crypto(it.get("baseCoin", ""))]

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
        ranked = [(sym, tickers[sym]["turnover"])
                  for sym in candidates
                  if sym in tickers and tickers[sym]["turnover"] >= CONSOLIDATION_MIN_TURNOVER_USD]
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

# ==================== СКОРИНГ ВХОДА ====================
def find_fresh_cross(df, window=FRESH_CROSS_WINDOW):
    ml, ms = df["macd_line"], df["macd_signal"]
    for ago in range(0, window):
        i = len(df) - 2 - ago
        if i < 1:
            return None
        if ml.iloc[i - 1] <= ms.iloc[i - 1] and ml.iloc[i] > ms.iloc[i]:
            return ago
    return None

def _rr_ok(entry, stop, target, min_rr=1.5):
    risk = entry - stop
    if risk <= 0 or target <= entry:
        return False
    return (target - entry) / risk >= min_rr

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
    min_score = MIN_SIGNAL_SCORE
    if SESSION_FILTER_ENABLED and datetime.now(timezone.utc).hour < 7:
        min_score += 1
    if score < min_score:
        return None
    entry = float(df.iloc[-1]["close"])
    atr_value = float(sig["atr"])
    stop = entry - atr_value * ATR_MULT_SL
    target = entry + atr_value * ATR_MULT_TP
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    if not _rr_ok(entry, stop, target):
        return None
    return {"entry": entry, "stop": stop, "target": target, "atr": atr_value,
            "score": score, "parts": parts, "strategy": "ws_15m",
            "trend_score": ti["score"]}

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
    if sig["ema_slow"] <= df["ema_slow"].iloc[-6]:
        return None
    recent_high = df["high"].iloc[-11:-1].max()
    prior_low = df["low"].iloc[-21:-6].min()
    if sig["ema_slow"] > 0 and (recent_high - sig["low"]) / sig["ema_slow"] > PULLBACK_MAX_DEPTH_PCT:
        return None
    if prior_low > 0 and (recent_high - prior_low) / prior_low < PULLBACK_MIN_PRIOR_MOVE_PCT:
        return None
    if not (sig["close"] > sig["open"]):
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
    if not _rr_ok(entry, stop, target):
        return None
    return {"entry": entry, "stop": stop, "target": target, "atr": atr_value,
            "score": "PB", "trend_score": ti["score"],
            "parts": [f"Откат к EMA21 · тренд 3/3 · объём {vol_ratio:.1f}×"],
            "strategy": "ws_pullback"}

# ==================== BREAKOUT RETEST ====================
def evaluate_ws_retest(df, symbol, level):
    if not RETEST_ENABLED or not level or level <= 0 or len(df) < 25:
        return None
    mem = retest_memory.setdefault(symbol, {"above": False})
    prev_close = float(df["close"].iloc[-3])
    last = df.iloc[-2]
    if prev_close < level * 0.97:
        mem["above"] = False
    if prev_close > level and not mem["above"]:
        mem["above"] = True
    if not mem["above"]:
        return None
    touched = last["low"] <= level * (1 + RETEST_TOUCH_TOLERANCE_PCT / 100)
    crashed = last["close"] < level * 0.99
    recovered = last["close"] > level and last["close"] > last["open"]
    if not (touched and recovered and not crashed):
        return None
    atr_val = float(df["atr"].iloc[-2]) if not pd.isna(df["atr"].iloc[-2]) else 0.0
    if atr_val <= 0:
        return None
    entry = float(df.iloc[-1]["close"])
    if entry <= level:
        return None
    stop = level - RETEST_SL_ATR * atr_val
    target = entry + RETEST_TP_ATR * atr_val
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    if not _rr_ok(entry, stop, target):
        return None
    mem["above"] = False
    ti = get_trend(symbol)
    return {"entry": entry, "stop": stop, "target": target, "atr": atr_val,
            "score": "RT", "trend_score": (ti["score"] if ti else 0),
            "parts": [f"Retest уровня {level:.6f} · отскок"],
            "strategy": "breakout_retest"}

# ==================== ОТКРЫТИЕ ПОЗИЦИИ ====================
def open_position(symbol, sig, closed_start):
    btc_blocked = not market_allows_longs()
    if btc_blocked and not is_strong_signal_for_blocked_market(sig):
        return None
    risk_pct = (sig["entry"] - sig["stop"]) / sig["entry"] * 100
    if not can_enter(symbol, signal=sig, signal_risk_pct=risk_pct):
        return None
    old_state = state.get(symbol, {})
    if old_state.get("last_signal_candle") == closed_start:
        return None
    rr = (sig["target"] - sig["entry"]) / (sig["entry"] - sig["stop"])
    atr_pct = sig["atr"] / sig["entry"] * 100
    size = position_size_fraction(
        sig["score"] if isinstance(sig["score"], int) else 6,
        rr, atr_pct, portfolio_risk_used(), risk_pct,
        btc_blocked=btc_blocked,
    )
    new_state = old_state.copy()
    new_state.update({
        "position": "open",
        "entry_price": sig["entry"],
        "stop": sig["stop"],
        "target": sig["target"],
        "atr_ref": sig["atr"],
        "entry_time": datetime.now(timezone.utc).isoformat(),
        "last_signal_candle": closed_start,
        "last_entry_ts": time.time(),
        "strategy": sig["strategy"],
        "score": sig["score"],
        "size_fraction": size,
        "btc_blocked_entry": btc_blocked,
    })
    state[symbol] = new_state
    trade_times.append(time.time())
    save_state(state)
    return new_state

# ==================== БЫСТРЫЙ BREAKOUT ЧЕРЕЗ WS (kline) ====================
def try_ws_breakout(symbol, new_candle, df):
    with breakout_cache_lock:
        level = breakout_cache.get(symbol)
    if level is None or level <= 0:
        return None
    prev_close = float(df["close"].iloc[-3])
    last_close = float(df["close"].iloc[-2])
    vol_sma = df["volume"].rolling(20).mean()
    vol_ratio = (float(df["volume"].iloc[-2] / vol_sma.iloc[-2])
                 if not pd.isna(vol_sma.iloc[-2]) and vol_sma.iloc[-2] > 0 else 0.0)
    if not (prev_close < level <= last_close):
        return None
    if last_close > level * (1 + MAX_BREAKOUT_DISTANCE_PCT / 100):
        return None
    if vol_ratio < 1.8:
        return None
    atr_val = float(df["atr"].iloc[-2]) if not pd.isna(df["atr"].iloc[-2]) else 0.0
    if atr_val <= 0:
        return None
    entry = float(new_candle["close"])
    stop = entry - atr_val * ATR_MULT_SL
    target = entry + atr_val * ATR_MULT_TP
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    if not _rr_ok(entry, stop, target):
        return None
    closed_start = int(df.iloc[-2]["start"])
    risk_pct = (entry - stop) / entry * 100
    ti = get_trend(symbol)
    trend_score = ti["score"] if ti else 0
    pseudo_sig = {"trend_score": trend_score, "score": 7}
    btc_blocked = not market_allows_longs()
    if btc_blocked and not is_strong_signal_for_blocked_market(pseudo_sig):
        return None
    with state_lock:
        if not can_enter(symbol, signal=pseudo_sig, signal_risk_pct=risk_pct):
            return None
        old_state = state.get(symbol, {})
        if old_state.get("last_signal_candle") == closed_start:
            return None
        rr = (target - entry) / (entry - stop)
        size = position_size_fraction(7, rr, atr_val / entry * 100,
                                      portfolio_risk_used(), risk_pct,
                                      btc_blocked=btc_blocked)
        new_state = old_state.copy()
        new_state.update({
            "position": "open",
            "entry_price": entry,
            "stop": stop,
            "target": target,
            "atr_ref": atr_val,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "last_signal_candle": closed_start,
            "last_entry_ts": time.time(),
            "strategy": "breakout_ws",
            "score": "BWS",
            "size_fraction": size,
            "btc_blocked_entry": btc_blocked,
        })
        state[symbol] = new_state
        trade_times.append(time.time())
        save_state(state)
    return {"entry": entry, "stop": stop, "target": target,
            "vol_ratio": vol_ratio, "level": level}

# ==================== ⚡ WS-REALTIME-HOT-X (v20.1) ====================
def _cand_hotness(s):
    """Меньше = горячее. Для кандидата: близость MACD к кроссу."""
    return abs(s.get("macd_gap_pct", 999))

def _cons_hotness(item):
    """Меньше = горячее. Для боковика: % до пробоя."""
    cur = item.get("current_price", 0)
    lvl = item.get("upper_level", 0)
    if cur <= 0 or lvl <= 0:
        return 999
    return max((lvl - cur) / cur * 100, 0)

def _rebuild_hot_pools_from_buffers():
    """
    Мягкая пересборка пулов из уже имеющихся ohlc_buffers (15m).
    Без REST. Используется раз в WS_TICKERS_REFRESH_SEC между сканами.
    Пулы обновляются только если новые данные «свежее» старых по горячести.
    """
    cand_pool = []
    cons_pool = []
    with state_lock:
        open_pairs = set(p for p, v in state.items() if v.get("position") == "open")

    for sym, buf in list(ohlc_buffers.items()):
        if not buf or len(buf) < MIN_BARS:
            continue
        try:
            df = pd.DataFrame(list(buf))
            df["ema_fast"] = ema(df["close"], 9)
            df["ema_slow"] = ema(df["close"], 21)
            df["macd_line"] = ema(df["close"], 12) - ema(df["close"], 26)
            df["macd_signal"] = ema(df["macd_line"], 9)
            last = df.iloc[-2]
            if pd.isna(last["macd_line"]) or pd.isna(last["macd_signal"]):
                continue
            macd_gap_pct = ((last["macd_line"] - last["macd_signal"])
                            / last["close"] * 100) if last["close"] else 0.0
            ti = get_trend(sym)
            trend_score = ti["score"] if ti else 0
            if (trend_score >= 2
                    and abs(macd_gap_pct) <= WS_HOT_NEAR_CROSS_PCT):
                cand_pool.append({
                    "pair": sym,
                    "macd_gap_pct": macd_gap_pct,
                    "trend_score": trend_score,
                    "close_price": float(last["close"]),
                    "current_price": float(last["close"]),
                    "source": "buffers",
                })
            with breakout_cache_lock:
                lvl = breakout_cache.get(sym)
            if lvl and lvl > 0:
                cur = float(last["close"])
                dist_pct = (lvl - cur) / cur * 100 if cur > 0 else 999
                if 0 <= dist_pct <= WS_HOT_NEAR_BREAKOUT_PCT:
                    cons_pool.append({
                        "pair": sym,
                        "upper_level": lvl,
                        "current_price": cur,
                        "dist_to_break_pct": dist_pct,
                        "days": 0, "range_pct": 0.0, "adx": 0.0,
                        "vol_trend": 1.0,
                        "source": "buffers",
                    })
        except Exception as e:
            logger.debug("_rebuild_hot_pools_from_buffers %s: %s", sym, e)

    cand_pool.sort(key=_cand_hotness)
    cons_pool.sort(key=_cons_hotness)

    with HOT_PAIRS_LOCK:
        # Если после скана пулы содержали данные, а теперь мягкая пересборка
        # не нашла — оставляем старые, чтобы не «обнулить» подписку.
        if cand_pool:
            HOT_PAIRS_CACHE["candidates_pool"] = cand_pool
        if cons_pool:
            HOT_PAIRS_CACHE["consolidations_pool"] = cons_pool
        HOT_PAIRS_CACHE["ts"] = time.time()
    logger.debug("Мягкая пересборка пулов: cand=%d, cons=%d",
                 len(cand_pool), len(cons_pool))


def update_ticker_subscription():
    """
    v20.1 X-режим: раздельные квоты.
      - до WS_TICKERS_QUOTA_CAND слотов — кандидаты (сортировка по |macd_gap|)
      - до WS_TICKERS_QUOTA_CONS слотов — боковики (сортировка по % до пробоя)
      - недобор в одной категории добирается из другой
      - итог обрезается до WS_TICKERS_MAX_PAIRS
    """
    if not WS_TICKERS_ENABLED:
        return
    with HOT_PAIRS_LOCK:
        cand_pool = list(HOT_PAIRS_CACHE.get("candidates_pool") or [])
        cons_pool = list(HOT_PAIRS_CACHE.get("consolidations_pool") or [])

    cand_pool = sorted(cand_pool, key=_cand_hotness)
    cons_pool = sorted(cons_pool, key=_cons_hotness)

    picked = []
    picked_set = set()

    # 1) квота кандидатов
    for s in cand_pool:
        if len(picked) >= WS_TICKERS_QUOTA_CAND:
            break
        p = s["pair"]
        if p in picked_set:
            continue
        picked.append(p)
        picked_set.add(p)

    # 2) квота боковиков
    for item in cons_pool:
        if len(picked) >= WS_TICKERS_QUOTA_CAND + WS_TICKERS_QUOTA_CONS:
            break
        p = item["pair"]
        if p in picked_set:
            continue
        picked.append(p)
        picked_set.add(p)

    # 3) добираем недобор кандидатов из боковиков и наоборот
    if len(picked) < WS_TICKERS_MAX_PAIRS:
        for s in cand_pool:
            if len(picked) >= WS_TICKERS_MAX_PAIRS:
                break
            p = s["pair"]
            if p in picked_set:
                continue
            picked.append(p)
            picked_set.add(p)
    if len(picked) < WS_TICKERS_MAX_PAIRS:
        for item in cons_pool:
            if len(picked) >= WS_TICKERS_MAX_PAIRS:
                break
            p = item["pair"]
            if p in picked_set:
                continue
            picked.append(p)
            picked_set.add(p)

    new_subset = set(picked[:WS_TICKERS_MAX_PAIRS])

    with WS_TICKER_PAIRS_LOCK:
        old_subset = set(WS_TICKER_PAIRS)
    to_add = new_subset - old_subset
    to_remove = old_subset - new_subset
    if not to_add and not to_remove:
        return

    app = WS_TICKERS_APP
    if app is not None and WS_TICKERS_CONNECTED.is_set():
        try:
            if to_add:
                args = [f"tickers.{s}" for s in to_add]
                for i in range(0, len(args), 100):
                    app.send(json.dumps({"op": "subscribe",
                                         "args": args[i:i + 100]}))
            if to_remove:
                args = [f"tickers.{s}" for s in to_remove]
                for i in range(0, len(args), 100):
                    app.send(json.dumps({"op": "unsubscribe",
                                         "args": args[i:i + 100]}))
            logger.info("⚡ WS-tickers (X): +%d -%d (всего %d)",
                        len(to_add), len(to_remove), len(new_subset))
        except Exception as e:
            logger.warning("WS-tickers send error: %s", e)
    with WS_TICKER_PAIRS_LOCK:
        WS_TICKER_PAIRS.clear()
        WS_TICKER_PAIRS.update(new_subset)


def _find_hot_entry(symbol):
    """Возвращает (kind, item) где kind ∈ {'cand','cons'} или (None, None)."""
    with HOT_PAIRS_LOCK:
        cand_pool = HOT_PAIRS_CACHE.get("candidates_pool") or []
        cons_pool = HOT_PAIRS_CACHE.get("consolidations_pool") or []
    cand = next((s for s in cand_pool if s["pair"] == symbol), None)
    con = next((item for item in cons_pool if item["pair"] == symbol), None)
    if cand:
        return "cand", cand
    if con:
        return "cons", con
    return None, None


def _open_on_tick(symbol, cur_price, source="tick"):
    """Проверяет возможность входа на текущем тике (для hot-пары)."""
    if not WS_TICKERS_ENABLED:
        return
    now_ts = time.time()
    last = HOT_LAST_CHECK.get(symbol, 0)
    if now_ts - last < WS_HOT_COOLDOWN_SEC:
        return
    kind, item = _find_hot_entry(symbol)
    if kind is None:
        return
    buf = ohlc_buffers.get(symbol)
    if not buf or len(buf) < MIN_BARS + 1:
        return
    df = pd.DataFrame(list(buf))
    if len(df) < MIN_BARS:
        return
    df["ema_fast"] = ema(df["close"], 9)
    df["ema_slow"] = ema(df["close"], 21)
    df["macd_line"] = ema(df["close"], 12) - ema(df["close"], 26)
    df["macd_signal"] = ema(df["macd_line"], 9)
    df["atr"] = atr(df)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df)
    df["vol_sma"] = df["volume"].rolling(20).mean()
    df["adx_slope_up"] = df["adx"] > df["adx"].shift(3)
    df["ema_cross_up"] = ((df["ema_fast"] > df["ema_slow"])
                          & (df["ema_fast"].shift(1) <= df["ema_slow"].shift(1)))
    df["vol_ratio"] = df["volume"] / df["vol_sma"].replace(0, np.nan)

    # --- Кандидат ---
    if kind == "cand":
        entry_ref = item.get("close_price", 0)
        if entry_ref > 0:
            drift_ok, _ = check_peak_guard_drift(cur_price, entry_ref)
            if not drift_ok:
                return
        sig = evaluate_ws_entry(df, symbol)
        if sig is None:
            return
        peak_ok, peak_reason, _ = check_peak_guard(df, cur_price)
        if not peak_ok:
            if now_ts - LAST_PEAK_ALERT.get(symbol, 0) > 600:
                logger.info("⛔ HOT cand %s: %s", symbol, peak_reason)
                LAST_PEAK_ALERT[symbol] = now_ts
            return
        rsi_ok, _ = check_peak_guard_rsi(float(df["rsi"].iloc[-2]))
        if not rsi_ok:
            return
        rr = (sig["target"] - sig["entry"]) / (sig["entry"] - sig["stop"])
        if rr < WS_HOT_MIN_RR:
            return
        HOT_LAST_CHECK[symbol] = now_ts
        with state_lock:
            opened = open_position(symbol, sig, int(df.iloc[-2]["start"]))
        if opened:
            logger.info("⚡ WS-REALTIME ВХОД (cand) %s @ %.6g", symbol, sig["entry"])
            btc_blocked = not market_allows_longs()
            extra = " ⚠️BTC-БЛОК ×0.5" if btc_blocked else ""
            send_telegram(
                f"⚡ <b>WS-REALTIME ВХОД</b>{extra}\n"
                f"Пара: {tv_link(symbol)}\n"
                f"Цена: {sig['entry']:.8f}\n"
                f"SL: {sig['stop']:.8f} · TP: {sig['target']:.8f}\n"
                f"<i>{' · '.join(sig['parts'])}</i>")
        return

    # --- Боковик ---
    if kind == "cons":
        lvl = item.get("upper_level", 0)
        if lvl <= 0:
            return
        if cur_price < lvl * (1 + BREAKOUT_MIN_TRIGGER_PCT / 100):
            return
        if cur_price > lvl * (1 + MAX_BREAKOUT_DISTANCE_PCT / 100):
            return
        vol_ratio = (float(df["volume"].iloc[-1] / df["vol_sma"].iloc[-1])
                     if not pd.isna(df["vol_sma"].iloc[-1]) and df["vol_sma"].iloc[-1] > 0 else 0.0)
        if vol_ratio < WS_HOT_BREAKOUT_VOL:
            return
        peak_ok, _, _ = check_peak_guard(df, cur_price)
        if not peak_ok:
            return
        atr_val = float(df["atr"].iloc[-2]) if not pd.isna(df["atr"].iloc[-2]) else 0.0
        if atr_val <= 0:
            return
        entry = cur_price
        stop = entry - atr_val * ATR_MULT_SL
        target = entry + atr_val * ATR_MULT_TP
        if stop >= entry or target <= entry:
            return
        if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
            return
        rr = (target - entry) / (entry - stop)
        if rr < WS_HOT_MIN_RR:
            return
        ti = get_trend(symbol)
        trend_score = ti["score"] if ti else 0
        pseudo_sig = {"trend_score": trend_score, "score": 7}
        btc_blocked = not market_allows_longs()
        if btc_blocked and not is_strong_signal_for_blocked_market(pseudo_sig):
            return
        HOT_LAST_CHECK[symbol] = now_ts
        with state_lock:
            if not can_enter(symbol, signal=pseudo_sig,
                             signal_risk_pct=(entry - stop) / entry * 100):
                return
            old_state = state.get(symbol, {})
            closed_start = int(df.iloc[-2]["start"]) if len(df) >= 2 else 0
            if old_state.get("last_signal_candle") == closed_start:
                return
            size = position_size_fraction(7, rr, atr_val / entry * 100,
                                          portfolio_risk_used(),
                                          (entry - stop) / entry * 100,
                                          btc_blocked=btc_blocked)
            new_state = old_state.copy()
            new_state.update({
                "position": "open", "entry_price": entry,
                "stop": stop, "target": target, "atr_ref": atr_val,
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "last_signal_candle": closed_start,
                "last_entry_ts": time.time(),
                "strategy": "breakout_realtime", "score": "RT",
                "size_fraction": size,
                "btc_blocked_entry": btc_blocked,
            })
            state[symbol] = new_state
            trade_times.append(time.time())
            save_state(state)
        logger.info("⚡ WS-REALTIME ПРОБОЙ %s @ %.6g (уровень %.6g, vol ×%.1f)",
                    symbol, entry, lvl, vol_ratio)
        extra = " ⚠️BTC-БЛОК ×0.5" if btc_blocked else ""
        send_telegram(
            f"⚡ <b>WS-REALTIME ПРОБОЙ</b>{extra}\n"
            f"Пара: {tv_link(symbol)}\n"
            f"Уровень: {lvl:.6g} → цена {entry:.6g}\n"
            f"SL: {stop:.8f} · TP: {target:.8f}\n"
            f"Объём: ×{vol_ratio:.1f}")

# ==================== WEBSOCKET: KLINE (основной) ====================
def on_open(ws):
    logger.info("WS kline подключен. Подписка на %d пар...", len(PAIRS_WS))
    args = [f"kline.{TIMEFRAME}.{p}" for p in PAIRS_WS]
    for i in range(0, len(args), 100):
        ws.send(json.dumps({"op": "subscribe", "args": args[i:i + 100]}))
        time.sleep(0.2)
    threading.Thread(target=pinger, args=(ws,), daemon=True).start()

def pinger(ws):
    while True:
        time.sleep(20)
        try:
            ws.send(json.dumps({"op": "ping"}))
        except Exception:
            return

def extend_ws_subscription(extra_pairs):
    global WS_APP
    if WS_APP is None:
        return
    new = [p for p in extra_pairs if p not in ohlc_buffers][:WS_EXTRA_SYMBOLS]
    if not new:
        return
    for i in range(0, len(new), 100):
        try:
            WS_APP.send(json.dumps({"op": "subscribe",
                                    "args": [f"kline.{TIMEFRAME}.{p}" for p in new[i:i + 100]]}))
        except Exception as e:
            logger.warning("WS subscribe error: %s", e)
            return
    for p in new:
        ohlc_buffers[p] = deque(maxlen=200)
        history = fetch_klines(p, TIMEFRAME, 80)
        if history:
            ohlc_buffers[p].extend(history)
            if len(ohlc_buffers[p]) >= 2:
                last_processed_closed[p] = int(list(ohlc_buffers[p])[-2]["start"])
        time.sleep(0.1)
    logger.info("WS-подписка расширена на %d пар из breakout-кэша", len(new))

def on_message(ws, message):
    global last_ws_msg_ts
    last_ws_msg_ts = time.time()
    try:
        data = json.loads(message)
        topic = data.get("topic", "")
        if not topic.startswith("kline."):
            return
        symbol = topic.split(".")[-1]
        if symbol not in ohlc_buffers:
            return
        for item in data.get("data", []):
            try:
                new_candle = {
                    "start": int(item["start"]) // 1000,
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(item["volume"]),
                }
            except (TypeError, ValueError, KeyError):
                continue
            buf = ohlc_buffers[symbol]
            if buf and buf[-1]["start"] == new_candle["start"]:
                buf[-1] = new_candle
            else:
                buf.append(new_candle)

            exit_messages = []
            with state_lock:
                pos = state.get(symbol, {})
                if pos.get("position") == "open" and pos.get("entry_price"):
                    atr_ref = pos.get("atr_ref") or pos["entry_price"] * 0.01
                    size = pos.get("size_fraction", 1.0)
                    frac = PARTIAL_TP_FRACTION if pos.get("partial_done") else 1.0
                    if new_candle["low"] <= pos["stop"]:
                        exit_price = min(new_candle["open"], pos["stop"])
                        pnl = log_trade(symbol, pos["entry_price"], exit_price,
                                        "Stop-Loss", pos.get("strategy", "?"),
                                        pos.get("entry_time"), size, frac)
                        exit_messages.append(
                            f"🔴 <b>СТОП-ЛОСС (WS)</b>\nПара: {tv_link(symbol)}\n"
                            f"Цена: {exit_price:.8f}\nРезультат: <b>{pnl:+.2f}%</b>"
                            + ("" if frac == 1.0 else " (оставшиеся 50%)"))
                        pos.update({"position": "closed", "last_exit_ts": time.time(),
                                    "last_exit_price": exit_price,
                                    "last_exit_reason": "stop-loss"})
                        state[symbol] = pos
                        save_state(state)
                    elif new_candle["high"] >= pos["target"]:
                        pnl = log_trade(symbol, pos["entry_price"], pos["target"],
                                        "Take-Profit", pos.get("strategy", "?"),
                                        pos.get("entry_time"), size, frac)
                        exit_messages.append(
                            f"🟢 <b>ТЕЙК-ПРОФИТ (WS)</b>\nПара: {tv_link(symbol)}\n"
                            f"Цена: {pos['target']:.8f}\nРезультат: <b>{pnl:+.2f}%</b>"
                            + ("" if frac == 1.0 else " (оставшиеся 50%)"))
                        pos.update({"position": "closed", "last_exit_ts": time.time(),
                                    "last_exit_price": pos["target"],
                                    "last_exit_reason": "take-profit"})
                        state[symbol] = pos
                        save_state(state)
                    elif (not pos.get("partial_done")
                          and new_candle["high"] >= pos["entry_price"] + PARTIAL_TP_ATR * atr_ref):
                        pos["partial_done"] = True
                        pos["stop"] = max(pos["stop"], pos["entry_price"] * 1.001)
                        part_price = pos["entry_price"] + PARTIAL_TP_ATR * atr_ref
                        pnl = log_trade(symbol, pos["entry_price"], part_price,
                                        "Частичный TP (50%)", pos.get("strategy", "?"),
                                        pos.get("entry_time"),
                                        size * PARTIAL_TP_FRACTION, PARTIAL_TP_FRACTION)
                        state[symbol] = pos
                        save_state(state)
                        exit_messages.append(
                            f"💰 <b>ЧАСТИЧНЫЙ TP (50%)</b>\nПара: {tv_link(symbol)}\n"
                            f"Зафиксировано: <b>{pnl:+.2f}%</b> на половину позиции\n"
                            f"Стоп переведён в безубыток")
            for msg in exit_messages:
                send_telegram(msg)

            if len(buf) < MIN_BARS + 1:
                continue
            closed_start = int(buf[-2]["start"])
            if last_processed_closed.get(symbol) == closed_start:
                continue
            last_processed_closed[symbol] = closed_start
            df = pd.DataFrame(list(buf))
            df["ema_fast"] = ema(df["close"], 9)
            df["ema_slow"] = ema(df["close"], 21)
            df["macd_line"] = ema(df["close"], 12) - ema(df["close"], 26)
            df["macd_signal"] = ema(df["macd_line"], 9)
            df["atr"] = atr(df)
            df["rsi"] = rsi(df["close"])
            df["adx"] = adx(df)
            df["vol_sma"] = df["volume"].rolling(20).mean()
            df["adx_slope_up"] = df["adx"] > df["adx"].shift(3)
            df["ema_cross_up"] = ((df["ema_fast"] > df["ema_slow"])
                                  & (df["ema_fast"].shift(1) <= df["ema_slow"].shift(1)))
            df["vol_ratio"] = df["volume"] / df["vol_sma"].replace(0, np.nan)

            breakout_sig = try_ws_breakout(symbol, new_candle, df)
            if breakout_sig:
                btc_blocked = not market_allows_longs()
                extra = " ⚠️BTC-БЛОК ×0.5" if btc_blocked else ""
                send_telegram(
                    f"📦 <b>ПРОБОЙ БОКОВИКА (WS)</b>{extra}\n"
                    f"Пара: {tv_link(symbol)}\n"
                    f"Цена: {breakout_sig['entry']:.8f}\n"
                    f"SL: {breakout_sig['stop']:.8f} · TP: {breakout_sig['target']:.8f}\n"
                    f"Объём: {breakout_sig['vol_ratio']:.1f}×")
                continue
            with breakout_cache_lock:
                level = breakout_cache.get(symbol)
            sig = evaluate_ws_retest(df, symbol, level)
            if sig is None:
                sig = evaluate_ws_entry(df, symbol)
            if sig is None:
                sig = evaluate_ws_pullback(df, symbol)
            if sig is None:
                continue
            cur_price_ws = float(new_candle["close"])
            peak_ok, peak_reason, _ = check_peak_guard(df, cur_price_ws)
            if not peak_ok:
                logger.info("⛔ PEAK-GUARD отклонил %s: %s", symbol, peak_reason)
                continue
            rsi_ok, rsi_reason = check_peak_guard_rsi(float(df["rsi"].iloc[-2]))
            if not rsi_ok:
                logger.info("⛔ PEAK-GUARD (RSI) отклонил %s: %s", symbol, rsi_reason)
                continue
            with state_lock:
                opened = open_position(symbol, sig, closed_start)
            if opened is None:
                continue
            score_txt = (f" · score {sig['score']}/10"
                         if isinstance(sig["score"], int) else f" · {sig['score']}")
            btc_blocked = not market_allows_longs()
            extra = " ⚠️BTC-БЛОК ×0.5" if btc_blocked else ""
            send_telegram(
                f"🟢 <b>ВХОД (WS{score_txt})</b>{extra}\n"
                f"Пара: {tv_link(symbol)} · {sig['strategy']}\n"
                f"Цена: {sig['entry']:.8f}\n"
                f"SL: {sig['stop']:.8f} · TP: {sig['target']:.8f}\n"
                f"<i>{' · '.join(sig['parts'])}</i>")
    except Exception as e:
        logger.error("Ошибка WS kline: %s", e)

def on_error(ws, error):
    logger.error("WS ошибка: %s", error)

def on_close(ws, close_status_code, close_msg):
    logger.warning("WS закрыт (%s). Переподключение...", close_status_code)
    time.sleep(5)

def run_websocket():
    global WS_APP
    while True:
        WS_APP = websocket.WebSocketApp(WS_URL, on_open=on_open,
                                        on_message=on_message,
                                        on_error=on_error, on_close=on_close)
        try:
            WS_APP.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error("WS критическая ошибка: %s", e)
        time.sleep(5)

def watchdog_loop():
    while True:
        time.sleep(30)
        silence = time.time() - last_ws_msg_ts
        if silence > WS_SILENCE_TIMEOUT and WS_APP is not None:
            logger.warning("Watchdog: тишина WS %.0f c — перезапуск соединения", silence)
            try:
                WS_APP.close()
            except Exception:
                pass

# ==================== ⚡ WEBSOCKET TICKERS ====================
def on_tickers_open(ws):
    logger.info("⚡ WS tickers подключен. Подписка на %d пар...",
                len(WS_TICKER_PAIRS))
    WS_TICKERS_CONNECTED.set()
    with WS_TICKER_PAIRS_LOCK:
        pairs = list(WS_TICKER_PAIRS)
    if pairs:
        args = [f"tickers.{s}" for s in pairs]
        for i in range(0, len(args), 100):
            try:
                ws.send(json.dumps({"op": "subscribe",
                                    "args": args[i:i + 100]}))
            except Exception as e:
                logger.warning("WS tickers subscribe error: %s", e)
            time.sleep(0.1)
    threading.Thread(target=tickers_pinger, args=(ws,), daemon=True).start()

def tickers_pinger(ws):
    while True:
        time.sleep(20)
        try:
            ws.send(json.dumps({"op": "ping"}))
        except Exception:
            return

def on_tickers_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("topic", "").startswith("tickers."):
            for item in data.get("data", []):
                symbol = item.get("symbol", "")
                if symbol not in WS_TICKER_PAIRS:
                    continue
                try:
                    cur_price = float(item.get("lastPrice", 0))
                except (TypeError, ValueError):
                    continue
                if cur_price <= 0:
                    continue
                buf = ohlc_buffers.get(symbol)
                if buf and len(buf) > 0:
                    buf[-1]["close"] = cur_price
                    if cur_price > buf[-1]["high"]:
                        buf[-1]["high"] = cur_price
                    if cur_price < buf[-1]["low"]:
                        buf[-1]["low"] = cur_price
                _open_on_tick(symbol, cur_price, source="tick")
    except Exception as e:
        logger.error("Ошибка WS tickers: %s", e)

def on_tickers_error(ws, error):
    logger.error("WS tickers ошибка: %s", error)

def on_tickers_close(ws, close_status_code, close_msg):
    logger.warning("WS tickers закрыт (%s). Переподключение...", close_status_code)
    WS_TICKERS_CONNECTED.clear()
    time.sleep(5)

def run_tickers_websocket():
    global WS_TICKERS_APP
    while True:
        WS_TICKERS_APP = websocket.WebSocketApp(
            WS_URL,
            on_open=on_tickers_open,
            on_message=on_tickers_message,
            on_error=on_tickers_error,
            on_close=on_tickers_close,
        )
        try:
            WS_TICKERS_APP.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error("WS tickers критическая ошибка: %s", e)
        WS_TICKERS_CONNECTED.clear()
        time.sleep(5)

def tickers_refresh_loop():
    """v20.1: раз в WS_TICKERS_REFRESH_SEC мягко пересобирает пулы
    из ohlc_buffers (без REST) и обновляет подписку."""
    first = True
    while True:
        if not first:
            time.sleep(WS_TICKERS_REFRESH_SEC)
        first = False
        try:
            _rebuild_hot_pools_from_buffers()
            update_ticker_subscription()
        except Exception as e:
            logger.error("tickers_refresh_loop: %s", e)

# ==================== ФОНОВОЕ СКАНИРОВАНИЕ ====================
TIMEFRAME_PARAMS = {
    "15m": {"bybit_interval": "15",  "min_bars": 80,  "ema_fast": 9,  "ema_slow": 21},
    "1h":  {"bybit_interval": "60",  "min_bars": 80,  "ema_fast": 9,  "ema_slow": 21},
    "4h":  {"bybit_interval": "240", "min_bars": 80,  "ema_fast": 9,  "ema_slow": 21},
    "1d":  {"bybit_interval": "D",   "min_bars": 150, "ema_fast": 20, "ema_slow": 50},
    "1w":  {"bybit_interval": "W",   "min_bars": 50,  "ema_fast": 10, "ema_slow": 30},
}

def try_open_breakout(pair, df_daily, current_price, now_iso, r4h=None):
    breakout = check_breakout(df_daily, current_price)
    if not breakout:
        return None
    daily_closed_start = int(df_daily.iloc[-2]["start"])
    stop_distance_pct = ((current_price - breakout["stop"]) / current_price * 100
                         if current_price > 0 else 0)
    if stop_distance_pct < MIN_STOP_DISTANCE_PCT:
        return None
    risk_pct = (current_price - breakout["stop"]) / current_price * 100
    ti = get_trend(pair)
    trend_score = ti["score"] if ti else 0
    pseudo_sig = {"trend_score": trend_score, "score": 7}
    btc_blocked = not market_allows_longs()
    if btc_blocked and not is_strong_signal_for_blocked_market(pseudo_sig):
        return None
    with state_lock:
        if not can_enter(pair, signal=pseudo_sig, signal_risk_pct=risk_pct):
            return None
        old_state = state.get(pair, {})
        if old_state.get("last_signal_candle") == daily_closed_start:
            return None
        rr = (breakout["target"] - current_price) / (current_price - breakout["stop"])
        atr_val = (current_price - breakout["stop"]) / ATR_MULT_SL
        size = position_size_fraction(7, rr, atr_val / current_price * 100,
                                      portfolio_risk_used(), risk_pct,
                                      btc_blocked=btc_blocked)
        new_state = old_state.copy()
        new_state.update({
            "position": "open", "entry_price": current_price,
            "stop": breakout["stop"], "target": breakout["target"],
            "atr_ref": atr_val,
            "entry_time": now_iso, "last_entry_ts": time.time(),
            "last_signal_candle": daily_closed_start,
            "strategy": "breakout", "score": "BO",
            "size_fraction": size,
            "btc_blocked_entry": btc_blocked,
        })
        state[pair] = new_state
        trade_times.append(time.time())
        save_state(state)
    return breakout

def background_scan_loop():
    global state, breakout_cache
    while True:
        try:
            volatile_pairs = get_filtered_pairs(TOP_N) or []
            all_pairs = get_all_available_pairs(TOTAL_PAIRS) or []
            with state_lock:
                open_pairs = [p for p, v in state.items() if v.get("position") == "open"]
            management_pairs = list(dict.fromkeys(volatile_pairs + open_pairs))
            if not management_pairs:
                logger.error("Нет пар для обработки")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            current_prices = fetch_current_prices(
                list(dict.fromkeys(volatile_pairs + all_pairs + open_pairs)))
            found_buy = found_sell = 0
            scan_summary = []
            consolidation_list = []
            consolidation_seen = set()
            now_iso = datetime.now(timezone.utc).isoformat()
            daily_cache = {}

            for pair in management_pairs:
                if not isinstance(pair, str):
                    continue
                try:
                    df_daily_data = fetch_klines(pair, TIMEFRAME_PARAMS["1d"]["bybit_interval"], 150)
                    if not df_daily_data:
                        continue
                    df_daily = pd.DataFrame(df_daily_data)
                    daily_cache[pair] = df_daily
                    results = {"1d": analyze_timeframe(df_daily, TIMEFRAME_PARAMS["1d"])}
                    for tf in ("4h", "1w"):
                        params = TIMEFRAME_PARAMS[tf]
                        df_tf = pd.DataFrame(fetch_klines(pair, params["bybit_interval"],
                                                          params["min_bars"]))
                        if not df_tf.empty:
                            results[tf] = analyze_timeframe(df_tf, params)
                    time.sleep(0.3)
                    if not results.get("4h") or not results.get("1d"):
                        continue
                    r4h, r1d = results["4h"], results["1d"]
                    trend_score = sum(1 for k in ("4h", "1d", "1w")
                                      if results.get(k) and results[k]["ema_fast"] > results[k]["ema_slow"])
                    macd_gap_pct = ((r4h["macd_line"] - r4h["macd_signal"])
                                    / r4h["close"] * 100 if r4h["close"] else 0.0)
                    current_price = current_prices.get(pair, r4h["close"])
                    scan_summary.append({
                        "pair": pair, "trend_score": trend_score,
                        "rsi_4h": r4h["rsi"], "adx_1d": r1d["adx"],
                        "adx_slope_up": r4h.get("adx_slope_up", False),
                        "vol_ratio": r4h.get("vol_ratio", 0.0),
                        "macd_gap_pct": macd_gap_pct,
                        "close_price": r4h["close"],
                        "current_price": current_price,
                    })
                    cons = detect_consolidation(df_daily)
                    if cons and pair not in consolidation_seen:
                        consolidation_seen.add(pair)
                        consolidation_list.append({"pair": pair,
                                                    "current_price": current_price,
                                                    **cons})
                    breakout = try_open_breakout(pair, df_daily, current_price,
                                                  now_iso, r4h=r4h)
                    if breakout:
                        found_buy += 1
                        btc_blocked = not market_allows_longs()
                        extra = " ⚠️BTC-БЛОК ×0.5" if btc_blocked else ""
                        send_telegram(
                            f"📦 <b>ПРОБОЙ БОКОВИКА (Breakout)</b>{extra}\n"
                            f"Пара: {tv_link(pair)}\n"
                            f"Цена: {current_price:.8f}\n"
                            f"SL: {breakout['stop']:.8f} · TP: {breakout['target']:.8f}\n"
                            f"Дней в боковике: {breakout['days']} · "
                            f"объём ×1.8 · ADX {breakout['adx']:.0f}")
                    messages = []
                    time_stopped = False
                    with state_lock:
                        pos = state.get(pair)
                        if pos and pos.get("position") == "open":
                            try:
                                entry_time = datetime.fromisoformat(
                                    pos.get("entry_time", "2026-01-01T00:00:00+00:00"))
                            except ValueError:
                                entry_time = datetime.now(timezone.utc)
                            atr1d = r1d["atr"] if r1d["atr"] > 0 else pos.get("atr_ref", 0.0)
                            profit_r = ((r4h["close"] - pos["entry_price"]) / atr1d
                                        if atr1d > 0 else 0.0)
                            days_in = (datetime.now(timezone.utc) - entry_time).days
                            size = pos.get("size_fraction", 1.0)
                            if days_in >= TIME_STOP_DAYS and profit_r < TIME_STOP_MIN_R:
                                frac = PARTIAL_TP_FRACTION if pos.get("partial_done") else 1.0
                                pnl = log_trade(pair, pos["entry_price"], r4h["close"],
                                                "Time Stop", pos.get("strategy", "?"),
                                                pos.get("entry_time"), size, frac)
                                messages.append(
                                    f"⏰ <b>ВЫХОД ПО ВРЕМЕНИ</b>\n"
                                    f"Пара: {tv_link(pair)}\n"
                                    f"Цена: {r4h['close']:.8f}\n"
                                    f"Результат: <b>{pnl:+.2f}%</b>")
                                pos.update({"position": "closed",
                                            "last_exit_ts": time.time(),
                                            "last_exit_price": r4h["close"],
                                            "last_exit_reason": "time-stop"})
                                state[pair] = pos
                                save_state(state)
                                found_sell += 1
                                time_stopped = True
                            else:
                                exit_now, reason, exit_price = check_exit(results, pos)
                                if reason == "partial":
                                    pnl = log_trade(pair, pos["entry_price"], exit_price,
                                                    "Частичный TP (50%)",
                                                    pos.get("strategy", "?"),
                                                    pos.get("entry_time"),
                                                    size * PARTIAL_TP_FRACTION,
                                                    PARTIAL_TP_FRACTION)
                                    messages.append(
                                        f"💰 <b>ЧАСТИЧНЫЙ TP (50%)</b>\n"
                                        f"Пара: {tv_link(pair)}\n"
                                        f"Зафиксировано: <b>{pnl:+.2f}%</b>\n"
                                        f"Стоп в безубытке, цель прежняя")
                                if exit_now:
                                    frac = PARTIAL_TP_FRACTION if pos.get("partial_done") else 1.0
                                    pnl = log_trade(pair, pos["entry_price"], exit_price,
                                                    reason, pos.get("strategy", "?"),
                                                    pos.get("entry_time"), size, frac)
                                    icon = "🟢" if pnl > 0 else "🔴"
                                    suffix = (" (оставшиеся 50%)" if frac != 1.0 else "")
                                    messages.append(
                                        f"{icon} <b>{reason.upper()}</b>\n"
                                        f"Пара: {tv_link(pair)}\n"
                                        f"Цена: {exit_price:.8f}\n"
                                        f"Результат: <b>{pnl:+.2f}%</b>{suffix}")
                                    pos.update({"position": "closed",
                                                "last_exit_ts": time.time(),
                                                "last_exit_price": exit_price,
                                                "last_exit_reason": reason})
                                    found_sell += 1
                                state[pair] = pos
                                save_state(state)
                    for msg in messages:
                        send_telegram(msg)
                    if time_stopped:
                        continue
                except Exception as e:
                    logger.error("Ошибка в %s: %s", pair, e)
                    continue

            for pair in all_pairs:
                if not isinstance(pair, str):
                    continue
                try:
                    df_daily = daily_cache.get(pair)
                    if df_daily is None:
                        df_daily_data = fetch_klines(pair, TIMEFRAME_PARAMS["1d"]["bybit_interval"], 150)
                        if not df_daily_data:
                            continue
                        df_daily = pd.DataFrame(df_daily_data)
                        time.sleep(0.15)
                    current_price = current_prices.get(pair, df_daily["close"].iloc[-1])
                    cons = detect_consolidation(df_daily)
                    if cons and pair not in consolidation_seen:
                        consolidation_seen.add(pair)
                        consolidation_list.append({"pair": pair,
                                                    "current_price": current_price,
                                                    **cons})
                    breakout = try_open_breakout(pair, df_daily, current_price, now_iso)
                    if breakout:
                        found_buy += 1
                        btc_blocked = not market_allows_longs()
                        extra = " ⚠️BTC-БЛОК ×0.5" if btc_blocked else ""
                        send_telegram(
                            f"📦 <b>ПРОБОЙ БОКОВИКА (Breakout)</b>{extra}\n"
                            f"Пара: {tv_link(pair)}\n"
                            f"Цена: {current_price:.8f}\n"
                            f"SL: {breakout['stop']:.8f} · TP: {breakout['target']:.8f}\n"
                            f"Дней в боковике: {breakout['days']}")
                except Exception as e:
                    logger.error("Ошибка во втором проходе %s: %s", pair, e)
                    continue

            with breakout_cache_lock:
                consolidation_list.sort(key=lambda x: (-x["days"], x.get("vol_trend", 1.0)))
                breakout_cache = {}
                for item in consolidation_list[:BREAKOUT_CACHE_SIZE]:
                    breakout_cache[item["pair"]] = item["upper_level"]
            logger.info("Breakout-кэш обновлён: %d уровней", len(breakout_cache))

            with breakout_cache_lock:
                extra = [p for p in breakout_cache.keys() if p not in PAIRS_WS]
            extend_ws_subscription(extra)

            try:
                with state_lock:
                    if len(state) > 500:
                        now_ts = time.time()
                        for p in list(state.keys()):
                            pos = state[p]
                            if pos.get("position") == "closed":
                                last_exit_ts = float(pos.get("last_exit_ts", 0) or 0)
                                if last_exit_ts > 0 and now_ts - last_exit_ts > CLEANUP_AFTER_DAYS * 86400:
                                    del state[p]
                        save_state(state)
            except Exception as e:
                logger.error("Ошибка очистки state: %s", e)

            send_status(scan_summary, consolidation_list, found_buy, found_sell)
            try:
                update_ticker_subscription()
            except Exception as e:
                logger.error("update_ticker_subscription (status): %s", e)
            time.sleep(SCAN_INTERVAL_SECONDS)
        except Exception as e:
            logger.critical("Критическая ошибка в фоне: %s", e)
            time.sleep(SCAN_INTERVAL_SECONDS)

# ==================== СТАТУС: ОДНО СООБЩЕНИЕ (v20.1) ====================
def send_status(scan_summary, consolidation_list, found_buy, found_sell):
    with state_lock:
        open_snapshot = [(pair, dict(pos)) for pair, pos in state.items()
                         if pos.get("position") == "open"]
    with trend_cache_lock:
        q3 = sum(1 for v in trend_cache.values() if v["score"] == 3)
    with market_context_lock:
        mok, mreason = market_context["ok"], market_context["reason"]
    with subscribers_lock:
        subs_count = len(SUBSCRIBERS)
    with WS_TICKER_PAIRS_LOCK:
        tickers_count = len(WS_TICKER_PAIRS)

    breakout_strategies = ("breakout", "breakout_ws", "breakout_retest", "breakout_realtime")
    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")

    btc_state_str = "OK" if mok else f"БЛОК: {mreason}"
    if not mok and BTC_ALLOW_STRONG_WHEN_BLOCKED:
        btc_state_str += " · сильные T3/3 score≥8 → ×0.5"

    header = [
        f"📡 <b>СТАТУС v20.1 «WS-REALTIME-HOT-X»</b> | <i>{now_str} UTC</i>",
        "━━━━━━━━━━━━━━━━━━━━━",
        f"🔹 Пар WS kline: <b>{len(PAIRS_WS)}</b> · Тренд 3/3: <b>{q3}</b>",
        f"🔹 ⚡ WS tickers: <b>{tickers_count}</b>/{WS_TICKERS_MAX_PAIRS} "
        f"(cand ≤{WS_TICKERS_QUOTA_CAND} · cons ≤{WS_TICKERS_QUOTA_CONS})",
        f"🔹 BTC: <b>{btc_state_str}</b>",
        f"🔹 Боковиков: <b>{len(consolidation_list)}</b> · "
        f"Позиций: <b>{len(open_snapshot)}/{MAX_OPEN_POSITIONS}</b>",
        f"🔹 Входов: <b>{found_buy}</b> · Выходов: <b>{found_sell}</b> · 👥 {subs_count}",
        f"🔹 🛡 PEAK-GUARD: <b>{'ON' if PEAK_GUARD_ENABLED else 'OFF'}</b>",
    ]
    if not cb_can_trade():
        header.append("🔹 🛑 Circuit breaker: <b>входы на паузе</b>")
    header.append("━━━━━━━━━━━━━━━━━━━━━")

    pos_lines = []
    if open_snapshot:
        for pair, pos in open_snapshot:
            et = str(pos.get("entry_time", "?")).replace("T", " ")[:16]
            tag = "📦" if pos.get("strategy") in breakout_strategies else "🟢"
            line = (f"{tag} <b>{pair}</b> {pos.get('entry_price', 0):.8f} → "
                    f"🛑{pos.get('stop', 0):.8f} 🎯{pos.get('target', 0):.8f} · {et}")
            if pos.get("partial_done"):
                line += " · 💰50%"
            if pos.get("btc_blocked_entry"):
                line += " · ⚠️BTC×0.5"
            pos_lines.append(line)
    else:
        pos_lines.append("💰 Позиций нет")

    # ---- пул кандидатов (расширенный) ----
    valid_calls = [
        s for s in scan_summary
        if s["trend_score"] >= 2
        and STATUS_RSI_MIN <= s.get("rsi_4h", 0) <= STATUS_RSI_MAX
        and STATUS_ADX_MIN <= s.get("adx_1d", 0) <= STATUS_ADX_MAX
        and s.get("close_price", 0) >= STATUS_MIN_PRICE
    ]
    valid_calls.sort(key=lambda s: (-s["trend_score"],
                                     s.get("macd_gap_pct", 999)))

    def _hot_score(item):
        cur = item.get("current_price", 0)
        lvl = item.get("upper_level", 0)
        if cur <= 0 or lvl <= 0:
            return 999
        dist_pct = (lvl - cur) / cur * 100
        return max(dist_pct, 0)
    consolidation_list = sorted(consolidation_list, key=_hot_score)

    # ---- топ-10 для отображения ----
    display_calls = valid_calls[:MAX_SHOW_CANDIDATES]
    display_cons = consolidation_list[:MAX_SHOW_CONSOLIDATIONS]

    # ---- расширенные пулы для WS tickers ----
    cand_pool = [s for s in valid_calls
                 if abs(s.get("macd_gap_pct", 999)) <= WS_HOT_NEAR_CROSS_PCT]
    cons_pool = []
    for item in consolidation_list:
        cur = item.get("current_price", 0)
        lvl = item.get("upper_level", 0)
        if cur <= 0 or lvl <= 0:
            continue
        dist_pct = (lvl - cur) / cur * 100
        if 0 <= dist_pct <= WS_HOT_NEAR_BREAKOUT_PCT:
            cons_pool.append(item)
    cand_pool.sort(key=_cand_hotness)
    cons_pool.sort(key=_cons_hotness)

    with HOT_PAIRS_LOCK:
        HOT_PAIRS_CACHE["candidates"] = list(display_calls)
        HOT_PAIRS_CACHE["consolidations"] = list(display_cons)
        HOT_PAIRS_CACHE["candidates_pool"] = list(cand_pool)
        HOT_PAIRS_CACHE["consolidations_pool"] = list(cons_pool)
        HOT_PAIRS_CACHE["ts"] = time.time()

    def cand_line(i, s, with_link):
        gap = s.get("macd_gap_pct", 0)
        if gap < 0:
            prox, hl = "⏳", gap > -0.25
        elif gap < 0.5:
            prox, hl = "🟡", True
        else:
            prox, hl = "⚠️", False
        name = tv_link(s["pair"]) if with_link else s["pair"]
        cur = s.get("current_price") or s.get("close_price", 0)
        entry = s.get("close_price", 0)
        buf = ohlc_buffers.get(s["pair"])
        peak_ok, peak_reason, _ = (True, "", {})
        if buf and len(buf) >= PEAK_LOOKBACK_CANDLES:
            df_pk = pd.DataFrame(list(buf))
            peak_ok, peak_reason, _ = check_peak_guard(df_pk, cur)
        drift_pct = abs(cur - entry) / entry * 100 if entry > 0 else 0
        drift_ok = drift_pct <= PEAK_MAX_DRIFT_PCT
        is_ready = (s["trend_score"] == 3 and gap < 0.5
                    and peak_ok and drift_ok)
        if is_ready:
            mark = "🟢"
        elif not peak_ok:
            mark = "⛔"
        elif not drift_ok:
            mark = "⚠️"
        elif hl:
            mark = "🟡"
        else:
            mark = ""
        vol = s.get("vol_ratio", 0.0)
        vol_txt = f" V{vol:.1f}x" if vol >= MIN_VOL_MULT else ""
        up = "↑" if s.get("adx_slope_up") else ""
        ready_tag = "✨" if is_ready else ""
        drift_txt = f" ⚠️+{drift_pct:.1f}%" if not drift_ok else ""
        return (f"{mark}{i}.{name}{ready_tag} T{s['trend_score']} "
                f"ADX{s['adx_1d']:.0f}{up} RSI{s['rsi_4h']:.0f}{vol_txt} "
                f"💰{cur:.6g} 🎯~{entry:.6g}{drift_txt} {prox}")

    def cons_line(i, item, with_link):
        dry = "🥀" if item.get("vol_trend", 1.0) <= 0.9 else ""
        name = tv_link(item["pair"]) if with_link else item["pair"]
        cur = item.get("current_price", 0)
        lvl = item.get("upper_level", 0)
        if cur > 0 and lvl > 0:
            dist_pct = (lvl - cur) / cur * 100
        else:
            dist_pct = 999
        if dist_pct <= HOT_DIST_PCT_1:
            mark = "🔥"
        elif dist_pct <= HOT_DIST_PCT_2:
            mark = "⚡"
        elif dist_pct <= HOT_DIST_PCT_3:
            mark = "🟢"
        else:
            mark = ""
        dist_txt = f"({dist_pct:.1f}%)" if dist_pct < 999 else ""
        return (f"{mark}{i}.{name} {item['days']}д {item['range_pct']:.0f}% "
                f"ADX{item['adx']:.0f}{dry} 🚀{lvl:.6g} 💰{cur:.6g}{dist_txt}")

    footer = [
        "━━━━━━━━━━━━━━━━━━━━━",
        f"🔄 Следующий статус через 2 ч · лимиты {MAX_OPEN_POSITIONS} поз / "
        f"{MAX_TRADES_PER_HOUR} в час",
        f"ℹ️ Показаны {MAX_SHOW_CANDIDATES} кандидатов + "
        f"{MAX_SHOW_CONSOLIDATIONS} боковиков.",
        f"⚡ WS-tickers (X): кандидаты ≤{WS_TICKERS_QUOTA_CAND} + "
        f"боковики ≤{WS_TICKERS_QUOTA_CONS} в реальном времени",
        f"🎯 Пороги: |MACD gap|≤{WS_HOT_NEAR_CROSS_PCT}% · "
        f"до пробоя ≤{WS_HOT_NEAR_BREAKOUT_PCT}%",
        "🛡 PEAK-GUARD: не входим на пике (RSI≤70, дрейф≤1%, топ-15%)",
        "🔥≤1% ⚡≤3% 🟢≤5% — % до пробоя · 🥀 объём↓ · ⛔ пик · ⚠️ дрейф",
        "👇 Тапни 📈-ссылку — TradingView",
    ]

    def build(with_links, max_cand, max_cons):
        lines = list(header)
        lines += pos_lines
        lines.append("")
        lines.append("🔵🔵🔵 <b>ТОП КАНДИДАТОВ (CONFLUENCE)</b> 🔵🔵🔵")
        lines.append("<blockquote expandable>")
        if display_calls:
            for i, s in enumerate(display_calls[:max_cand], 1):
                lines.append(cand_line(i, s, with_links))
        else:
            lines.append("😴 готовых кандидатов нет")
        lines.append("</blockquote>")
        lines.append("🟡🟡🟡 <b>МОНЕТЫ В БОКОВИКЕ (30–60 ДНЕЙ)</b> 🟡🟡🟡")
        lines.append("<blockquote expandable>")
        if display_cons:
            for i, item in enumerate(display_cons[:max_cons], 1):
                lines.append(cons_line(i, item, with_links))
        else:
            lines.append("📦 боковиков нет")
        lines.append("</blockquote>")
        lines += footer
        return "\n".join(lines)

    text = build(True, len(display_calls), len(display_cons))
    if len(text) > TG_SAFE_LIMIT:
        for max_cand in range(len(display_calls), 2, -1):
            for max_cons in range(len(display_cons), 2, -1):
                text = build(True, max_cand, max_cons)
                if len(text) <= TG_SAFE_LIMIT:
                    break
            if len(text) <= TG_SAFE_LIMIT:
                break
    if len(text) > TG_SAFE_LIMIT:
        text = build(True, 5, 5)

    _send_to_all_one(text)
    logger.info("Статус v20.1: %d символов · cand_pool=%d cons_pool=%d · tickers=%d",
                len(text), len(cand_pool), len(cons_pool), tickers_count)

# ==================== MAIN ====================
def handle_stop(signum, _frame):
    logger.info("Получен сигнал %s — сохраняю state и завершаюсь", signum)
    with state_lock:
        save_state(state)
    raise SystemExit(0)

if __name__ == "__main__":
    logger.info("Запуск бота v20.1 «WS-REALTIME-HOT-X» (Bybit) ...")
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    _load_subscribers()

    state = load_state()
    for sym, pos in state.items():
        if pos.get("position") == "open":
            if "atr_ref" not in pos or pos["atr_ref"] <= 0:
                if "atr_entry" in pos and pos["atr_entry"] > 0:
                    pos["atr_ref"] = pos["atr_entry"]
                elif pos.get("entry_price") and pos.get("stop"):
                    pos["atr_ref"] = (pos["entry_price"] - pos["stop"]) / ATR_MULT_SL
                else:
                    pos["atr_ref"] = pos.get("entry_price", 0) * 0.01
            pos.setdefault("partial_done", False)
            pos.setdefault("breakeven_moved", False)
            pos.setdefault("highest_close", pos.get("entry_price", 0))
            pos.setdefault("size_fraction", 1.0)
            pos.setdefault("btc_blocked_entry", False)
    save_state(state)

    refresh_market_context()

    retry_count = 0
    while not PAIRS_WS and retry_count < 3:
        PAIRS_WS = get_filtered_pairs(TOP_N)
        if not PAIRS_WS:
            logger.warning("Попытка %d: нет волатильных пар, повтор...", retry_count + 1)
            time.sleep(10)
        retry_count += 1
    if not PAIRS_WS:
        logger.warning("Fallback: использую все доступные пары")
        PAIRS_WS = get_all_available_pairs(TOP_N)
    if not PAIRS_WS:
        logger.critical("Не удалось получить пары для WebSocket!")
        raise SystemExit(1)
    logger.info("Загружено %d пар для WebSocket", len(PAIRS_WS))

    warm = PAIRS_WS[:TREND_CACHE_INITIAL_LIMIT]
    for idx, pair in enumerate(warm, 1):
        try:
            refresh_trend_for(pair)
        except Exception as e:
            logger.debug("warm %s: %s", pair, e)
        if idx % 15 == 0:
            logger.info("Прогрев тренд-кэша: %d/%d", idx, len(warm))
    with trend_cache_lock:
        q3 = sum(1 for v in trend_cache.values() if v["score"] == 3)
    logger.info("Прогрев тренд-кэша завершён: 3/3 = %d пар", q3)

    for pair in PAIRS_WS:
        if not isinstance(pair, str):
            continue
        history = fetch_klines(pair, TIMEFRAME, 80)
        if history:
            ohlc_buffers[pair] = deque(history, maxlen=200)
            if len(ohlc_buffers[pair]) >= 2:
                last_processed_closed[pair] = int(list(ohlc_buffers[pair])[-2]["start"])
        time.sleep(0.1)

    # Стартуем потоки
    threading.Thread(target=run_websocket, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()
    threading.Thread(target=trend_cache_loop, daemon=True).start()
    threading.Thread(target=market_context_loop, daemon=True).start()
    threading.Thread(target=polling_loop, daemon=True).start()
    threading.Thread(target=run_tickers_websocket, daemon=True).start()
    threading.Thread(target=tickers_refresh_loop, daemon=True).start()

    _send_to_all_one(
        f"🟢 <b>СКАНЕР v20.1 «WS-REALTIME-HOT-X» ЗАПУЩЕН</b>\n"
        f"WS kline: {len(PAIRS_WS)} пар · динам. подписка\n"
        f"⚡ WS tickers (X): квоты {WS_TICKERS_QUOTA_CAND} cand + "
        f"{WS_TICKERS_QUOTA_CONS} cons (макс {WS_TICKERS_MAX_PAIRS})\n"
        f"🎯 Пороги близости: |MACD gap|≤{WS_HOT_NEAR_CROSS_PCT}% · "
        f"до пробоя ≤{WS_HOT_NEAR_BREAKOUT_PCT}%\n"
        f"Тренд 3/3: {q3} · BTC: {'OK' if market_allows_longs() else 'БЛОК'}\n"
        f"🛡 PEAK-GUARD: не входим на пике (RSI≤{PEAK_MAX_RSI}, "
        f"дрейф≤{PEAK_MAX_DRIFT_PCT}%)\n"
        f"ℹ️ Показ: {MAX_SHOW_CANDIDATES} кандидатов + "
        f"{MAX_SHOW_CONSOLIDATIONS} боковиков\n"
        f"👥 Подписчиков: {len(SUBSCRIBERS)} · /start · /stop · /help")
    background_scan_loop()
