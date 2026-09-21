#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
BYBIT SCANNER v21.6.0 «VOLUME-AWARE-HOLD» — ЕДИНЫЙ ФАЙЛ VPS
WebSocket (wss://stream.bybit.com/v5/public/spot) + REST (api.bybit.com/v5)
Бумажная торговля: сделки -> bybit_trades.json, алерты -> Telegram

НОВОЕ В v21.6.0 (относительно v21.5.0):
• 🚀 3-режимная логика удержания позиции:
    ДНО       — обычная логика (BE, EMA-кросс, трейлинг 2R)
    У ПРОБОЯ  — цена ≥ upper×0.97: при vol_score ≥ 0.6 EMA-кросс
                игнорируется, при vol_score < 0.4 + красная 4h — выход
    ПОСЛЕ     — cur > upper: трейлинг 2×R под high,
                возврат ниже upper−0.5% → «Ложный пробой»
• 📊 VOLUME-AWARE оценка по 4h: vol_ratio, ADX slope, trend_up, MACD.
• 🌊 Оценка накопления в боковике (vol_trend) — бонус/штраф к скору bounce.
• 🛡 Флаг broke_out — фиксирует факт пробоя для проверки ложного пробоя.
• ⚡ Отображение режима в статусе: «⚡ у пробоя (держим, vol=0.72)»
• 🐛 FIX: breakout_cache сохраняет vol_trend.
• 🐛 FIX: сброс near_upper_bars при уходе от upper.
• Миграция старых позиций: добавляет box_upper, broke_out и др.
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

# --- аккаунт для risk/CB ---
ACCOUNT_SIZE_USD = 10_000.0

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

CONSOLIDATION_WINDOWS = (30, 45, 60)
CONSOLIDATION_MAX_RANGE_PCT = 24.0
CONSOLIDATION_MAX_ADX = 25

WS_EXTRA_SYMBOLS = 50
WS_SUBSCRIBE_CHUNK = 10
WS_SUBSCRIBE_PAUSE = 0.15

# --- BTC-ФИЛЬТР ---
BTC_CONTEXT_ENABLED = True
BTC_CONTEXT_TTL = 300
BTC_DROP_6H_PCT = -10.0
BTC_ADX_BLOCK_THRESHOLD = 55
BTC_ALLOW_STRONG_WHEN_BLOCKED = True
BTC_STRONG_MIN_TREND_SCORE = 3
BTC_STRONG_MIN_SCORE = 8
BTC_STRONG_SIZE_MULT = 0.5

# --- CIRCUIT BREAKER ---
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

# --- RISK ---
RISK_PER_TRADE_PCT = 1.0
MAX_PORTFOLIO_RISK_PCT = 6.0

# --- КОМИССИИ ---
FEE_PCT = 0.25
SLIPPAGE_PCT = 0.05

# --- ATR-МУЛЬТИПЛИКАТОРЫ ---
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
PULLBACK_ENABLED = False

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

# --- ДНЕВНОЙ РЕЖИМ ---
DAILY_REGIME_FILTER_ENABLED = True
DAILY_REGIME_BLOCK_BELOW_SCORE = 7
DAILY_REGIME_FETCH_BARS = 260

# --- 💎 RSI DIP-BUY ---
DIPBUY_ENABLED = True
DIPBUY_RSI_ZONE = 35
DIPBUY_RSI_MIN_BARS = 3
DIPBUY_MIN_VOL = 1.3
DIPBUY_SL_ATR = 2.5
DIPBUY_TP_ATR = 5.0
DIPBUY_REQUIRE_DAILY_BULL = True
DIPBUY_REQUIRE_EMA_UP = True
DIPBUY_EMA_SLACK = 0.998
DIPBUY_REQUIRE_BB = False
DIPBUY_BB_PERIOD = 20
DIPBUY_BB_STD = 2.0
DIPBUY_LOG_REJECTS = True

# --- DIP-FIRST ---
BOTTOM_FILTER_ENABLED = True
BOTTOM_FILTER_PCT = 0.50
BOTTOM_FILTER_LOOKBACK_BARS = 96

# --- 🌊 WAVETREND ---
WT_ENABLED = True
WT_N1 = 10
WT_N2 = 21
WT_SMA_LEN = 4
WT_OS1 = -60
WT_OS2 = -53
WT_OB1 = 60
WT_OB2 = 53
WT_DIP_STRATEGY_ENABLED = True
WT_DIP_REQUIRE_EMA_UP = False
WT_CONFIRM_BYPASS_BTC = True
WT_DIP_SL_ATR = 2.5
WT_DIP_TP_ATR = 5.0
WT_HOT_RSI_MIN = 25
WT_HOT_RSI_MAX = 55

# --- 💥 SQUEEZE MOMENTUM ---
SQZ_ENABLED = True
SQZ_BB_LEN = 20
SQZ_BB_MULT = 2.0
SQZ_KC_LEN = 20
SQZ_KC_MULT = 1.5
SQZ_BREAKOUT_VOL_MULT = 1.4
SQZ_RELEASE_MAX_BARS = 3
SQZ_BREAKOUT_STRATEGY_ENABLED = False
SQZ_DIP_STRATEGY_ENABLED = False
SQZ_DIP_SL_ATR = 2.5
SQZ_DIP_TP_ATR = 5.0
SQZ_DIP_VOL_MIN = 1.2
SQZ_DIP_RSI_RISING = True
SQZ_DIP_CLOSE_ABOVE_EMA9 = True

# --- 🎯 RANGE-BOUNCE ---
RANGE_BOUNCE_ENABLED = True
RANGE_BOUNCE_ZONE_PCT = 4.0
RANGE_BOUNCE_VOL_MULT = 1.1
RANGE_BOUNCE_RSI_MAX = 55
RANGE_BOUNCE_SL_PCT = 0.5
RANGE_BOUNCE_MIN_RR = 1.5
RANGE_BOUNCE_HOT_ZONE_PCT = 7.0

# --- 🔒 HOLD-TO-REVERSAL ---
HOLD_MODE_ENABLED = True
BREAKEVEN_TRIGGER_ATR_FAST = 0.5
NO_PARTIAL_TP = True
NO_FIXED_TP = True

# --- 🚀 VOLUME-AWARE HOLD (v21.6.0) ---
# 3 режима позиции: дно → у пробоя → после пробоя
VOL_HOLD_ENABLED = True
VOL_HOLD_NEAR_UPPER_PCT = 3.0       # цена ≥ upper×0.97 = «у пробоя»
VOL_HOLD_SCORE_KEEP = 0.60          # vol_score ≥ 0.6 → держим, EMA-кросс игнор
VOL_HOLD_SCORE_WEAK = 0.40          # vol_score < 0.4 + красная 4h → выход
# Внимание: check_exit вызывается из scan-loop раз в 2 часа,
# а НЕ на каждой 4h-свече. Поэтому 12 «баров» ≈ 24 часа реального времени.
VOL_HOLD_MAX_BARS_NEAR = 12
VOL_HOLD_AFTER_BREAK_TRAIL = 2.0    # после пробоя — трейлинг 2×R
VOL_HOLD_FALSE_BREAK_PCT = 0.5      # ушли ниже upper на 0.5% → ложный пробой

# --- ПОРОГИ БОКОВИКОВ ---
HOT_DIST_PCT_1 = 1.0
HOT_DIST_PCT_2 = 3.0
HOT_DIST_PCT_3 = 5.0

MAX_SHOW_CANDIDATES = 10
MAX_SHOW_CONSOLIDATIONS = 10
MAX_SHOW_DIPBUY = 5
MAX_SHOW_WTDIP = 5
MAX_SHOW_BOUNCE = 10

# --- ⚡ WS-REALTIME-HOT ---
WS_TICKERS_ENABLED = True
WS_TICKERS_MAX_PAIRS = 50
WS_TICKERS_QUOTA_CAND = 15
WS_TICKERS_QUOTA_CONS = 15
WS_TICKERS_QUOTA_DIP = 8
WS_TICKERS_QUOTA_WT = 4
WS_TICKERS_QUOTA_BOUNCE = 8
WS_TICKERS_REFRESH_SEC = 300
WS_HOT_COOLDOWN_SEC = 5
WS_HOT_MIN_RR = 1.5
WS_HOT_MIN_VOL = 1.2
WS_HOT_BREAKOUT_VOL = 1.8
WS_HOT_MAX_DRIFT_PCT = 1.0

WS_HOT_NEAR_CROSS_PCT = 0.5
WS_HOT_NEAR_BREAKOUT_PCT = 5.0
WS_HOT_DIP_RSI_MIN = 25
WS_HOT_DIP_RSI_MAX = 45

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
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8"),
    ],
)
logger = logging.getLogger("bybit-scanner")
# ==================== 🔍 DEBUG-FUNNEL ====================
FUNNEL = {
    # 🎯 Range-Bounce
    "bounce_total": 0, "bounce_zone": 0, "bounce_rsi": 0,
    "bounce_green": 0, "bounce_vol": 0, "bounce_rr": 0,
    # 💎 RSI dip
    "dip_total": 0, "dip_bull": 0, "dip_zone": 0, "dip_rising": 0,
    "dip_green": 0, "dip_bb": 0, "dip_vol": 0, "dip_ema": 0,
    # 🌊 WT-dip
    "wt_total": 0, "wt_bull": 0, "wt_rsi": 0, "wt_cross": 0,
    "wt_bottom": 0, "wt_ema": 0, "wt_green": 0,
    # 💥 SQZ-dip
    "sqzdip_total": 0, "sqzdip_bull": 0, "sqzdip_release": 0,
    "sqzdip_rsi": 0, "sqzdip_green": 0, "sqzdip_vol": 0, "sqzdip_bottom": 0,
    # Общие
    "peak_block": 0, "btc_block": 0, "opened": 0,
    "bars_processed": 0,
    # v21.6.0: удержания
    "hold_kept": 0,      # «держим» у пробоя
    "hold_weak": 0,      # «слабый объём у пробоя»
    "hold_false": 0,     # «ложный пробой»
}
FUNNEL_LOCK = threading.Lock()
_FUNNEL_SILENT = threading.local()   # v21.6.0: тихий режим для also_valid

def funnel_inc(key, n=1):
    if getattr(_FUNNEL_SILENT, "on", False):
        return
    with FUNNEL_LOCK:
        if key in FUNNEL:
            FUNNEL[key] += n

def funnel_snapshot():
    with FUNNEL_LOCK:
        return dict(FUNNEL)

def funnel_reset():
    with FUNNEL_LOCK:
        for k in FUNNEL:
            FUNNEL[k] = 0

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
last_tickers_msg_ts = time.time()
WS_APP = None
PAIRS_WS = []
SESSION = requests.Session()

market_context = {"ok": True, "reason": "", "ts": 0.0}
market_context_lock = threading.RLock()

cb_state = {
    "consec": 0, "paused_until": 0.0, "day": "",
    "day_account_pnl_pct": 0.0,
    "day_trades_pnl_sum": 0.0,
    "day_trades": 0,
}
cb_lock = threading.Lock()

SUBSCRIBERS = set()
subscribers_lock = threading.RLock()
MAIN_CHAT_ID = None

HOT_PAIRS_CACHE = {
    "candidates_pool": [], "consolidations_pool": [],
    "dipbuy_pool": [], "wtdip_pool": [], "bounce_pool": [],
    "candidates": [], "consolidations": [],
    "dipbuy": [], "wtdip": [], "bounce": [],
    "ts": 0.0,
}
HOT_PAIRS_LOCK = threading.RLock()
HOT_LAST_CHECK = {}

WS_TICKER_PAIRS = set()
WS_TICKER_PAIRS_LOCK = threading.RLock()
WS_TICKERS_APP = None
WS_TICKERS_CONNECTED = threading.Event()

LAST_PEAK_ALERT = {}

SESSION_STATS = {"entries": 0, "exits": 0, "session_day": ""}
SESSION_STATS_LOCK = threading.Lock()

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
                logger.warning("HTTP %s от Bybit, повтор через %d c",
                               resp.status_code, wait)
                time.sleep(wait)
                continue
            _http_failures = 0
            return resp.json()
        except Exception as e:
            logger.warning("Сетевая ошибка %s (попытка %d): %s",
                           url, attempt + 1, e)
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
def cb_register(net_pnl_pct, size_fraction=1.0):
    account_pnl_pct = net_pnl_pct * size_fraction
    with cb_lock:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if cb_state["day"] != today:
            cb_state["day"] = today
            cb_state["day_account_pnl_pct"] = 0.0
            cb_state["day_trades_pnl_sum"] = 0.0
            cb_state["day_trades"] = 0
        cb_state["day_account_pnl_pct"] += account_pnl_pct
        cb_state["day_trades_pnl_sum"] += net_pnl_pct
        cb_state["day_trades"] += 1

        if net_pnl_pct < 0:
            cb_state["consec"] += 1
            if cb_state["consec"] >= CB_MAX_CONSEC_LOSSES:
                cb_state["paused_until"] = time.time() + CB_PAUSE_SECONDS
                logger.warning(
                    "🚫 Circuit Breaker: %d убытков подряд — пауза %d ч",
                    cb_state["consec"], CB_PAUSE_SECONDS // 3600)
        else:
            cb_state["consec"] = 0

        if cb_state["day_account_pnl_pct"] <= -CB_DAILY_LOSS_PCT:
            midnight = (datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0)
                + pd.Timedelta(days=1))
            cb_state["paused_until"] = max(
                cb_state["paused_until"], midnight.timestamp())
            logger.error(
                "🚫 CB: дневной лимит счёта %.2f%% (сделок %d, сумма сделок %.2f%%)",
                cb_state["day_account_pnl_pct"], cb_state["day_trades"],
                cb_state["day_trades_pnl_sum"])

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
                chg_6h = ((closes[-2] / closes[-8] - 1.0) * 100
                          if len(closes) > 8 else 0.0)
                df = pd.DataFrame(candles)
                ef = ema(df["close"], 9).iloc[-2]
                es = ema(df["close"], 21).iloc[-2]
                adx_val = adx(df).iloc[-2]
                if chg_6h <= BTC_DROP_6H_PCT:
                    ok, reason = False, "BTC %.1f%% за 6ч" % chg_6h
                elif (ef < es and not pd.isna(adx_val)
                      and adx_val > BTC_ADX_BLOCK_THRESHOLD):
                    ok, reason = False, (
                        "BTC нисходящий тренд (ADX>%.0f)"
                        % BTC_ADX_BLOCK_THRESHOLD)
        except Exception as e:
            logger.debug("market context: %s", e)
    with market_context_lock:
        market_context.update(ok=ok, reason=reason, ts=time.time())
    if not ok:
        logger.info("🔧 BTC-контекст: лонги заблокированы (%s)", reason)

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

def _signal_bypasses_btc(signal):
    if not signal:
        return False
    if signal.get("strategy") in ("rsi_dipbuy", "wt_dip", "sqz_dip", "range_bounce"):
        return True
    if signal.get("wt_confirm") or signal.get("sqz_release"):
        return True
    return bool(signal.get("bottom_ok"))

def btc_extra_tag(signal):
    if market_allows_longs():
        return ""
    if _signal_bypasses_btc(signal):
        return " 🎯BOTTOM (BTC-обход)"
    return " ⚠️BTC-БЛОК ×0.5"

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
        position_pct = ((entry_price - local_low) / (local_high - local_low) * 100)
        dist_to_max_pct = (local_high - entry_price) / entry_price * 100
        metrics = {
            "position_pct": round(position_pct, 1),
            "dist_to_max_pct": round(dist_to_max_pct, 2),
            "local_high": local_high, "local_low": local_low,
        }
        if position_pct > PEAK_MAX_POSITION_PCT:
            return False, (
                f"цена в верхних {100 - PEAK_MAX_POSITION_PCT:.0f}% "
                f"({position_pct:.0f}%)"), metrics
        if dist_to_max_pct < PEAK_MIN_DIST_TO_MAX_PCT:
            return False, (
                f"близко к максимуму ({dist_to_max_pct:.2f}%)"), metrics
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

def peak_guard_ok(df, symbol, entry, rsi_val=None):
    if not PEAK_GUARD_ENABLED:
        return True, ""
    ok1, r1, _ = check_peak_guard(df, entry)
    if not ok1:
        funnel_inc("peak_block")
        return False, r1
    ok2, r2 = check_peak_guard_rsi(rsi_val)
    if not ok2:
        funnel_inc("peak_block")
        return False, r2
    return True, ""

# ==================== DIP-FIRST ====================
def _bottom_filter_ok(symbol, cur_price):
    if not BOTTOM_FILTER_ENABLED:
        return True
    if cur_price is None or cur_price <= 0:
        return True
    buf = ohlc_buffers.get(symbol)
    if not buf or len(buf) < 48:
        return True
    recent = list(buf)[-BOTTOM_FILTER_LOOKBACK_BARS:]
    try:
        day_low = min(float(c["low"]) for c in recent)
        day_high = max(float(c["high"]) for c in recent)
    except (KeyError, TypeError, ValueError):
        return True
    if day_high <= day_low:
        return True
    pos = (cur_price - day_low) / (day_high - day_low)
    ok = pos <= BOTTOM_FILTER_PCT
    if not ok:
        logger.debug("🎯 DIP-FIRST: %s в верхних %.0f%% (pos=%.2f)",
                     symbol, (1 - BOTTOM_FILTER_PCT) * 100, pos)
    return ok

# ==================== СЕКТОРА ====================
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

# ==================== РАЗМЕР ПОЗИЦИИ ====================
def position_size_fraction(stop_dist_pct, confidence=1.0,
                            portfolio_risk_pct=0.0, btc_blocked=False):
    if stop_dist_pct <= 0:
        return 0.0
    risk_budget_pct = RISK_PER_TRADE_PCT * max(0.5, min(1.5, confidence))
    if btc_blocked:
        risk_budget_pct *= BTC_STRONG_SIZE_MULT
    remaining = MAX_PORTFOLIO_RISK_PCT - portfolio_risk_pct
    if remaining <= 0:
        return 0.0
    risk_budget_pct = min(risk_budget_pct, remaining)
    size = risk_budget_pct / stop_dist_pct
    size = min(size, 0.25)
    return round(max(size, 0.0), 4)

def portfolio_risk_used():
    total = 0.0
    for v in state.values():
        if v.get("position") == "open" and v.get("entry_price"):
            dist = ((v["entry_price"] - v.get("stop", 0))
                    / v["entry_price"] * 100)
            total += dist * v.get("size_fraction", 1.0)
    return total

def confidence_for_signal(sig):
    score = sig.get("score", 6) if isinstance(sig.get("score", 6), int) else 6
    if score >= 9:
        return 1.5
    if score >= 8:
        return 1.3
    if score >= 7:
        return 1.1
    if score >= 6:
        return 1.0
    if score >= 5:
        return 0.8
    return 0.5
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
    logger.info("➕ Новый подписчик: %s (всего %d)",
                cid, len(SUBSCRIBERS))
    return True

def remove_subscriber(cid):
    with subscribers_lock:
        if cid not in SUBSCRIBERS or cid == MAIN_CHAT_ID:
            return False
        SUBSCRIBERS.discard(cid)
    _save_subscribers()
    logger.info("➖ Отписка: %s (осталось %d)",
                cid, len(SUBSCRIBERS))
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
            logger.error("Ошибка отправки Telegram chat=%s: %s",
                         chat_id, e)
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
        if ("<blockquote" in text
                and "</blockquote>" not in text.rsplit("<blockquote", 1)[1]):
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

def tp_label():
    return "REF" if NO_FIXED_TP else "TP"

HELP_TEXT = (
    "📡 <b>Bybit Scanner v21.6.0 «VOLUME-AWARE-HOLD» — справка</b>\n"
    "Все стратегии ищут дно.\n"
    "🎯 RANGE-BOUNCE: отскок от дна боковика (≤4%).\n"
    "💎 RSI-DIPBUY: RSI≤35 × 3св → отскок + объём + 1D bull.\n"
    "🌊 WT-DIP: WaveTrend кросс внизу (без EMA-фильтра).\n"
    "🔒 HOLD-TO-REVERSAL: BE +0.5×R, без partial TP.\n"
    "🚀 VOLUME-AWARE-HOLD (v21.6.0):\n"
    "   • у пробоя (≥ upper×0.97): vol_score ≥ 0.6 → держим\n"
    "   • vol_score < 0.4 + красная 4h → выход\n"
    "   • после пробоя → трейлинг 2×R, ложный пробой −0.5%\n"
    f"Выход по умолчанию: EMA-кросс 4h / трейлинг / SL / "
    f"time-stop {TIME_STOP_DAYS}д.\n"
    "🔧 BTC-фильтр: -10%/6ч или ADX>55. dip/bounce обходят.\n"
    "🛡 Peak Guard: реальный входной фильтр.\n"
    "💥 SQZ-dip отключён (убытки).\n"
    "Команды: /stop, /help, /funnel, /stats, /reset."
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
            r = requests.get(url, params={
                "offset": offset, "timeout": 30,
                "allowed_updates": '["message"]',
            }, timeout=40)
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
                            "Бот ищет ТОЛЬКО дно.\n"
                            "/stop — отписаться, /help — справка.")
                    else:
                        _post_telegram(cid, "✅ Вы уже подписаны.")
                elif text == "/stop":
                    if remove_subscriber(int(cid)):
                        _post_telegram(cid,
                            "🔴 Вы отписаны.\n/start — подписаться снова.")
                    else:
                        _post_telegram(cid, "Вы и так не подписаны.")
                elif text == "/help":
                    _post_telegram(cid, HELP_TEXT)
                elif text == "/funnel":
                    _post_telegram(cid, _format_funnel(funnel_snapshot()))
                elif text == "/stats":
                    _post_telegram(cid, _format_stats())
                elif text == "/reset":
                    _post_telegram(cid, _format_reset())
        except Exception as e:
            logger.warning("Polling ошибка: %s", e)
            time.sleep(3)

# ==================== СОСТОЯНИЕ ====================
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

# ==================== ЖУРНАЛ СДЕЛОК ====================
def log_trade(symbol, entry, exit_price, reason, strategy,
              entry_time=None, size_fraction=1.0, traded_fraction=1.0,
              daily_regime=None):
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
        net_pnl_pct = raw_pnl * traded_fraction - costs
        account_pnl_pct = net_pnl_pct * size_fraction
        trades.append({
            "symbol": symbol, "entry_price": entry,
            "exit_price": exit_price,
            "entry_time": entry_time, "exit_time": now_iso,
            "size_fraction": round(size_fraction, 4),
            "traded_fraction": round(traded_fraction, 4),
            "raw_pnl_pct": round(raw_pnl, 2),
            "net_pnl_pct": round(net_pnl_pct, 2),
            "account_pnl_pct": round(account_pnl_pct, 4),
            "pnl_pct": round(net_pnl_pct, 2),
            "result": "win" if net_pnl_pct > 0 else "loss",
            "reason": reason, "strategy": strategy,
            "daily_regime": daily_regime or "unknown",
        })
        with open(TRADES_LOG_FILE, "w") as f:
            json.dump(trades, f, indent=2)
        if traded_fraction >= 1.0:
            cb_register(net_pnl_pct, size_fraction)
        with SESSION_STATS_LOCK:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if SESSION_STATS["session_day"] != today:
                SESSION_STATS["session_day"] = today
                SESSION_STATS["entries"] = 0
                SESSION_STATS["exits"] = 0
            SESSION_STATS["exits"] += 1
        return round(net_pnl_pct, 2), round(account_pnl_pct, 4)

def register_entry():
    with SESSION_STATS_LOCK:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if SESSION_STATS["session_day"] != today:
            SESSION_STATS["session_day"] = today
            SESSION_STATS["entries"] = 0
            SESSION_STATS["exits"] = 0
        SESSION_STATS["entries"] += 1

def get_session_stats():
    with SESSION_STATS_LOCK:
        return dict(SESSION_STATS)

def _format_stats():
    try:
        if not os.path.exists(TRADES_LOG_FILE):
            return "📊 Сделок пока нет."
        with open(TRADES_LOG_FILE, "r") as f:
            trades = json.load(f)
        if not trades:
            return "📊 Сделок пока нет."
        total = len(trades)
        wins = sum(1 for t in trades if t.get("result") == "win")
        losses = total - wins
        sum_net = sum(t.get("net_pnl_pct", 0) for t in trades)
        sum_acc = sum(t.get("account_pnl_pct", 0) for t in trades)
        # разбивка по стратегиям
        by_strat = {}
        for t in trades:
            s = t.get("strategy", "?")
            if s not in by_strat:
                by_strat[s] = {"n": 0, "wins": 0, "sum": 0.0}
            by_strat[s]["n"] += 1
            if t.get("result") == "win":
                by_strat[s]["wins"] += 1
            by_strat[s]["sum"] += t.get("net_pnl_pct", 0)
        last5 = trades[-5:]
        lines = [
            "📊 <b>СТАТИСТИКА СДЕЛОК</b>",
            f"Всего: <b>{total}</b> · 🟢 {wins} · 🔴 {losses} · "
            f"WR <b>{wins / total * 100:.1f}%</b>",
            f"Σ PnL сделок: <b>{sum_net:+.2f}%</b>",
            f"Σ вклад в счёт: <b>{sum_acc:+.2f}%</b>",
            "── по стратегиям ──",
        ]
        for s, d in sorted(by_strat.items(),
                            key=lambda x: -x[1]["sum"]):
            wr = d["wins"] / d["n"] * 100 if d["n"] else 0
            lines.append(
                f"{s}: {d['n']} сделок · WR {wr:.0f}% · "
                f"Σ {d['sum']:+.2f}%")
        lines.append("── последние 5 ──")
        for t in last5:
            pnl = t.get("net_pnl_pct", 0)
            icon = "🟢" if pnl > 0 else "🔴"
            lines.append(
                f"{icon} {t.get('symbol', '?')} "
                f"{t.get('net_pnl_pct', 0):+.2f}% "
                f"({t.get('reason', '?')})")
        return "\n".join(lines)
    except Exception as e:
        return f"Ошибка /stats: {e}"

def _format_reset():
    try:
        if os.path.exists(TRADES_LOG_FILE):
            os.replace(TRADES_LOG_FILE, TRADES_LOG_FILE + ".bak")
        return "♻️ Журнал сделок сброшен (bybit_trades.json.bak)."
    except Exception as e:
        return f"Ошибка /reset: {e}"
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
    plus_dm = pd.Series(
        np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(
        np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    tr = atr(df, 1).replace(0, np.nan)
    plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False).mean() / tr)
    minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False).mean() / tr)
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * np.abs((plus_di - minus_di) / di_sum)
    return dx.ewm(alpha=1 / period, adjust=False).mean()

# ==================== 🌊 WAVETREND (LazyBear) ====================
def wavetrend(df, n1=10, n2=21, sma_len=4):
    ap = (df["high"] + df["low"] + df["close"]) / 3.0
    esa = ap.ewm(span=n1, adjust=False).mean()
    d = (ap - esa).abs().ewm(span=n1, adjust=False).mean()
    ci = (ap - esa) / (0.015 * d.replace(0, np.nan))
    tci = ci.ewm(span=n2, adjust=False).mean()
    wt1 = tci
    wt2 = wt1.rolling(sma_len).mean()
    return wt1, wt2

def detect_wt_buy_cross(df, os_level=None):
    if not WT_ENABLED:
        return False
    if os_level is None:
        os_level = WT_OS2
    if len(df) < 25:
        return False
    try:
        wt1, wt2 = wavetrend(df, WT_N1, WT_N2, WT_SMA_LEN)
    except Exception:
        return False
    if len(wt1) < 3:
        return False
    prev1, prev2 = wt1.iloc[-3], wt2.iloc[-3]
    last1, last2 = wt1.iloc[-2], wt2.iloc[-2]
    if (pd.isna(prev1) or pd.isna(prev2)
            or pd.isna(last1) or pd.isna(last2)):
        return False
    cross_up = prev1 <= prev2 and last1 > last2
    return bool(cross_up and last1 < os_level)

# ==================== 💥 SQUEEZE MOMENTUM (LazyBear) ====================
def squeeze_momentum(df, bb_len=20, bb_mult=2.0, kc_len=20, kc_mult=1.5):
    src = df["close"]
    basis = src.rolling(bb_len).mean()
    dev = bb_mult * src.rolling(bb_len).std(ddof=0)
    upper_bb = basis + dev
    lower_bb = basis - dev

    ma = src.rolling(kc_len).mean()
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    rangema = tr.rolling(kc_len).mean()
    upper_kc = ma + rangema * kc_mult
    lower_kc = ma - rangema * kc_mult

    sqz_on = (lower_bb > lower_kc) & (upper_bb < upper_kc)
    sqz_off = (lower_bb < lower_kc) & (upper_bb > upper_kc)

    hh = df["high"].rolling(kc_len).max()
    ll = df["low"].rolling(kc_len).min()
    mid = (hh + ll) / 2.0
    sma_close = src.rolling(kc_len).mean()
    delta = src - (mid + sma_close) / 2.0
    try:
        val = delta.rolling(kc_len).apply(
            lambda y: np.polyval(
                np.polyfit(np.arange(len(y)), y, 1), len(y) - 1)
            if not np.any(np.isnan(y)) else np.nan,
            raw=True)
    except Exception:
        val = pd.Series(np.nan, index=df.index)
    return val, sqz_on, sqz_off

def detect_sqz_release_bull(df, max_bars=None):
    if not SQZ_ENABLED:
        return False
    if max_bars is None:
        max_bars = SQZ_RELEASE_MAX_BARS
    if len(df) < SQZ_KC_LEN + 5:
        return False
    try:
        val, sqz_on, sqz_off = squeeze_momentum(
            df, SQZ_BB_LEN, SQZ_BB_MULT, SQZ_KC_LEN, SQZ_KC_MULT)
    except Exception:
        return False
    n = len(sqz_on)
    for i in range(1, max_bars + 1):
        idx = n - 2 - i
        if idx < 1:
            return False
        if bool(sqz_on.iloc[idx - 1]) and bool(sqz_off.iloc[idx]):
            v_now = val.iloc[idx]
            v_prev = val.iloc[idx - 1]
            if pd.isna(v_now) or pd.isna(v_prev):
                return False
            return bool(v_now > v_prev)
    return False

def sqz_state(df):
    if not SQZ_ENABLED or len(df) < SQZ_KC_LEN + 5:
        return 0.0, False, False, "grey"
    try:
        val, sqz_on, sqz_off = squeeze_momentum(
            df, SQZ_BB_LEN, SQZ_BB_MULT, SQZ_KC_LEN, SQZ_KC_MULT)
    except Exception:
        return 0.0, False, False, "grey"
    v_now = float(val.iloc[-2]) if not pd.isna(val.iloc[-2]) else 0.0
    v_prev = (float(val.iloc[-3])
              if len(val) > 3 and not pd.isna(val.iloc[-3]) else v_now)
    is_on = bool(sqz_on.iloc[-2]) if not pd.isna(sqz_on.iloc[-2]) else False
    is_off = bool(sqz_off.iloc[-2]) if not pd.isna(sqz_off.iloc[-2]) else False
    if v_now > 0:
        color = "lime" if v_now > v_prev else "green"
    else:
        color = "maroon" if v_now > v_prev else "red"
    return v_now, is_on, is_off, color

# ==================== ANALYZE TIMEFRAME ====================
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
    adx_slope_up = (bool(last["adx"] > adx_ago)
                    if not pd.isna(adx_ago) else False)
    vol_sma = last["vol_sma"]
    vol_ratio = (float(last["volume"] / vol_sma)
                 if not pd.isna(vol_sma) and vol_sma > 0 else 0.0)
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
        range_pct = (window["close"].max()
                     - window["close"].min()) / mean * 100
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
    return {
        "days": len(window),
        "range_pct": range_pct,
        "adx": float(adx_val),
        "upper_level": float(window["high"].max()),
        "lower_level": float(window["low"].min()),
        "vol_trend": round(vol_trend, 2),
    }
    # ==================== ВЫХОДЫ (v21.6.0) ====================
def _get_r_unit(pos):
    """R-unit из позиции. r_unit_price = entry - stop при входе."""
    entry = pos.get("entry_price") or 0.0
    if entry <= 0:
        return 0.0
    r_unit = pos.get("r_unit_price") or 0.0
    if r_unit > 0:
        return r_unit
    atr_ref = pos.get("atr_ref") or 0.0
    return atr_ref if atr_ref > 0 else entry * 0.01


def _volume_before_breakout(r4h):
    """
    v21.6.0: оценка «есть ли агрессивный покупатель» у пробоя.
    Score 0.0..1.0 из 4 компонент:
      vol_ratio (4h vs SMA20), ADX slope, trend_up (EMA9>EMA21), MACD.
    """
    if not r4h:
        return 0.0
    score = 0.0
    weights = 0.0

    vr = r4h.get("vol_ratio", 0.0)
    if vr >= 1.5:
        score += 1.0
    elif vr >= 1.2:
        score += 0.7
    elif vr >= 1.0:
        score += 0.4
    else:
        score += 0.0
    weights += 1.0

    if r4h.get("adx_slope_up"):
        score += 1.0
    elif r4h.get("adx", 0) >= 25:
        score += 0.5
    weights += 1.0

    if r4h.get("trend_up"):
        score += 1.0
    weights += 1.0

    if r4h.get("macd_line", 0) > r4h.get("macd_signal", 0):
        score += 0.7
    weights += 1.0

    return round(score / weights, 3) if weights > 0 else 0.0


def check_exit(results, pos):
    """
    v21.6.0: 3 режима позиции + broke_out flag.

    ДНО       — обычная логика (BE, EMA-кросс, трейлинг 2×R).
    У ПРОБОЯ  — cur ≥ upper×0.97:
                vol_score ≥ 0.6 → держим (EMA-кросс игнор)
                vol_score < 0.4 + красная 4h → выход
                0.4–0.6 → падаем в дно-логику
    ПОСЛЕ     — cur > upper (broke_out=True):
                трейлинг 2×R под high,
                возврат ниже upper−0.5% → «Ложный пробой».
    """
    if not results.get(TRIGGER_TF) or not results.get("1d"):
        return False, "", 0.0
    r4h = results[TRIGGER_TF]

    entry = pos.get("entry_price") or 0.0
    if entry <= 0:
        return False, "", 0.0

    # SL — всегда
    if r4h["low"] <= pos["stop"]:
        return True, "Stop-Loss", min(r4h["open"], pos["stop"])

    # Fixed TP
    if not NO_FIXED_TP:
        if r4h["high"] >= pos["target"]:
            return True, "Take-Profit", pos["target"]

    r_unit = _get_r_unit(pos)
    if r_unit <= 0:
        return False, "", 0.0

    # Partial TP
    if not NO_PARTIAL_TP:
        if (not pos.get("partial_done")
                and r4h["high"] >= entry + PARTIAL_TP_ATR * r_unit):
            pos["partial_done"] = True
            pos["stop"] = max(pos["stop"], entry * 1.001)
            return False, "partial", entry + PARTIAL_TP_ATR * r_unit

    # Breakeven
    be_trig = (BREAKEVEN_TRIGGER_ATR_FAST
               if HOLD_MODE_ENABLED else BREAKEVEN_TRIGGER_ATR)
    if (not pos.get("breakeven_moved")
            and r4h["close"] >= entry + be_trig * r_unit):
        pos["breakeven_moved"] = True
        pos["stop"] = max(pos["stop"], entry * 1.001)

    # Базовый трейлинг +2R
    if r4h["close"] >= entry + TRAILING_TRIGGER_ATR * r_unit:
        pos["highest_close"] = max(
            pos.get("highest_close", r4h["close"]), r4h["close"])
        new_stop = pos["highest_close"] - TRAILING_STEP_ATR * r_unit
        floor = (entry * 1.001
                 if (pos.get("partial_done") or pos.get("breakeven_moved"))
                 else pos["stop"])
        if new_stop > pos["stop"]:
            pos["stop"] = max(new_stop, floor)

    # ===== v21.6.0: 3-режимная логика =====
    box_upper = pos.get("box_upper")
    cur = r4h["close"]

    # Проверка ложного пробоя (только если уже был пробой)
    if (box_upper and box_upper > 0
            and pos.get("broke_out")):
        false_thr = box_upper * (1 - VOL_HOLD_FALSE_BREAK_PCT / 100)
        if cur < false_thr:
            return True, "Ложный пробой", cur

    if VOL_HOLD_ENABLED and box_upper and box_upper > 0:
        near_upper_thr = box_upper * (1 - VOL_HOLD_NEAR_UPPER_PCT / 100)

        # --- РЕЖИМ: ПОСЛЕ ПРОБОЯ ---
        if cur > box_upper:
            pos["broke_out"] = True
            pos["near_upper_bars"] = 0
            pos["highest_close"] = max(
                pos.get("highest_close", cur), cur)
            trail_stop = (pos["highest_close"]
                          - VOL_HOLD_AFTER_BREAK_TRAIL * r_unit)
            floor = (entry * 1.001
                     if (pos.get("partial_done")
                         or pos.get("breakeven_moved"))
                     else pos["stop"])
            if trail_stop > pos["stop"]:
                pos["stop"] = max(trail_stop, floor)
            return False, "", 0.0

        # --- РЕЖИМ: У ПРОБОЯ ---
        if cur >= near_upper_thr:
            pos["near_upper_bars"] = pos.get("near_upper_bars", 0) + 1
            vol_score = _volume_before_breakout(r4h)
            pos["last_vol_score"] = vol_score

            red_candle = r4h["close"] < r4h["open"]

            # Зона 1: сильный объём → держим
            if vol_score >= VOL_HOLD_SCORE_KEEP:
                if pos["near_upper_bars"] >= VOL_HOLD_MAX_BARS_NEAR:
                    return True, "Не пробили upper", cur
                return False, "", 0.0

            # Зона 2: слабый объём + красная → выход
            if vol_score < VOL_HOLD_SCORE_WEAK and red_candle:
                return True, "Слабый объём у пробоя", cur

            # Зона 3 (0.4–0.6): падаем в дно-логику ниже
        else:
            # Ушли от upper — сбрасываем счётчик
            pos["near_upper_bars"] = 0

    # --- РЕЖИМ: ДНО ---
    if r4h.get("ema_cross_down"):
        return True, "Разворот", r4h["close"]
    return False, "", 0.0

# ==================== ВХОД / ФИЛЬТРЫ ====================
def can_enter(pair, signal=None, signal_risk_pct=None):
    now = time.time()
    trade_times[:] = [t for t in trade_times if now - t < 3600]
    if not cb_can_trade():
        return False

    if not market_allows_longs():
        if not _signal_bypasses_btc(signal):
            if signal is None or not is_strong_signal_for_blocked_market(signal):
                funnel_inc("btc_block")
                return False

    if DAILY_REGIME_FILTER_ENABLED:
        regime = daily_regime_bull(pair)
        if regime is False:
            sc = signal.get("score", 0) if signal else 0
            if isinstance(sc, str):
                sc_num = 6
            else:
                sc_num = int(sc) if sc is not None else 0
            if sc_num < DAILY_REGIME_BLOCK_BELOW_SCORE:
                logger.info("🚫 Bear 1D блок %s (score=%s)", pair, sc)
                return False

    old_state = state.get(pair, {})
    if old_state.get("position") == "open":
        return False
    if (now - float(old_state.get("last_exit_ts", 0) or 0)
            < ENTRY_COOLDOWN_SECONDS):
        return False
    open_positions = sum(
        1 for p in state.values() if p.get("position") == "open")
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

# ==================== ТРЕНД-КЭШ + ДНЕВНОЙ РЕЖИМ ====================
def get_trend(symbol):
    with trend_cache_lock:
        return trend_cache.get(symbol)

def compute_tf_trend(df, params):
    if df is None or df.empty or len(df) < 40:
        return False
    f = ema(df["close"], params["ema_fast"])
    s = ema(df["close"], params["ema_slow"])
    if (len(f) < 6 or pd.isna(f.iloc[-2])
            or pd.isna(s.iloc[-2]) or pd.isna(f.iloc[-5])):
        return False
    return bool(f.iloc[-2] > s.iloc[-2] and f.iloc[-2] > f.iloc[-5])

def refresh_trend_for(pair):
    res = {}
    for tf in ("4h", "1d", "1w"):
        p = TIMEFRAME_PARAMS[tf]
        df = pd.DataFrame(fetch_klines(
            pair, p["bybit_interval"], p["min_bars"]))
        res[tf] = compute_tf_trend(df, p)
        time.sleep(0.15)

    d_ema50_gt_200 = None
    d_close_above_200 = None
    try:
        df_reg = pd.DataFrame(fetch_klines(pair, "D", DAILY_REGIME_FETCH_BARS))
        if len(df_reg) >= 210:
            ef50 = ema(df_reg["close"], 50).iloc[-2]
            ef200 = ema(df_reg["close"], 200).iloc[-2]
            close_prev = float(df_reg["close"].iloc[-2])
            if not pd.isna(ef50) and not pd.isna(ef200):
                d_ema50_gt_200 = bool(ef50 > ef200)
                d_close_above_200 = bool(close_prev > ef200)
    except Exception as e:
        logger.debug("daily regime %s: %s", pair, e)

    score = sum(1 for v in res.values() if v)
    with trend_cache_lock:
        trend_cache[pair] = {
            "score": score,
            "d_ema50_gt_200": d_ema50_gt_200,
            "d_close_above_200": d_close_above_200,
            **res,
        }
    return score

def refresh_all_trends():
    with state_lock:
        open_pairs = [p for p, v in state.items()
                      if v.get("position") == "open"]
    watch = list(dict.fromkeys(LAST_FILTERED_PAIRS + open_pairs))
    q3 = 0
    bull_count = 0
    for pair in watch:
        try:
            if refresh_trend_for(pair) == 3:
                q3 += 1
            ti = get_trend(pair)
            if (ti and ti.get("d_ema50_gt_200")
                    and ti.get("d_close_above_200")):
                bull_count += 1
        except Exception as e:
            logger.debug("trend %s: %s", pair, e)
    logger.info("Тренд-кэш: %d пар · 3/3=%d · 1D-bull=%d",
                len(watch), q3, bull_count)

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

def daily_regime_bull(symbol):
    ti = get_trend(symbol)
    if ti is None:
        return None
    d50 = ti.get("d_ema50_gt_200")
    d200 = ti.get("d_close_above_200")
    if d50 is None or d200 is None:
        return None
    return bool(d50 and d200)

def daily_regime_str(symbol):
    r = daily_regime_bull(symbol)
    if r is True:
        return "bull"
    if r is False:
        return "bear"
    return "unknown"

# ==================== ВСЕЛЕННАЯ ПАР ====================
FIAT_BASES = {"AUD", "GBP", "EUR", "CAD", "CHF", "JPY", "USD",
              "BRL", "MXN", "TRY", "ZAR", "INR", "SGD", "HKD"}
STABLECOINS = {"USDC", "USDT", "DAI", "PYUSD", "TUSD", "FDUSD", "AUSD",
               "EURR", "USDR", "FRNT", "EURQ", "USDPT", "BRL1", "EUROP",
               "USDTB", "USD1", "RLUSD", "EURT", "GUSD", "FRAX", "LUSD",
               "SUSD", "USDP"}
STOCK_TOKENS = {"AAPLX", "AMZNX", "GOOGLX", "MCDX", "TSLAX", "COINX"}
STABLE_SUBSTRINGS = ("USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY",
                     "BRL", "MXN")

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
                  if sym in tickers
                  and tickers[sym]["turnover"] >= CONSOLIDATION_MIN_TURNOVER_USD]
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

# ==================== 🎯 RANGE-BOUNCE (v21.6.0) ====================
def evaluate_ws_range_bounce(df, symbol, lower_level, upper_level):
    """Отскок от дна боковика + оценка накопления (vol_trend)."""
    funnel_inc("bounce_total")
    if not RANGE_BOUNCE_ENABLED:
        return None
    if (not lower_level or not upper_level
            or lower_level <= 0 or upper_level <= lower_level):
        return None
    if len(df) < MIN_BARS + 1:
        return None

    sig = df.iloc[-2]
    if pd.isna(sig["atr"]) or sig["atr"] <= 0:
        return None
    if pd.isna(sig["rsi"]):
        return None

    # 1) v21.6.0: касание зоны в последних 5 свечах
    zone = lower_level * (1 + RANGE_BOUNCE_ZONE_PCT / 100)
    recent = df.iloc[-6:-1]
    touched_recently = bool((recent["low"] <= zone).any())
    if not touched_recently:
        return None
    funnel_inc("bounce_zone")

    # 2) RSI
    if sig["rsi"] > RANGE_BOUNCE_RSI_MAX:
        return None
    funnel_inc("bounce_rsi")

    # 3) Зелёная свеча (на текущей закрытой)
    if not (sig["close"] > sig["open"]):
        return None
    funnel_inc("bounce_green")

    # 4) Объём свечи
    vol_ratio = (float(sig["vol_ratio"])
                 if not pd.isna(sig["vol_ratio"]) else 0.0)
    if vol_ratio < RANGE_BOUNCE_VOL_MULT:
        return None
    funnel_inc("bounce_vol")

    entry = float(df.iloc[-1]["close"])
    atr_value = float(sig["atr"])
    stop = lower_level * (1 - RANGE_BOUNCE_SL_PCT / 100)
    target = float(upper_level)
    if stop >= entry or target <= entry:
        return None
    risk = entry - stop
    if risk <= 0:
        return None
    rr = (target - entry) / risk
    if rr < RANGE_BOUNCE_MIN_RR:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    funnel_inc("bounce_rr")

    # Peak Guard
    pg_ok, pg_reason = peak_guard_ok(df, symbol, entry, sig["rsi"])
    if not pg_ok:
        logger.debug("bounce peak-block %s: %s", symbol, pg_reason)
        return None

    ti = get_trend(symbol)
    trend_score = ti["score"] if ti else 0

    # v21.6.0: накопление в боковике
    box_vol_trend = 1.0
    with breakout_cache_lock:
        ce = breakout_cache.get(symbol)
    if isinstance(ce, dict):
        box_vol_trend = float(ce.get("vol_trend", 1.0) or 1.0)

    score = 7
    parts = [
        f"🎯 Bounce от дна боковика (lower={lower_level:.6g})",
        f"RSI {sig['rsi']:.0f}, объём {vol_ratio:.1f}×",
        "🛡 Peak OK",
    ]
    if box_vol_trend >= 1.1:
        score += 1
        parts.append(f"🌊 Накопление (vol_trend {box_vol_trend:.2f}×) +1")
    elif box_vol_trend < 0.7:
        score -= 1
        parts.append(f"🥀 Распределение (vol_trend {box_vol_trend:.2f}×) −1")

    return {
        "entry": entry, "stop": stop, "target": target, "atr": atr_value,
        "score": score, "trend_score": trend_score,
        "parts": parts,
        "strategy": "range_bounce",
        "bottom_ok": True,
        "hold_mode": True,
        "box_upper": float(upper_level),
        "box_vol_trend": box_vol_trend,
    }

# ==================== 💎 RSI DIP-BUY (v21.6.0) ====================
def evaluate_ws_dipbuy(df, symbol):
    funnel_inc("dip_total")
    if not DIPBUY_ENABLED or len(df) < MIN_BARS + 1:
        return None

    if DIPBUY_REQUIRE_DAILY_BULL:
        regime = daily_regime_bull(symbol)
        if regime is not True:
            if DIPBUY_LOG_REJECTS:
                logger.debug("dipbuy reject %s: no 1D-bull", symbol)
            return None
    funnel_inc("dip_bull")

    need = DIPBUY_RSI_MIN_BARS + 1
    if len(df) < need:
        return None

    last = df.iloc[-2]
    if pd.isna(last["rsi"]) or pd.isna(last["atr"]) or last["atr"] <= 0:
        return None

    zone_slice = df["rsi"].iloc[-DIPBUY_RSI_MIN_BARS-1:-1]
    if zone_slice.isna().any():
        return None
    if not bool((zone_slice <= DIPBUY_RSI_ZONE).all()):
        return None
    funnel_inc("dip_zone")

    rsi_now = float(df["rsi"].iloc[-2])
    rsi_prev = float(df["rsi"].iloc[-3])
    if rsi_now <= rsi_prev:
        return None
    funnel_inc("dip_rising")

    if not (last["close"] > last["open"]):
        return None
    funnel_inc("dip_green")

    if DIPBUY_REQUIRE_BB:
        bb_ma = df["close"].rolling(DIPBUY_BB_PERIOD).mean().iloc[-2]
        bb_sd = df["close"].rolling(DIPBUY_BB_PERIOD).std().iloc[-2]
        touched_bb = False
        if not pd.isna(bb_ma) and not pd.isna(bb_sd):
            lower_bb = bb_ma - DIPBUY_BB_STD * bb_sd
            touched_bb = bool(df["low"].iloc[-4:-1].min() <= lower_bb)
        if not touched_bb:
            return None
    funnel_inc("dip_bb")

    vol_ratio = (float(last["vol_ratio"])
                 if not pd.isna(last["vol_ratio"]) else 0.0)
    if vol_ratio < DIPBUY_MIN_VOL:
        return None
    funnel_inc("dip_vol")

    if DIPBUY_REQUIRE_EMA_UP:
        if not (last["ema_fast"] > last["ema_slow"] * DIPBUY_EMA_SLACK):
            if DIPBUY_LOG_REJECTS:
                logger.debug("dipbuy reject %s: EMA9<=EMA21×%.3f",
                             symbol, DIPBUY_EMA_SLACK)
            return None
    funnel_inc("dip_ema")

    entry = float(df.iloc[-1]["close"])
    atr_value = float(last["atr"])
    stop = entry - atr_value * DIPBUY_SL_ATR
    target = entry + atr_value * DIPBUY_TP_ATR
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    if not _rr_ok(entry, stop, target):
        return None

    pg_ok, pg_reason = peak_guard_ok(df, symbol, entry, rsi_now)
    if not pg_ok:
        logger.debug("dipbuy peak-block %s: %s", symbol, pg_reason)
        return None

    ti = get_trend(symbol)
    trend_score = ti["score"] if ti else 0

    wt_confirm = detect_wt_buy_cross(df)
    sqz_release = detect_sqz_release_bull(df)
    score = 6
    parts = [
        f"💎 RSI зона≤{DIPBUY_RSI_ZONE} ×{DIPBUY_RSI_MIN_BARS}св "
        f"→ отскок {rsi_prev:.0f}→{rsi_now:.0f}",
        f"Объём {vol_ratio:.1f}× · 🛡 Peak OK",
        "1D аптренд ✓",
    ]
    if wt_confirm:
        score += 2
        parts.append("🌊 WT-кросс +2")
    if sqz_release:
        score += 1
        parts.append("💥 SQZ-release +1")
    return {
        "entry": entry, "stop": stop, "target": target, "atr": atr_value,
        "score": score, "trend_score": trend_score, "parts": parts,
        "strategy": "rsi_dipbuy",
        "wt_confirm": wt_confirm, "sqz_release": sqz_release,
        "hold_mode": True,
    }

# ==================== 🌊 WT-DIP (v21.6.0) ====================
def evaluate_ws_wt_dip(df, symbol):
    funnel_inc("wt_total")
    if not WT_ENABLED or not WT_DIP_STRATEGY_ENABLED:
        return None
    if len(df) < MIN_BARS + 1:
        return None
    if DIPBUY_REQUIRE_DAILY_BULL:
        if daily_regime_bull(symbol) is not True:
            return None
    funnel_inc("wt_bull")

    sig = df.iloc[-2]
    rsi_now = float(sig["rsi"]) if not pd.isna(sig["rsi"]) else 0.0
    if not (WT_HOT_RSI_MIN <= rsi_now <= WT_HOT_RSI_MAX):
        return None
    funnel_inc("wt_rsi")

    if not detect_wt_buy_cross(df):
        return None
    funnel_inc("wt_cross")

    if pd.isna(sig["atr"]) or sig["atr"] <= 0:
        return None

    if WT_DIP_REQUIRE_EMA_UP:
        if not (sig["ema_fast"] > sig["ema_slow"] * DIPBUY_EMA_SLACK):
            return None
    funnel_inc("wt_ema")

    if not (sig["close"] > sig["open"]):
        return None
    funnel_inc("wt_green")

    entry = float(df.iloc[-1]["close"])
    if not _bottom_filter_ok(symbol, entry):
        return None
    funnel_inc("wt_bottom")

    pg_ok, pg_reason = peak_guard_ok(df, symbol, entry, rsi_now)
    if not pg_ok:
        logger.debug("wt_dip peak-block %s: %s", symbol, pg_reason)
        return None

    atr_value = float(sig["atr"])
    stop = entry - atr_value * WT_DIP_SL_ATR
    target = entry + atr_value * WT_DIP_TP_ATR
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    if not _rr_ok(entry, stop, target):
        return None

    ti = get_trend(symbol)
    trend_score = ti["score"] if ti else 0
    wt1, _ = wavetrend(df, WT_N1, WT_N2, WT_SMA_LEN)
    wt_now = float(wt1.iloc[-2]) if not pd.isna(wt1.iloc[-2]) else 0.0
    return {
        "entry": entry, "stop": stop, "target": target, "atr": atr_value,
        "score": 7, "trend_score": trend_score,
        "parts": [
            f"🌊 WT-кросс внизу (wt1={wt_now:.0f})",
            "🎯 BOTTOM ✓ · 🛡 Peak OK",
            "1D аптренд ✓ (без EMA)",
        ],
        "strategy": "wt_dip",
        "wt_confirm": True, "bottom_ok": True,
        "hold_mode": True,
    }

# ==================== 💥 SQZ-DIP (ОТКЛЮЧЁН) ====================
def evaluate_ws_sqz_dip(df, symbol):
    funnel_inc("sqzdip_total")
    if not SQZ_ENABLED or not SQZ_DIP_STRATEGY_ENABLED:
        return None
    if len(df) < MIN_BARS + 1:
        return None
    if DIPBUY_REQUIRE_DAILY_BULL:
        if daily_regime_bull(symbol) is not True:
            return None
    funnel_inc("sqzdip_bull")
    if not detect_sqz_release_bull(df):
        return None
    funnel_inc("sqzdip_release")
    sig = df.iloc[-2]
    if pd.isna(sig["atr"]) or sig["atr"] <= 0:
        return None
    if pd.isna(sig["rsi"]):
        return None
    if SQZ_DIP_RSI_RISING:
        rsi_now = float(df["rsi"].iloc[-2])
        rsi_prev = float(df["rsi"].iloc[-3])
        if rsi_now <= rsi_prev:
            return None
    funnel_inc("sqzdip_rsi")
    if not (sig["close"] > sig["open"]):
        return None
    funnel_inc("sqzdip_green")
    if SQZ_DIP_CLOSE_ABOVE_EMA9 and not (sig["close"] > sig["ema_fast"]):
        return None
    vol_ratio = (float(sig["vol_ratio"])
                 if not pd.isna(sig["vol_ratio"]) else 0.0)
    if vol_ratio < SQZ_DIP_VOL_MIN:
        return None
    funnel_inc("sqzdip_vol")
    entry = float(df.iloc[-1]["close"])
    if not _bottom_filter_ok(symbol, entry):
        return None
    funnel_inc("sqzdip_bottom")
    pg_ok, _ = peak_guard_ok(df, symbol, entry, sig["rsi"])
    if not pg_ok:
        return None
    atr_value = float(sig["atr"])
    stop = entry - atr_value * SQZ_DIP_SL_ATR
    target = entry + atr_value * SQZ_DIP_TP_ATR
    if stop >= entry or target <= entry:
        return None
    if (entry - stop) / entry * 100 < MIN_STOP_DISTANCE_PCT:
        return None
    if not _rr_ok(entry, stop, target):
        return None
    ti = get_trend(symbol)
    trend_score = ti["score"] if ti else 0
    try:
        val_now, is_on, is_off, color = sqz_state(df)
    except Exception:
        val_now, color = 0.0, "grey"
    return {
        "entry": entry, "stop": stop, "target": target, "atr": atr_value,
        "score": 7, "trend_score": trend_score,
        "parts": [
            f"💥 SQZ-release внизу (val={val_now:.4g}, {color})",
            f"RSI растёт, объём {vol_ratio:.1f}×",
            "🎯 BOTTOM ✓", "1D аптренд ✓",
        ],
        "strategy": "sqz_dip",
        "sqz_release": True, "bottom_ok": True,
        "hold_mode": True,
    }

# ==================== ОТКРЫТИЕ ПОЗИЦИИ (v21.6.0) ====================
def open_position(symbol, sig, closed_start, also_valid=None):
    btc_blocked_raw = not market_allows_longs()
    bypassed = _signal_bypasses_btc(sig)
    btc_blocked = btc_blocked_raw and not bypassed
    if btc_blocked:
        if not is_strong_signal_for_blocked_market(sig):
            return None

    entry = sig["entry"]
    stop = sig["stop"]
    risk_pct = (entry - stop) / entry * 100
    if not can_enter(symbol, signal=sig, signal_risk_pct=risk_pct):
        return None

    old_state = state.get(symbol, {})
    if old_state.get("last_signal_candle") == closed_start:
        return None

    confidence = confidence_for_signal(sig)
    size = position_size_fraction(
        stop_dist_pct=risk_pct,
        confidence=confidence,
        portfolio_risk_pct=portfolio_risk_used(),
        btc_blocked=btc_blocked,
    )
    if size <= 0:
        logger.info("🚫 %s: size=0 (риск-лимит)", symbol)
        return None

    r_unit_price = max(entry - stop, entry * 0.001)

    new_state = old_state.copy()
    new_state.update({
        "position": "open",
        "entry_price": entry,
        "stop": stop,
        "target": sig["target"],
        "atr_ref": sig["atr"],
        "r_unit_price": r_unit_price,
        "entry_time": datetime.now(timezone.utc).isoformat(),
        "last_signal_candle": closed_start,
        "last_entry_ts": time.time(),
        "strategy": sig["strategy"],
        "score": sig["score"],
        "size_fraction": size,
        "btc_blocked_entry": btc_blocked,
        "daily_regime_at_entry": daily_regime_str(symbol),
        "hold_mode": sig.get("hold_mode", True),
        "confidence": confidence,
        "also_valid": also_valid or [],
        # v21.6.0: 3-режимная логика
        "box_upper": sig.get("box_upper"),
        "box_vol_trend": sig.get("box_vol_trend", 1.0),
        "near_upper_bars": 0,
        "last_vol_score": 0.0,
        "broke_out": False,
    })
    state[symbol] = new_state
    trade_times.append(time.time())
    save_state(state)
    funnel_inc("opened")
    register_entry()
    return new_state
    # ==================== HOT-POOLS ====================
def _cand_hotness(s):
    return abs(s.get("macd_gap_pct", 999))

def _cons_hotness(item):
    cur = item.get("current_price", 0)
    lvl = item.get("upper_level", 0)
    if cur <= 0 or lvl <= 0:
        return 999
    dist = (lvl - cur) / cur * 100
    # v21.6.0: пробитые — в конец списка
    if dist < 0:
        return 10_000 + abs(dist)
    return dist

def _dip_hotness(s):
    return s.get("rsi_now", 999)

def _wtdip_hotness(s):
    return s.get("wt_now", 999)

def _bounce_hotness(s):
    return s.get("dist_to_bottom", 999)

def _rebuild_hot_pools_from_buffers():
    """Отбор «горячих» пулов из ohlc_buffers (без REST)."""
    cand_pool = []
    cons_pool = []
    dip_pool = []
    wtdip_pool = []
    bounce_pool = []

    for sym, buf in list(ohlc_buffers.items()):
        if not buf or len(buf) < MIN_BARS:
            continue
        try:
            df = pd.DataFrame(list(buf))
            df["ema_fast"] = ema(df["close"], 9)
            df["ema_slow"] = ema(df["close"], 21)
            df["macd_line"] = ema(df["close"], 12) - ema(df["close"], 26)
            df["macd_signal"] = ema(df["macd_line"], 9)
            df["rsi"] = rsi(df["close"])
            last = df.iloc[-2]
            if pd.isna(last["macd_line"]) or pd.isna(last["macd_signal"]):
                continue
            macd_gap_pct = ((last["macd_line"] - last["macd_signal"])
                            / last["close"] * 100) if last["close"] else 0.0
            ti = get_trend(sym)
            trend_score = ti["score"] if ti else 0

            # --- cand pool ---
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

            # --- cons pool ---
            with breakout_cache_lock:
                cache_entry = breakout_cache.get(sym)
            upper_lvl = None
            lower_lvl = None
            if isinstance(cache_entry, dict):
                upper_lvl = cache_entry.get("upper")
                lower_lvl = cache_entry.get("lower")
            elif isinstance(cache_entry, (int, float)):
                upper_lvl = cache_entry

            if upper_lvl and upper_lvl > 0:
                cur = float(last["close"])
                dist_pct = (upper_lvl - cur) / cur * 100 if cur > 0 else 999
                if 0 <= dist_pct <= WS_HOT_NEAR_BREAKOUT_PCT:
                    cons_pool.append({
                        "pair": sym,
                        "upper_level": upper_lvl,
                        "current_price": cur,
                        "dist_to_break_pct": dist_pct,
                        "days": 0, "range_pct": 0.0, "adx": 0.0,
                        "vol_trend": 1.0,
                        "source": "buffers",
                    })

            # --- bounce pool ---
            if (RANGE_BOUNCE_ENABLED and lower_lvl and upper_lvl
                    and lower_lvl > 0 and upper_lvl > lower_lvl):
                cur = float(last["close"])
                dist_to_bottom = (cur - lower_lvl) / lower_lvl * 100
                if 0 <= dist_to_bottom <= RANGE_BOUNCE_HOT_ZONE_PCT:
                    bounce_pool.append({
                        "pair": sym,
                        "lower": lower_lvl,
                        "upper": upper_lvl,
                        "current_price": cur,
                        "dist_to_bottom": dist_to_bottom,
                        "rsi_now": (float(last["rsi"])
                                    if not pd.isna(last["rsi"]) else 50),
                        "source": "buffers",
                    })

            # --- dip pool ---
            if DIPBUY_ENABLED and not pd.isna(last["rsi"]):
                rsi_now = float(last["rsi"])
                regime = daily_regime_bull(sym)
                if (regime is True
                        and WS_HOT_DIP_RSI_MIN <= rsi_now <= WS_HOT_DIP_RSI_MAX
                        and last["ema_fast"]
                            > last["ema_slow"] * DIPBUY_EMA_SLACK):
                    dip_pool.append({
                        "pair": sym,
                        "rsi_now": rsi_now,
                        "current_price": float(last["close"]),
                        "close_price": float(last["close"]),
                        "source": "buffers",
                    })

            # --- wtdip pool ---
            if (WT_ENABLED and WT_DIP_STRATEGY_ENABLED
                    and not pd.isna(last["rsi"])):
                rsi_now = float(last["rsi"])
                regime = daily_regime_bull(sym)
                if (regime is True
                        and WT_HOT_RSI_MIN <= rsi_now <= WT_HOT_RSI_MAX
                        and last["ema_fast"]
                            > last["ema_slow"] * DIPBUY_EMA_SLACK):
                    try:
                        wt1, wt2 = wavetrend(df, WT_N1, WT_N2, WT_SMA_LEN)
                        wt_now = (float(wt1.iloc[-2])
                                  if not pd.isna(wt1.iloc[-2]) else 999)
                        wt_prev = (float(wt1.iloc[-3])
                                   if len(wt1) > 3
                                   and not pd.isna(wt1.iloc[-3]) else wt_now)
                        if wt_now <= 0 or (wt_now < 0 and wt_now > wt_prev):
                            wtdip_pool.append({
                                "pair": sym,
                                "wt_now": wt_now,
                                "rsi_now": rsi_now,
                                "current_price": float(last["close"]),
                                "close_price": float(last["close"]),
                                "source": "buffers",
                            })
                    except Exception:
                        pass
        except Exception as e:
            logger.debug("_rebuild_hot_pools_from_buffers %s: %s", sym, e)

    cand_pool.sort(key=_cand_hotness)
    cons_pool.sort(key=_cons_hotness)
    dip_pool.sort(key=_dip_hotness)
    wtdip_pool.sort(key=_wtdip_hotness)
    bounce_pool.sort(key=_bounce_hotness)

    with HOT_PAIRS_LOCK:
        if cand_pool:
            HOT_PAIRS_CACHE["candidates_pool"] = cand_pool
        if cons_pool:
            HOT_PAIRS_CACHE["consolidations_pool"] = cons_pool
        if dip_pool:
            HOT_PAIRS_CACHE["dipbuy_pool"] = dip_pool
        if wtdip_pool:
            HOT_PAIRS_CACHE["wtdip_pool"] = wtdip_pool
        if bounce_pool:
            HOT_PAIRS_CACHE["bounce_pool"] = bounce_pool
        HOT_PAIRS_CACHE["ts"] = time.time()
    logger.debug("HOT: cand=%d cons=%d dip=%d wt=%d bounce=%d",
                 len(cand_pool), len(cons_pool), len(dip_pool),
                 len(wtdip_pool), len(bounce_pool))

def update_ticker_subscription():
    if not WS_TICKERS_ENABLED:
        return
    with HOT_PAIRS_LOCK:
        cand_pool = list(HOT_PAIRS_CACHE.get("candidates_pool") or [])
        cons_pool = list(HOT_PAIRS_CACHE.get("consolidations_pool") or [])
        dip_pool = list(HOT_PAIRS_CACHE.get("dipbuy_pool") or [])
        wtdip_pool = list(HOT_PAIRS_CACHE.get("wtdip_pool") or [])
        bounce_pool = list(HOT_PAIRS_CACHE.get("bounce_pool") or [])

    cand_pool = sorted(cand_pool, key=_cand_hotness)
    cons_pool = sorted(cons_pool, key=_cons_hotness)
    dip_pool = sorted(dip_pool, key=_dip_hotness)
    wtdip_pool = sorted(wtdip_pool, key=_wtdip_hotness)
    bounce_pool = sorted(bounce_pool, key=_bounce_hotness)

    picked = []
    picked_set = set()

    for s in cand_pool:
        if len(picked) >= WS_TICKERS_QUOTA_CAND:
            break
        p = s["pair"]
        if p in picked_set:
            continue
        picked.append(p)
        picked_set.add(p)

    cap_cons = WS_TICKERS_QUOTA_CAND + WS_TICKERS_QUOTA_CONS
    for item in cons_pool:
        if len(picked) >= cap_cons:
            break
        p = item["pair"]
        if p in picked_set:
            continue
        picked.append(p)
        picked_set.add(p)

    cap_dip = cap_cons + WS_TICKERS_QUOTA_DIP
    for s in dip_pool:
        if len(picked) >= cap_dip:
            break
        p = s["pair"]
        if p in picked_set:
            continue
        picked.append(p)
        picked_set.add(p)

    cap_bounce = cap_dip + WS_TICKERS_QUOTA_BOUNCE
    for s in bounce_pool:
        if len(picked) >= cap_bounce:
            break
        p = s["pair"]
        if p in picked_set:
            continue
        picked.append(p)
        picked_set.add(p)

    cap_wt = cap_bounce + WS_TICKERS_QUOTA_WT
    for s in wtdip_pool:
        if len(picked) >= cap_wt:
            break
        p = s["pair"]
        if p in picked_set:
            continue
        picked.append(p)
        picked_set.add(p)

    for pool in (bounce_pool, dip_pool, wtdip_pool, cons_pool, cand_pool):
        if len(picked) >= WS_TICKERS_MAX_PAIRS:
            break
        for item in pool:
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
                for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
                    app.send(json.dumps(
                        {"op": "subscribe",
                         "args": args[i:i + WS_SUBSCRIBE_CHUNK]}))
                    time.sleep(0.1)
            if to_remove:
                args = [f"tickers.{s}" for s in to_remove]
                for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
                    app.send(json.dumps(
                        {"op": "unsubscribe",
                         "args": args[i:i + WS_SUBSCRIBE_CHUNK]}))
                    time.sleep(0.1)
            logger.info("⚡ WS-tickers: +%d -%d (всего %d)",
                        len(to_add), len(to_remove), len(new_subset))
        except Exception as e:
            logger.warning("WS-tickers send error: %s", e)
    with WS_TICKER_PAIRS_LOCK:
        WS_TICKER_PAIRS.clear()
        WS_TICKER_PAIRS.update(new_subset)

def _find_hot_entry(symbol):
    with HOT_PAIRS_LOCK:
        cand_pool = HOT_PAIRS_CACHE.get("candidates_pool") or []
        cons_pool = HOT_PAIRS_CACHE.get("consolidations_pool") or []
        dip_pool = HOT_PAIRS_CACHE.get("dipbuy_pool") or []
        wtdip_pool = HOT_PAIRS_CACHE.get("wtdip_pool") or []
        bounce_pool = HOT_PAIRS_CACHE.get("bounce_pool") or []
    bounce = next((s for s in bounce_pool if s["pair"] == symbol), None)
    if bounce:
        return "bounce", bounce
    cand = next((s for s in cand_pool if s["pair"] == symbol), None)
    if cand:
        return "cand", cand
    con = next((item for item in cons_pool if item["pair"] == symbol), None)
    if con:
        return "cons", con
    dip = next((s for s in dip_pool if s["pair"] == symbol), None)
    if dip:
        return "dip", dip
    wt = next((s for s in wtdip_pool if s["pair"] == symbol), None)
    if wt:
        return "wtdip", wt
    return None, None

def _build_df_with_indicators(buf):
    df = pd.DataFrame(list(buf))
    if len(df) < MIN_BARS:
        return None
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
                          & (df["ema_fast"].shift(1)
                             <= df["ema_slow"].shift(1)))
    df["vol_ratio"] = df["volume"] / df["vol_sma"].replace(0, np.nan)
    try:
        wt1, wt2 = wavetrend(df, WT_N1, WT_N2, WT_SMA_LEN)
        df["wt1"] = wt1
        df["wt2"] = wt2
    except Exception:
        df["wt1"] = np.nan
        df["wt2"] = np.nan
    return df

# ==================== v21.6.0: also_valid в тихом режиме ====================
def _collect_also_valid(df, symbol, lower, upper, primary_kind):
    """
    v21.6.0: прогоняет все стратегии в ТИХОМ режиме — воронка не портится.
    Возвращает список названий стратегий, которые тоже дали бы сигнал.
    """
    _FUNNEL_SILENT.on = True
    try:
        also = []
        if primary_kind != "bounce" and lower and upper:
            s = evaluate_ws_range_bounce(df, symbol, lower, upper)
            if s is not None:
                also.append("bounce")
        if primary_kind != "wtdip":
            s = evaluate_ws_wt_dip(df, symbol)
            if s is not None:
                also.append("wt_dip")
        if primary_kind != "dip":
            s = evaluate_ws_dipbuy(df, symbol)
            if s is not None:
                also.append("rsi_dipbuy")
        return also
    finally:
        _FUNNEL_SILENT.on = False

# ==================== v21.6.0: единая форма алерта ====================
def _format_entry_alert(sig, symbol, also_valid=None):
    strat = sig.get("strategy", "")
    titles = {
        "range_bounce": ("BOUNCE ВХОД", "🎯"),
        "rsi_dipbuy":   ("RSI-DIPBUY ВХОД", "💎"),
        "wt_dip":       ("WT-DIP ВХОД", "🌊"),
        "sqz_dip":      ("SQZ-DIP ВХОД", "💥"),
    }
    title, icon = titles.get(strat, (f"ВХОД ({strat})", "🟢"))
    extra = btc_extra_tag(sig)
    tl = tp_label()
    lines = [
        f"{icon} <b>{title}</b>{extra}",
        f"Пара: {tv_link(symbol)}",
        f"Цена: {sig['entry']:.8f}",
        f"SL: {sig['stop']:.8f} · {tl}: {sig['target']:.8f}",
        f"<i>{' · '.join(sig['parts'])}</i>",
    ]
    if also_valid:
        lines.append(f"<i>Также валидны: {', '.join(also_valid)}</i>")
    return "\n".join(lines)

# ==================== ОТКРЫТИЕ ПО ТИКУ ====================
def _open_on_tick(symbol, cur_price, source="tick"):
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
    df = _build_df_with_indicators(buf)
    if df is None:
        return

    with breakout_cache_lock:
        cache_entry = breakout_cache.get(symbol)
    lower = upper = None
    if isinstance(cache_entry, dict):
        lower = cache_entry.get("lower")
        upper = cache_entry.get("upper")

    # --- 🎯 BOUNCE ---
    if kind == "bounce":
        lower_i = item.get("lower", 0)
        upper_i = item.get("upper", 0)
        if not lower_i or not upper_i:
            return
        sig = evaluate_ws_range_bounce(df, symbol, lower_i, upper_i)
        if sig is None:
            return
        HOT_LAST_CHECK[symbol] = now_ts
        also = _collect_also_valid(df, symbol, lower, upper, "bounce")
        with state_lock:
            opened = open_position(
                symbol, sig, int(df.iloc[-2]["start"]), also_valid=also)
        if opened:
            logger.info("🎯 BOUNCE WS ВХОД %s @ %.6g (lower=%.6g, also=%s)",
                        symbol, sig["entry"], lower_i, also)
            send_telegram(_format_entry_alert(sig, symbol, also))
        return

    # --- 💎 Dip-buy (RSI) ---
    if kind == "dip":
        sig = evaluate_ws_dipbuy(df, symbol)
        if sig is None:
            return
        HOT_LAST_CHECK[symbol] = now_ts
        also = _collect_also_valid(df, symbol, lower, upper, "dip")
        with state_lock:
            opened = open_position(
                symbol, sig, int(df.iloc[-2]["start"]), also_valid=also)
        if opened:
            logger.info("💎 RSI-DIPBUY ВХОД %s @ %.6g (also=%s)",
                        symbol, sig["entry"], also)
            send_telegram(_format_entry_alert(sig, symbol, also))
        return

    # --- 🌊 WT-DIP ---
    if kind == "wtdip":
        sig = evaluate_ws_wt_dip(df, symbol)
        if sig is None:
            return
        HOT_LAST_CHECK[symbol] = now_ts
        also = _collect_also_valid(df, symbol, lower, upper, "wtdip")
        with state_lock:
            opened = open_position(
                symbol, sig, int(df.iloc[-2]["start"]), also_valid=also)
        if opened:
            logger.info("🌊 WT-DIP ВХОД %s @ %.6g (also=%s)",
                        symbol, sig["entry"], also)
            send_telegram(_format_entry_alert(sig, symbol, also))
        return

    # cand / cons / sqz отключены
    return
    # ==================== WEBSOCKET: KLINE ====================
def on_open(ws):
    logger.info("WS kline подключен. Подписка на %d пар (по %d)...",
                len(PAIRS_WS), WS_SUBSCRIBE_CHUNK)
    args = [f"kline.{TIMEFRAME}.{p}" for p in PAIRS_WS]
    sent = 0
    for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
        chunk = args[i:i + WS_SUBSCRIBE_CHUNK]
        try:
            ws.send(json.dumps({"op": "subscribe", "args": chunk}))
            sent += len(chunk)
        except Exception as e:
            logger.warning("WS kline subscribe error: %s", e)
            return
        time.sleep(WS_SUBSCRIBE_PAUSE)
    logger.info("WS kline: отправлено %d подписок чанками по %d",
                sent, WS_SUBSCRIBE_CHUNK)
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
    for i in range(0, len(new), WS_SUBSCRIBE_CHUNK):
        chunk = new[i:i + WS_SUBSCRIBE_CHUNK]
        args = [f"kline.{TIMEFRAME}.{p}" for p in chunk]
        try:
            WS_APP.send(json.dumps({"op": "subscribe", "args": args}))
        except Exception as e:
            logger.warning("WS subscribe error: %s", e)
            return
        time.sleep(0.1)
    for p in new:
        ohlc_buffers[p] = deque(maxlen=200)
        history = fetch_klines(p, TIMEFRAME, 80)
        if history:
            ohlc_buffers[p].extend(history)
            if len(ohlc_buffers[p]) >= 2:
                last_processed_closed[p] = int(
                    list(ohlc_buffers[p])[-2]["start"])
        time.sleep(0.1)
    logger.info("WS-подписка расширена на %d пар", len(new))

# ==================== v21.6.0: единый формат алерта выхода ====================
def _format_exit_alert(symbol, reason, exit_price, net_pnl_pct,
                       account_pnl_pct, size_fraction, suffix="",
                       source="WS"):
    icon = "🟢" if net_pnl_pct > 0 else "🔴"
    return (
        f"{icon} <b>ВЫХОД · {reason.upper()}</b> ({source}){suffix}\n"
        f"Пара: {tv_link(symbol)}\n"
        f"Цена: {exit_price:.8f}\n"
        f"PnL сделки: <b>{net_pnl_pct:+.2f}%</b>\n"
        f"Вклад в счёт: <b>{account_pnl_pct:+.3f}%</b> "
        f"(×{size_fraction:.3f})"
    )

def on_message(ws, message):
    global last_ws_msg_ts
    last_ws_msg_ts = time.time()
    try:
        data = json.loads(message)
        if not isinstance(data, dict):
            return
        topic = data.get("topic", "")
        if not topic.startswith("kline."):
            return
        symbol = topic.split(".")[-1]
        if symbol not in ohlc_buffers:
            return
        raw = data.get("data")
        if isinstance(raw, dict):
            raw = [raw]
        elif not isinstance(raw, list):
            return

        for item in raw:
            if not isinstance(item, dict):
                continue
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

            # ===== Мгновенные выходы по SL =====
            exit_messages = []
            with state_lock:
                pos = state.get(symbol, {})
                if (pos.get("position") == "open"
                        and pos.get("entry_price")):
                    size = pos.get("size_fraction", 1.0)
                    frac = (PARTIAL_TP_FRACTION
                            if pos.get("partial_done") else 1.0)
                    regime = pos.get("daily_regime_at_entry", "unknown")

                    # SL
                    if new_candle["low"] <= pos["stop"]:
                        exit_price = min(new_candle["open"], pos["stop"])
                        net_pnl, acc_pnl = log_trade(
                            symbol, pos["entry_price"], exit_price,
                            "Stop-Loss", pos.get("strategy", "?"),
                            pos.get("entry_time"), size, frac, regime)
                        # v21.6.0: определяем причину по режиму
                        if pos.get("broke_out"):
                            reason_tag = "Stop-Loss (после пробоя)"
                        elif (pos.get("box_upper")
                              and new_candle["close"]
                                  >= pos["box_upper"] * 0.97):
                            reason_tag = "Stop-Loss (у пробоя)"
                        else:
                            reason_tag = "Stop-Loss"
                        exit_messages.append(_format_exit_alert(
                            symbol, reason_tag, exit_price,
                            net_pnl, acc_pnl, size, suffix="",
                            source="WS"))
                        pos.update({
                            "position": "closed",
                            "last_exit_ts": time.time(),
                            "last_exit_price": exit_price,
                            "last_exit_reason": reason_tag.lower(),
                        })
                        state[symbol] = pos
                        save_state(state)

                    # v21.6.0: ложный пробой по моментальному тику
                    # (быстрая реакция — не ждём scan-loop)
                    elif (pos.get("broke_out")
                          and pos.get("box_upper")
                          and new_candle["close"]
                              < pos["box_upper"]
                                  * (1 - VOL_HOLD_FALSE_BREAK_PCT / 100)):
                        exit_price = new_candle["close"]
                        net_pnl, acc_pnl = log_trade(
                            symbol, pos["entry_price"], exit_price,
                            "Ложный пробой", pos.get("strategy", "?"),
                            pos.get("entry_time"), size, frac, regime)
                        exit_messages.append(_format_exit_alert(
                            symbol, "Ложный пробой", exit_price,
                            net_pnl, acc_pnl, size, suffix="",
                            source="WS-тик"))
                        pos.update({
                            "position": "closed",
                            "last_exit_ts": time.time(),
                            "last_exit_price": exit_price,
                            "last_exit_reason": "ложный пробой",
                        })
                        state[symbol] = pos
                        save_state(state)

            for msg in exit_messages:
                send_telegram(msg)

            if len(buf) < MIN_BARS + 1:
                continue
            closed_start = int(buf[-2]["start"])
            if last_processed_closed.get(symbol) == closed_start:
                continue
            last_processed_closed[symbol] = closed_start
            funnel_inc("bars_processed")
            df = _build_df_with_indicators(buf)
            if df is None:
                continue

            with breakout_cache_lock:
                cache_entry = breakout_cache.get(symbol)
            level = None
            lower_level = None
            if isinstance(cache_entry, dict):
                level = cache_entry.get("upper")
                lower_level = cache_entry.get("lower")
            elif isinstance(cache_entry, (int, float)):
                level = cache_entry

            # Приоритеты: bounce → WT → RSI
            sig = evaluate_ws_range_bounce(df, symbol, lower_level, level)
            primary = "bounce" if sig else None
            if sig is None:
                sig = evaluate_ws_wt_dip(df, symbol)
                if sig:
                    primary = "wtdip"
            if sig is None:
                sig = evaluate_ws_dipbuy(df, symbol)
                if sig:
                    primary = "dip"
            if sig is None:
                continue

            also = _collect_also_valid(
                df, symbol, lower_level, level, primary) if primary else []

            with state_lock:
                opened = open_position(
                    symbol, sig, closed_start, also_valid=also)
            if opened is None:
                continue

            logger.info("%s WS ВХОД %s @ %.6g (also=%s)",
                        primary, symbol, sig["entry"], also)
            send_telegram(_format_entry_alert(sig, symbol, also))

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
        WS_APP = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            WS_APP.run_forever(ping_interval=0, ping_timeout=None)
        except Exception as e:
            logger.error("WS критическая ошибка: %s", e)
        time.sleep(5)

def watchdog_loop():
    global last_ws_msg_ts
    while True:
        time.sleep(30)
        silence = time.time() - last_ws_msg_ts
        if silence > WS_SILENCE_TIMEOUT and WS_APP is not None:
            logger.warning("Watchdog kline: тишина %.0f c — перезапуск",
                           silence)
            try:
                WS_APP.close()
            except Exception:
                pass
            last_ws_msg_ts = time.time()

# ==================== WEBSOCKET: TICKERS ====================
def on_tickers_open(ws):
    logger.info("⚡ WS tickers подключен. Подписка на %d пар (по %d)...",
                len(WS_TICKER_PAIRS), WS_SUBSCRIBE_CHUNK)
    WS_TICKERS_CONNECTED.set()
    with WS_TICKER_PAIRS_LOCK:
        pairs = list(WS_TICKER_PAIRS)
    if pairs:
        args = [f"tickers.{s}" for s in pairs]
        for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
            chunk = args[i:i + WS_SUBSCRIBE_CHUNK]
            try:
                ws.send(json.dumps({"op": "subscribe", "args": chunk}))
            except Exception as e:
                logger.warning("WS tickers subscribe error: %s", e)
                break
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
    global last_tickers_msg_ts
    last_tickers_msg_ts = time.time()
    try:
        data = json.loads(message)
        if not isinstance(data, dict):
            return
        if not data.get("topic", "").startswith("tickers."):
            return
        raw = data.get("data")
        if isinstance(raw, dict):
            items = [raw]
        elif isinstance(raw, list):
            items = raw
        else:
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if not symbol or symbol not in WS_TICKER_PAIRS:
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
    logger.warning("WS tickers закрыт (%s). Переподключение...",
                   close_status_code)
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
            WS_TICKERS_APP.run_forever(ping_interval=0, ping_timeout=None)
        except Exception as e:
            logger.error("WS tickers критическая ошибка: %s", e)
        WS_TICKERS_CONNECTED.clear()
        time.sleep(5)

def tickers_refresh_loop():
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

def background_scan_loop():
    global state, breakout_cache
    while True:
        try:
            volatile_pairs = get_filtered_pairs(TOP_N) or []
            all_pairs = get_all_available_pairs(TOTAL_PAIRS) or []
            with state_lock:
                open_pairs = [p for p, v in state.items()
                              if v.get("position") == "open"]
            management_pairs = list(dict.fromkeys(volatile_pairs + open_pairs))
            if not management_pairs:
                logger.error("Нет пар для обработки")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            current_prices = fetch_current_prices(
                list(dict.fromkeys(volatile_pairs + all_pairs + open_pairs)))
            scan_summary = []
            consolidation_list = []
            dipbuy_candidates = []
            wtdip_candidates = []
            bounce_candidates = []
            consolidation_seen = set()
            daily_cache = {}

            # ========== ПЕРВЫЙ ПРОХОД: management_pairs ==========
            for pair in management_pairs:
                if not isinstance(pair, str):
                    continue
                try:
                    df_daily_data = fetch_klines(
                        pair, TIMEFRAME_PARAMS["1d"]["bybit_interval"], 150)
                    if not df_daily_data:
                        continue
                    df_daily = pd.DataFrame(df_daily_data)
                    daily_cache[pair] = df_daily

                    results = {"1d": analyze_timeframe(
                        df_daily, TIMEFRAME_PARAMS["1d"])}
                    for tf in ("4h", "1w"):
                        params = TIMEFRAME_PARAMS[tf]
                        df_tf = pd.DataFrame(fetch_klines(
                            pair, params["bybit_interval"], params["min_bars"]))
                        if not df_tf.empty:
                            results[tf] = analyze_timeframe(df_tf, params)
                    time.sleep(0.3)

                    if not results.get("4h") or not results.get("1d"):
                        continue
                    r4h, r1d = results["4h"], results["1d"]
                    trend_score = sum(
                        1 for k in ("4h", "1d", "1w")
                        if results.get(k)
                        and results[k]["ema_fast"] > results[k]["ema_slow"])
                    macd_gap_pct = ((r4h["macd_line"] - r4h["macd_signal"])
                                    / r4h["close"] * 100
                                    if r4h["close"] else 0.0)
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

                    # --- 💎 dip-кандидаты ---
                    if DIPBUY_ENABLED:
                        buf = ohlc_buffers.get(pair)
                        if buf and len(buf) >= MIN_BARS + 1:
                            df15 = _build_df_with_indicators(buf)
                            if df15 is not None:
                                last15 = df15.iloc[-2]
                                rsi_now = (float(last15["rsi"])
                                           if not pd.isna(last15["rsi"])
                                           else None)
                                if (rsi_now is not None
                                        and WS_HOT_DIP_RSI_MIN
                                            <= rsi_now <= WS_HOT_DIP_RSI_MAX):
                                    regime = daily_regime_bull(pair)
                                    if (regime is True
                                            and last15["ema_fast"]
                                                > last15["ema_slow"]
                                                    * DIPBUY_EMA_SLACK):
                                        dipbuy_candidates.append({
                                            "pair": pair,
                                            "rsi_now": rsi_now,
                                            "current_price": current_price,
                                            "close_price": float(last15["close"]),
                                        })

                    # --- 🌊 WT-dip кандидаты ---
                    if WT_ENABLED and WT_DIP_STRATEGY_ENABLED:
                        buf = ohlc_buffers.get(pair)
                        if buf and len(buf) >= MIN_BARS + 1:
                            df15 = _build_df_with_indicators(buf)
                            if df15 is not None:
                                last15 = df15.iloc[-2]
                                rsi_now = (float(last15["rsi"])
                                           if not pd.isna(last15["rsi"])
                                           else None)
                                regime = daily_regime_bull(pair)
                                if (rsi_now is not None
                                        and WT_HOT_RSI_MIN
                                            <= rsi_now <= WT_HOT_RSI_MAX
                                        and regime is True
                                        and last15["ema_fast"]
                                            > last15["ema_slow"]
                                                * DIPBUY_EMA_SLACK):
                                    try:
                                        wt1s, _ = wavetrend(
                                            df15, WT_N1, WT_N2, WT_SMA_LEN)
                                        wt_now = (float(wt1s.iloc[-2])
                                                  if not pd.isna(wt1s.iloc[-2])
                                                  else 999)
                                        if wt_now <= 0:
                                            wtdip_candidates.append({
                                                "pair": pair,
                                                "wt_now": wt_now,
                                                "rsi_now": rsi_now,
                                                "current_price": current_price,
                                            })
                                    except Exception:
                                        pass

                    # --- боковик + bounce кандидат ---
                    cons = detect_consolidation(df_daily)
                    if cons and pair not in consolidation_seen:
                        consolidation_seen.add(pair)
                        consolidation_list.append({
                            "pair": pair,
                            "current_price": current_price,
                            **cons,
                        })
                        if (RANGE_BOUNCE_ENABLED
                                and cons["lower_level"] > 0
                                and cons["upper_level"]
                                    > cons["lower_level"]):
                            dist_to_bottom = ((current_price
                                               - cons["lower_level"])
                                              / cons["lower_level"] * 100)
                            if 0 <= dist_to_bottom <= RANGE_BOUNCE_HOT_ZONE_PCT:
                                rsi_now = 50
                                buf = ohlc_buffers.get(pair)
                                if buf and len(buf) >= MIN_BARS + 1:
                                    df15b = _build_df_with_indicators(buf)
                                    if df15b is not None:
                                        r15 = df15b.iloc[-2]
                                        if not pd.isna(r15["rsi"]):
                                            rsi_now = float(r15["rsi"])
                                bounce_candidates.append({
                                    "pair": pair,
                                    "lower": cons["lower_level"],
                                    "upper": cons["upper_level"],
                                    "current_price": current_price,
                                    "dist_to_bottom": dist_to_bottom,
                                    "rsi_now": rsi_now,
                                    "days": cons["days"],
                                })

                    # ===== УПРАВЛЕНИЕ ПОЗИЦИЕЙ (v21.6.0) =====
                    messages = []
                    time_stopped = False
                    with state_lock:
                        pos = state.get(pair)
                        if pos and pos.get("position") == "open":
                            try:
                                entry_time = datetime.fromisoformat(
                                    pos.get("entry_time",
                                            "2026-01-01T00:00:00+00:00"))
                            except ValueError:
                                entry_time = datetime.now(timezone.utc)

                            r_unit = _get_r_unit(pos)
                            entry = pos["entry_price"]
                            profit_r = ((r4h["close"] - entry) / r_unit
                                        if r_unit > 0 else 0.0)
                            days_in = (datetime.now(timezone.utc)
                                       - entry_time).days
                            size = pos.get("size_fraction", 1.0)
                            regime = pos.get("daily_regime_at_entry", "unknown")

                            # Time-stop
                            if (days_in >= TIME_STOP_DAYS
                                    and profit_r < TIME_STOP_MIN_R):
                                frac = (PARTIAL_TP_FRACTION
                                        if pos.get("partial_done") else 1.0)
                                net_pnl, acc_pnl = log_trade(
                                    pair, entry, r4h["close"], "Time Stop",
                                    pos.get("strategy", "?"),
                                    pos.get("entry_time"), size, frac, regime)
                                messages.append(
                                    f"⏰ <b>ВЫХОД ПО ВРЕМЕНИ</b>\n"
                                    f"Пара: {tv_link(pair)}\n"
                                    f"Цена: {r4h['close']:.8f}\n"
                                    f"PnL сделки: <b>{net_pnl:+.2f}%</b>\n"
                                    f"Вклад в счёт: <b>{acc_pnl:+.3f}%</b>")
                                pos.update({
                                    "position": "closed",
                                    "last_exit_ts": time.time(),
                                    "last_exit_price": r4h["close"],
                                    "last_exit_reason": "time-stop",
                                })
                                state[pair] = pos
                                save_state(state)
                                time_stopped = True
                            else:
                                exit_now, reason, exit_price = check_exit(
                                    results, pos)
                                if reason == "partial":
                                    net_pnl, acc_pnl = log_trade(
                                        pair, entry, exit_price,
                                        "Частичный TP (50%)",
                                        pos.get("strategy", "?"),
                                        pos.get("entry_time"),
                                        size * PARTIAL_TP_FRACTION,
                                        PARTIAL_TP_FRACTION, regime)
                                    messages.append(
                                        f"💰 <b>ЧАСТИЧНЫЙ TP (50%)</b>\n"
                                        f"Пара: {tv_link(pair)}\n"
                                        f"Зафиксировано: <b>{net_pnl:+.2f}%</b>\n"
                                        f"Вклад в счёт: <b>{acc_pnl:+.3f}%</b>")
                                if exit_now:
                                    # v21.6.0: счётчик удержаний
                                    if reason == "Ложный пробой":
                                        funnel_inc("hold_false")
                                    elif reason == "Слабый объём у пробоя":
                                        funnel_inc("hold_weak")

                                    frac = (PARTIAL_TP_FRACTION
                                            if pos.get("partial_done") else 1.0)
                                    net_pnl, acc_pnl = log_trade(
                                        pair, entry, exit_price, reason,
                                        pos.get("strategy", "?"),
                                        pos.get("entry_time"),
                                        size, frac, regime)
                                    icon = "🟢" if net_pnl > 0 else "🔴"
                                    suffix = (" (оставшиеся 50%)"
                                              if frac != 1.0 else "")
                                    messages.append(
                                        f"{icon} <b>{reason.upper()}</b>"
                                        f"{suffix}\n"
                                        f"Пара: {tv_link(pair)}\n"
                                        f"Цена: {exit_price:.8f}\n"
                                        f"PnL сделки: <b>{net_pnl:+.2f}%</b>\n"
                                        f"Вклад в счёт: <b>{acc_pnl:+.3f}%</b>")
                                    pos.update({
                                        "position": "closed",
                                        "last_exit_ts": time.time(),
                                        "last_exit_price": exit_price,
                                        "last_exit_reason": reason,
                                    })
                                # v21.6.0: если держим у пробоя — считаем
                                elif (pos.get("near_upper_bars", 0) > 0
                                      and pos.get("last_vol_score", 0)
                                          >= VOL_HOLD_SCORE_KEEP):
                                    funnel_inc("hold_kept")
                                state[pair] = pos
                                save_state(state)
                    for msg in messages:
                        send_telegram(msg)
                    if time_stopped:
                        continue

                except Exception as e:
                    logger.error("Ошибка в %s: %s", pair, e)
                    continue

            # ========== ВТОРОЙ ПРОХОД: all_pairs ==========
            for pair in all_pairs:
                if not isinstance(pair, str):
                    continue
                try:
                    df_daily = daily_cache.get(pair)
                    if df_daily is None:
                        df_daily_data = fetch_klines(
                            pair,
                            TIMEFRAME_PARAMS["1d"]["bybit_interval"], 150)
                        if not df_daily_data:
                            continue
                        df_daily = pd.DataFrame(df_daily_data)
                        time.sleep(0.15)
                    current_price = current_prices.get(
                        pair, df_daily["close"].iloc[-1])
                    cons = detect_consolidation(df_daily)
                    if cons and pair not in consolidation_seen:
                        consolidation_seen.add(pair)
                        consolidation_list.append({
                            "pair": pair,
                            "current_price": current_price,
                            **cons,
                        })
                        if (RANGE_BOUNCE_ENABLED
                                and cons["lower_level"] > 0
                                and cons["upper_level"]
                                    > cons["lower_level"]):
                            dist_to_bottom = ((current_price
                                               - cons["lower_level"])
                                              / cons["lower_level"] * 100)
                            if 0 <= dist_to_bottom <= RANGE_BOUNCE_HOT_ZONE_PCT:
                                bounce_candidates.append({
                                    "pair": pair,
                                    "lower": cons["lower_level"],
                                    "upper": cons["upper_level"],
                                    "current_price": current_price,
                                    "dist_to_bottom": dist_to_bottom,
                                    "rsi_now": 50,
                                    "days": cons["days"],
                                })
                except Exception as e:
                    logger.error("Ошибка во втором проходе %s: %s", pair, e)
                    continue

            # ========== Обновление breakout_cache (v21.6.0 fix A) ==========
            with breakout_cache_lock:
                consolidation_list.sort(
                    key=lambda x: (-x["days"], x.get("vol_trend", 1.0)))
                breakout_cache = {}
                for item in consolidation_list[:BREAKOUT_CACHE_SIZE]:
                    breakout_cache[item["pair"]] = {
                        "upper": item["upper_level"],
                        "lower": item["lower_level"],
                        "days": item["days"],
                        # v21.6.0 fix A: vol_trend сохраняется!
                        "vol_trend": float(item.get("vol_trend", 1.0) or 1.0),
                    }
            logger.info("Breakout-кэш: %d уровней (upper+lower+vol_trend)",
                        len(breakout_cache))

            with breakout_cache_lock:
                extra = [p for p in breakout_cache.keys()
                         if p not in PAIRS_WS]
            extend_ws_subscription(extra)

            # ========== Чистка state ==========
            try:
                with state_lock:
                    if len(state) > 500:
                        now_ts = time.time()
                        for p in list(state.keys()):
                            pos = state[p]
                            if pos.get("position") == "closed":
                                last_exit_ts = float(
                                    pos.get("last_exit_ts", 0) or 0)
                                if (last_exit_ts > 0
                                        and now_ts - last_exit_ts
                                            > CLEANUP_AFTER_DAYS * 86400):
                                    del state[p]
                        save_state(state)
            except Exception as e:
                logger.error("Ошибка очистки state: %s", e)

            # ========== Статус ==========
            session_stats = get_session_stats()
            send_status(scan_summary, consolidation_list, dipbuy_candidates,
                        wtdip_candidates, bounce_candidates,
                        session_stats["entries"], session_stats["exits"])
            try:
                update_ticker_subscription()
            except Exception as e:
                logger.error("update_ticker_subscription (status): %s", e)
            time.sleep(SCAN_INTERVAL_SECONDS)
        except Exception as e:
            logger.critical("Критическая ошибка в фоне: %s", e)
            time.sleep(SCAN_INTERVAL_SECONDS)
            # ==================== 🔍 ВОРОНКА ====================
def _format_funnel(fs):
    lines = []
    lines.append(f"🔍 <b>ВОРОНКА</b> (баров: {fs.get('bars_processed', 0)})")
    lines.append(
        f"🎯 BOUNCE: {fs['bounce_total']}→зона {fs['bounce_zone']}"
        f"→RSI {fs['bounce_rsi']}→зел {fs['bounce_green']}"
        f"→объём {fs['bounce_vol']}→R:R {fs['bounce_rr']}")
    lines.append(
        f"💎 RSI-dip: {fs['dip_total']}→bull {fs['dip_bull']}"
        f"→зона {fs['dip_zone']}→растёт {fs['dip_rising']}"
        f"→зел {fs['dip_green']}→BB {fs['dip_bb']}"
        f"→объём {fs['dip_vol']}→EMA {fs['dip_ema']}")
    lines.append(
        f"🌊 WT-dip: {fs['wt_total']}→bull {fs['wt_bull']}"
        f"→RSI {fs['wt_rsi']}→кросс {fs['wt_cross']}"
        f"→EMA {fs['wt_ema']}→зел {fs['wt_green']}"
        f"→BOTTOM {fs['wt_bottom']}")
    lines.append(
        f"💥 SQZ-dip: OFF (отключён)")
    lines.append(
        f"🚀 HOLD: ✅ kept {fs['hold_kept']} · "
        f"⚠️ weak {fs['hold_weak']} · "
        f"❌ false {fs['hold_false']}")
    lines.append(
        f"⛔ PEAK-GUARD: {fs['peak_block']} · "
        f"🔧 BTC-блок: {fs['btc_block']} · "
        f"✅ Открыто: {fs['opened']}")
    return "\n".join(lines)

# ==================== СТАТУС (v21.6.0) ====================
def send_status(scan_summary, consolidation_list, dipbuy_candidates,
                wtdip_candidates, bounce_candidates, found_buy, found_sell):
    with state_lock:
        open_snapshot = [(pair, dict(pos)) for pair, pos in state.items()
                         if pos.get("position") == "open"]
    with trend_cache_lock:
        q3 = sum(1 for v in trend_cache.values() if v["score"] == 3)
        bull_1d = sum(1 for v in trend_cache.values()
                      if v.get("d_ema50_gt_200")
                      and v.get("d_close_above_200"))
    with market_context_lock:
        mok, mreason = market_context["ok"], market_context["reason"]
    with subscribers_lock:
        subs_count = len(SUBSCRIBERS)
    with WS_TICKER_PAIRS_LOCK:
        tickers_count = len(WS_TICKER_PAIRS)
    with cb_lock:
        cb_consec = cb_state["consec"]
        cb_paused_until = cb_state["paused_until"]
        cb_day_acc = cb_state["day_account_pnl_pct"]
        cb_day_trades = cb_state["day_trades"]

    funnel_snap = funnel_snapshot()
    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")

    btc_state_str = "OK" if mok else f"БЛОК: {mreason}"
    if not mok and BTC_ALLOW_STRONG_WHEN_BLOCKED:
        btc_state_str += " · dip/bounce обходят"

    cb_str = "OK"
    if cb_paused_until > time.time():
        rem_min = int((cb_paused_until - time.time()) / 60)
        cb_str = f"⏸ пауза {rem_min}м"
    elif cb_consec >= 2:
        cb_str = f"⚠️ {cb_consec} убытков"
    if cb_day_trades:
        cb_str += f" · сегодня {cb_day_acc:+.2f}%"

    header = [
        f"📡 <b>СТАТУС v21.6.0 «VOLUME-AWARE-HOLD»</b> | <i>{now_str} UTC</i>",
        "━━━━━━━━━━━━━━━━━━━━━",
        f"🔹 Пар WS kline: <b>{len(PAIRS_WS)}</b> · Тренд 3/3: <b>{q3}</b>",
        f"🔹 1D-bull (EMA50&gt;200): <b>{bull_1d}</b> пар",
        f"🔹 ⚡ WS tickers: <b>{tickers_count}</b>/{WS_TICKERS_MAX_PAIRS}",
        f"🔹 BTC: <b>{btc_state_str}</b>",
        f"🔹 CB: <b>{cb_str}</b>",
        f"🔹 Боковиков: <b>{len(consolidation_list)}</b> · "
        f"RSI-dip: <b>{len(dipbuy_candidates)}</b> · "
        f"WT-dip: <b>{len(wtdip_candidates)}</b> · "
        f"Bounce: <b>{len(bounce_candidates)}</b>",
        f"🔹 Позиций: <b>{len(open_snapshot)}/{MAX_OPEN_POSITIONS}</b> · "
        f"Входов: <b>{found_buy}</b> · Выходов: <b>{found_sell}</b> · "
        f"👥 {subs_count}",
        "━━━━━━━━━━━━━━━━━━━━━",
    ]
    header += _format_funnel(funnel_snap).split("\n")
    header.append("━━━━━━━━━━━━━━━━━━━━━")

    # ===== Позиции (v21.6.0: с режимом) =====
    pos_lines = []
    if open_snapshot:
        for pair, pos in open_snapshot:
            et = str(pos.get("entry_time", "?")).replace("T", " ")[:16]
            strat = pos.get("strategy", "")
            tag_map = {
                "range_bounce": "🎯",
                "rsi_dipbuy":   "💎",
                "wt_dip":       "🌊",
                "sqz_dip":      "💥",
            }
            tag = tag_map.get(strat, "🟢")
            entry = pos.get("entry_price", 0)
            stop = pos.get("stop", 0)
            target = pos.get("target", 0)
            size = pos.get("size_fraction", 1.0)
            r_unit = pos.get("r_unit_price", 0)
            line = (
                f"{tag} <b>{pair}</b> {entry:.8f} → "
                f"🛑{stop:.8f} 🎯{target:.8f} · {et} · "
                f"size={size:.3f}")
            if r_unit > 0:
                line += f" · R={r_unit:.6g}"
            if pos.get("breakeven_moved"):
                line += " · 🔒BE"
            if pos.get("btc_blocked_entry"):
                line += " · ⚠️BTC×0.5"
            regime = pos.get("daily_regime_at_entry", "")
            if regime == "bull":
                line += " · 🐂"
            elif regime == "bear":
                line += " · 🐻"
            also = pos.get("also_valid") or []
            if also:
                line += f" · also:{','.join(also)}"

            # v21.6.0: показываем режим у пробоя / после пробоя
            box_upper = pos.get("box_upper")
            if box_upper and entry > 0:
                cur = entry
                buf = ohlc_buffers.get(pair)
                if buf and len(buf) > 0:
                    cur = float(buf[-1].get("close", entry))
                if pos.get("broke_out"):
                    line += " · 🚀 после пробоя"
                elif cur >= box_upper * 0.97:
                    vs = pos.get("last_vol_score", 0.0)
                    nb = pos.get("near_upper_bars", 0)
                    keep = ("держим" if vs >= VOL_HOLD_SCORE_KEEP
                            else "наблюдаем")
                    line += (f" · ⚡ у пробоя ({keep}, "
                             f"vol={vs:.2f}, {nb}б)")
            pos_lines.append(line)
    else:
        pos_lines.append("💰 Позиций нет")

    # ===== Кандидаты (для отображения) =====
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
        dist = (lvl - cur) / cur * 100
        # v21.6.0: пробитые — в конец списка
        if dist < 0:
            return 10_000 + abs(dist)
        return dist

    consolidation_list = sorted(consolidation_list, key=_hot_score)
    dipbuy_candidates = sorted(dipbuy_candidates,
                               key=lambda x: x.get("rsi_now", 999))
    wtdip_candidates = sorted(wtdip_candidates,
                              key=lambda x: x.get("wt_now", 999))
    bounce_candidates = sorted(bounce_candidates,
                               key=lambda x: x.get("dist_to_bottom", 999))

    display_calls = valid_calls[:MAX_SHOW_CANDIDATES]
    display_cons = consolidation_list[:MAX_SHOW_CONSOLIDATIONS]
    display_dip = dipbuy_candidates[:MAX_SHOW_DIPBUY]
    display_wtdip = wtdip_candidates[:MAX_SHOW_WTDIP]
    display_bounce = bounce_candidates[:MAX_SHOW_BOUNCE]

    # ===== Обновление HOT-пулов для WS-подписок =====
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
        HOT_PAIRS_CACHE["dipbuy"] = list(display_dip)
        HOT_PAIRS_CACHE["wtdip"] = list(display_wtdip)
        HOT_PAIRS_CACHE["bounce"] = list(display_bounce)
        HOT_PAIRS_CACHE["candidates_pool"] = list(cand_pool)
        HOT_PAIRS_CACHE["consolidations_pool"] = list(cons_pool)
        HOT_PAIRS_CACHE["ts"] = time.time()

    # ===== Форматтеры строк =====
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
        peak_ok = True
        buf = ohlc_buffers.get(s["pair"])
        if buf and len(buf) >= PEAK_LOOKBACK_CANDLES:
            df_pk = pd.DataFrame(list(buf))
            peak_ok, _, _ = check_peak_guard(df_pk, cur)
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
        # v21.6.0: пробитые отдельно
        if dist_pct < 0:
            mark = "🚀"
        elif dist_pct <= HOT_DIST_PCT_1:
            mark = "🔥"
        elif dist_pct <= HOT_DIST_PCT_2:
            mark = "⚡"
        elif dist_pct <= HOT_DIST_PCT_3:
            mark = "🟢"
        else:
            mark = ""
        dist_txt = f"({dist_pct:.1f}%)" if dist_pct < 999 else ""
        vt = item.get("vol_trend", 1.0)
        vt_txt = f" 📊{vt:.2f}×" if vt >= 1.1 else ""
        return (f"{mark}{i}.{name} {item['days']}д "
                f"{item['range_pct']:.0f}% ADX{item['adx']:.0f}{dry} "
                f"🚀{lvl:.6g} 💰{cur:.6g}{dist_txt}{vt_txt}")

    def dip_line(i, s, with_link):
        name = tv_link(s["pair"]) if with_link else s["pair"]
        cur = s.get("current_price", 0)
        rsi_now = s.get("rsi_now", 0)
        return f"💎{i}.{name} RSI{rsi_now:.0f} 💰{cur:.6g} (ждём отскок)"

    def wtdip_line(i, s, with_link):
        name = tv_link(s["pair"]) if with_link else s["pair"]
        cur = s.get("current_price", 0)
        wt_now = s.get("wt_now", 0)
        rsi_now = s.get("rsi_now", 0)
        return f"🌊{i}.{name} WT{wt_now:.0f} RSI{rsi_now:.0f} 💰{cur:.6g}"

    def bounce_line(i, s, with_link):
        name = tv_link(s["pair"]) if with_link else s["pair"]
        cur = s.get("current_price", 0)
        lower = s.get("lower", 0)
        upper = s.get("upper", 0)
        rsi_now = s.get("rsi_now", 0)
        dist = s.get("dist_to_bottom", 0)
        days = s.get("days", 0)
        return (f"🎯{i}.{name} {days}д lower={lower:.6g} "
                f"upper={upper:.6g} 💰{cur:.6g} (+{dist:.1f}%) "
                f"RSI{rsi_now:.0f}")

    # ===== Footer (v21.6.0) =====
    footer = [
        "━━━━━━━━━━━━━━━━━━━━━",
        f"🔄 Следующий статус через 2 ч · лимиты {MAX_OPEN_POSITIONS} поз / "
        f"{MAX_TRADES_PER_HOUR} в час",
        f"🔒 HOLD: BE +{BREAKEVEN_TRIGGER_ATR_FAST}×R, "
        f"без partial TP, без fixed TP",
        f"🚀 VOL-HOLD: у пробоя ≥{VOL_HOLD_SCORE_KEEP} держим, "
        f"<{VOL_HOLD_SCORE_WEAK}+🔴 выходим",
        f"🚀 после пробоя: трейлинг {VOL_HOLD_AFTER_BREAK_TRAIL}×R, "
        f"ложный −{VOL_HOLD_FALSE_BREAK_PCT}%",
        f"🚪 Выход: EMA-кросс 4h / трейлинг / SL / "
        f"time-stop {TIME_STOP_DAYS}д",
        f"🎯 Все стратегии ищут ДНО. Пробои удерживаем по объёму.",
        f"🔧 BTC: -{abs(BTC_DROP_6H_PCT):.0f}%/6ч или "
        f"ADX&gt;{BTC_ADX_BLOCK_THRESHOLD:.0f} · "
        f"🛡 Peak Guard · 🚫 SQZ-dip off",
        f"💎 RSI-dip · 🌊 WT-dip · 🎯 BOUNCE · "
        f"/funnel · /stats · /reset",
    ]

    def build(with_links, max_cand, max_cons, max_dip, max_wtdip, max_bounce):
        lines = list(header)
        lines += pos_lines
        lines.append("")
        lines.append("🎯🎯🎯 <b>BOUNCE ОТ ДНА БОКОВИКА</b> 🎯🎯🎯")
        lines.append("<blockquote expandable>")
        if display_bounce:
            for i, s in enumerate(display_bounce[:max_bounce], 1):
                lines.append(bounce_line(i, s, with_links))
        else:
            lines.append("— bounce-кандидатов нет")
        lines.append("</blockquote>")
        lines.append("💎💎💎 <b>RSI-DIPBUY (ОТСКОК ОТ ДНА)</b> 💎💎💎")
        lines.append("<blockquote expandable>")
        if display_dip:
            for i, s in enumerate(display_dip[:max_dip], 1):
                lines.append(dip_line(i, s, with_links))
        else:
            lines.append("— RSI dip-кандидатов нет")
        lines.append("</blockquote>")
        lines.append("🌊🌊🌊 <b>WT-DIP (WAVETREND ВНИЗУ)</b> 🌊🌊🌊")
        lines.append("<blockquote expandable>")
        if display_wtdip:
            for i, s in enumerate(display_wtdip[:max_wtdip], 1):
                lines.append(wtdip_line(i, s, with_links))
        else:
            lines.append("— WT dip-кандидатов нет")
        lines.append("</blockquote>")
        lines.append("🟡🟡🟡 <b>МОНЕТЫ В БОКОВИКЕ (30–60 ДНЕЙ)</b> 🟡🟡🟡")
        lines.append("<blockquote expandable>")
        if display_cons:
            for i, item in enumerate(display_cons[:max_cons], 1):
                lines.append(cons_line(i, item, with_links))
        else:
            lines.append("📦 боковиков нет")
        lines.append("</blockquote>")
        lines.append("🔵🔵🔵 <b>ТОП КАНДИДАТОВ (INFO)</b> 🔵🔵🔵")
        lines.append("<blockquote expandable>")
        if display_calls:
            for i, s in enumerate(display_calls[:max_cand], 1):
                lines.append(cand_line(i, s, with_links))
        else:
            lines.append("кандидатов нет")
        lines.append("</blockquote>")
        lines += footer
        return "\n".join(lines)

    text = build(True, len(display_calls), len(display_cons),
                 len(display_dip), len(display_wtdip), len(display_bounce))

    # Автосжатие если не влезает
    if len(text) > TG_SAFE_LIMIT:
        for mc in range(len(display_calls), 2, -1):
            for mcons in range(len(display_cons), 2, -1):
                for md in range(len(display_dip), 0, -1):
                    for mw in range(len(display_wtdip), 0, -1):
                        for mb in range(len(display_bounce), 0, -1):
                            text = build(True, mc, mcons, md, mw, mb)
                            if len(text) <= TG_SAFE_LIMIT:
                                break
                        if len(text) <= TG_SAFE_LIMIT:
                            break
                    if len(text) <= TG_SAFE_LIMIT:
                        break
                if len(text) <= TG_SAFE_LIMIT:
                    break
            if len(text) <= TG_SAFE_LIMIT:
                break
    if len(text) > TG_SAFE_LIMIT:
        text = build(True, 5, 5, 3, 3, 3)

    _send_to_all_one(text)
    logger.info("Статус v21.6.0: %d симв · bounce=%d dip=%d wt=%d "
                "cons=%d · tickers=%d · hold_kept=%d hold_weak=%d "
                "hold_false=%d",
                len(text), len(bounce_candidates),
                len(dipbuy_candidates), len(wtdip_candidates),
                len(consolidation_list), tickers_count,
                funnel_snap.get("hold_kept", 0),
                funnel_snap.get("hold_weak", 0),
                funnel_snap.get("hold_false", 0))
    funnel_reset()

# ==================== MAIN ====================
def handle_stop(signum, _frame):
    logger.info("Получен сигнал %s — сохраняю state", signum)
    with state_lock:
        save_state(state)
    raise SystemExit(0)

if __name__ == "__main__":
    logger.info("Запуск бота v21.6.0 «VOLUME-AWARE-HOLD» (Bybit) ...")
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
                    pos["atr_ref"] = ((pos["entry_price"] - pos["stop"])
                                      / ATR_MULT_SL)
                else:
                    pos["atr_ref"] = pos.get("entry_price", 0) * 0.01

            # Миграция r_unit_price
            if "r_unit_price" not in pos or pos["r_unit_price"] <= 0:
                entry = pos.get("entry_price", 0)
                stop = pos.get("stop", 0)
                if entry > 0 and stop > 0 and stop < entry:
                    pos["r_unit_price"] = entry - stop
                else:
                    pos["r_unit_price"] = pos.get("atr_ref", entry * 0.01)

            pos.setdefault("partial_done", False)
            pos.setdefault("breakeven_moved", False)
            pos.setdefault("highest_close", pos.get("entry_price", 0))
            pos.setdefault("size_fraction", 1.0)
            pos.setdefault("btc_blocked_entry", False)
            pos.setdefault("daily_regime_at_entry", "unknown")
            pos.setdefault("hold_mode", True)
            pos.setdefault("confidence", 1.0)
            pos.setdefault("also_valid", [])
            # v21.6.0: миграция 3-режимной логики
            pos.setdefault("box_upper", None)
            pos.setdefault("box_vol_trend", 1.0)
            pos.setdefault("near_upper_bars", 0)
            pos.setdefault("last_vol_score", 0.0)
            pos.setdefault("broke_out", False)
    save_state(state)

    refresh_market_context()

    retry_count = 0
    while not PAIRS_WS and retry_count < 3:
        PAIRS_WS = get_filtered_pairs(TOP_N)
        if not PAIRS_WS:
            logger.warning("Попытка %d: нет пар, повтор...", retry_count + 1)
            time.sleep(10)
        retry_count += 1
    if not PAIRS_WS:
        logger.warning("Fallback: все доступные пары")
        PAIRS_WS = get_all_available_pairs(TOP_N)
    if not PAIRS_WS:
        logger.critical("Не удалось получить пары!")
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
        bull_1d = sum(1 for v in trend_cache.values()
                      if v.get("d_ema50_gt_200")
                      and v.get("d_close_above_200"))
    logger.info("Прогрев завершён: 3/3=%d, 1D-bull=%d", q3, bull_1d)

    for pair in PAIRS_WS:
        if not isinstance(pair, str):
            continue
        history = fetch_klines(pair, TIMEFRAME, 80)
        if history:
            ohlc_buffers[pair] = deque(history, maxlen=200)
            if len(ohlc_buffers[pair]) >= 2:
                last_processed_closed[pair] = int(
                    list(ohlc_buffers[pair])[-2]["start"])
        time.sleep(0.1)

    threading.Thread(target=run_websocket, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()
    threading.Thread(target=trend_cache_loop, daemon=True).start()
    threading.Thread(target=market_context_loop, daemon=True).start()
    threading.Thread(target=polling_loop, daemon=True).start()
    threading.Thread(target=run_tickers_websocket, daemon=True).start()
    threading.Thread(target=tickers_refresh_loop, daemon=True).start()

    _send_to_all_one(
        f"🟢 <b>СКАНЕР v21.6.0 «VOLUME-AWARE-HOLD» ЗАПУЩЕН</b>\n"
        f"📡 WS kline: {len(PAIRS_WS)} пар "
        f"(подписка чанками по {WS_SUBSCRIBE_CHUNK})\n"
        f"⚡ WS tickers: {WS_TICKERS_QUOTA_CAND} cand + "
        f"{WS_TICKERS_QUOTA_CONS} cons + "
        f"{WS_TICKERS_QUOTA_DIP} dip + {WS_TICKERS_QUOTA_WT} wt + "
        f"{WS_TICKERS_QUOTA_BOUNCE} bounce "
        f"(макс {WS_TICKERS_MAX_PAIRS})\n"
        f"🎯 BOUNCE: отскок от дна (≤{RANGE_BOUNCE_ZONE_PCT}%) + "
        f"оценка накопления (vol_trend)\n"
        f"💎 RSI-DIPBUY: RSI≤{DIPBUY_RSI_ZONE} × "
        f"{DIPBUY_RSI_MIN_BARS}св + объём (без BB)\n"
        f"🌊 WT-DIP: WaveTrend кросс внизу (без EMA)\n"
        f"🔒 HOLD: BE +{BREAKEVEN_TRIGGER_ATR_FAST}×R, "
        f"без partial TP\n"
        f"🚀 VOL-HOLD (3 режима):\n"
        f"   • у пробоя (≥{VOL_HOLD_NEAR_UPPER_PCT}% от upper): "
        f"vol_score ≥{VOL_HOLD_SCORE_KEEP} держим\n"
        f"   • vol_score <{VOL_HOLD_SCORE_WEAK} + красная 4h → выход\n"
        f"   • после пробоя: трейлинг "
        f"{VOL_HOLD_AFTER_BREAK_TRAIL}×R, "
        f"ложный −{VOL_HOLD_FALSE_BREAK_PCT}%\n"
        f"🚪 Стандартный выход: EMA-кросс 4h / трейлинг / SL / "
        f"time-stop {TIME_STOP_DAYS}д\n"
        f"🛡 Peak Guard — реальный фильтр\n"
        f"📊 Risk: {RISK_PER_TRADE_PCT}%/сделку от ${ACCOUNT_SIZE_USD:.0f} · "
        f"портфель ≤{MAX_PORTFOLIO_RISK_PCT}%\n"
        f"🔧 BTC-фильтр: -{abs(BTC_DROP_6H_PCT):.0f}%/6ч или "
        f"ADX&gt;{BTC_ADX_BLOCK_THRESHOLD:.0f}\n"
        f"🚫 SQZ-dip отключён\n"
        f"Тренд 3/3: {q3} · 1D-bull: {bull_1d} · "
        f"BTC: {'OK' if market_allows_longs() else 'БЛОК'}\n"
        f"👥 Подписчиков: {len(SUBSCRIBERS)} · "
        f"/start · /stop · /help · /funnel · /stats · /reset")
    background_scan_loop()
