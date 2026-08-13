#!/usr/bin/env python3
"""
Сканер сигналов Kraken Spot с мультитаймфреймовым подтверждением (confluence)
Версия 3.0 – НЕПРЕРЫВНЫЙ РЕЖИМ с интервалом сканирования 2 часа

ЛОГИКА ВХОДА:
    – восходящий тренд (EMA fast > EMA slow) на 4h, 1d и 1w (bias)
    – точный триггер входа на 15m: свежее пересечение MACD вверх
    – RSI(14) на 15m в диапазоне [RSI_MIN, RSI_MAX]
    – ADX(14) на 1d >= ADX_MIN
    – (опционально) Smart Money BOS на 15m

ВЫХОД:
    – по 4h: разворот EMA/MACD или срабатывание Stop-Loss/Take-Profit

РЕЖИМЫ ЗАПУСКА:
    Без аргументов              – непрерывное сканирование (по умолчанию)
    --mode scan                 – однократное сканирование
    --mode report --period 3d   – отчёт за 3 дня
    --mode report --period month – отчёт за месяц

СТРАТЕГИИ (только для режима scan):
    --strategy aggressive   – больше сигналов
    --strategy balanced     – сбалансирован (по умолчанию)
    --strategy conservative – высокая точность

УСТАНОВКА:
    pip install requests pandas numpy
"""

import argparse
import json
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple

import requests
import pandas as pd
import numpy as np

# ==================== КОНФИГУРАЦИЯ ====================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SUBSCRIBERS_FILE = "telegram_subscribers.json"
STATE_FILE = "kraken_scanner_state.json"
TRADES_LOG_FILE = "trades_log.json"
SCANNER_LOG_FILE = "kraken_scanner.log"
LAST_STATUS_FILE = "last_status_time.json"

WELCOME_TEXT = (
    "✅ Вы подписались на сигналы Kraken Scanner v3.0 (непрерывный режим)\n"
    "Сканирование выполняется каждые 2 часа, статус – не чаще 2 часов."
)

BASE_URL = "https://api.kraken.com/0/public"

# ---------- Настройка логирования ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(SCANNER_LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- Параметры таймфреймов ----------
TIMEFRAME_PARAMS = {
    "15m": {"kraken_interval": 15,   "ema_fast": 9,  "ema_slow": 21, "min_bars": 80},
    "1h":  {"kraken_interval": 60,   "ema_fast": 12, "ema_slow": 26, "min_bars": 100},
    "4h":  {"kraken_interval": 240,  "ema_fast": 21, "ema_slow": 55, "min_bars": 120},
    "1d":  {"kraken_interval": 1440, "ema_fast": 50, "ema_slow": 100, "min_bars": 150},
    "1w":  {"kraken_interval": 10080, "ema_fast": 8, "ema_slow": 20, "min_bars": 40},
}

TIMEFRAME_ORDER = ["4h", "1d", "1w"]
ENTRY_TRIGGER_TFS = ["15m"]          # можно добавить "1h" для большего числа сигналов
TRIGGER_TF = "4h"
ADX_REF_TF = "1d"

RSI_LENGTH = 14
ADX_LENGTH = 14
BREAKEVEN_BUFFER_PCT = 0.1

EXCLUDE_BASE_SUBSTRINGS = ["UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S"]
STABLECOINS = {"USDC", "USDT", "DAI", "USD", "EUR", "GBP", "PYUSD", "TUSD", "FDUSD"}

# ---------- Настройки стратегий ----------
STRATEGY_CONFIGS = {
    "aggressive": {
        "name": "Агрессивная",
        "rsi_range": [30, 80],
        "adx_min": 15,
        "require_smc": False,
        "require_volume": False,
        "require_divergence": False,
        "require_fibonacci": False,
        "require_trend_all": False,
        "min_rr_ratio": 1.5,
        "top_n": 200,                # больше пар для сканирования
        "min_volatility": 1.5,
        "min_turnover": 100000,
        "atr_mult_sl": 1.5,
        "atr_mult_tp": 3.0,
        "breakeven_trigger_atr": 0.8,
        "trailing_atr_mult": 1.2,
        "description": "Максимальное количество сигналов"
    },
    "balanced": {
        "name": "Сбалансированная",
        "rsi_range": [40, 75],
        "adx_min": 20,
        "require_smc": True,
        "require_volume": True,
        "require_divergence": False,
        "require_fibonacci": False,
        "require_trend_all": True,
        "min_rr_ratio": 2.0,
        "top_n": 200,                # увеличено до 200
        "min_volatility": 1.5,       # снижен порог для охвата большего числа пар
        "min_turnover": 100000,
        "atr_mult_sl": 2.0,
        "atr_mult_tp": 4.0,
        "breakeven_trigger_atr": 1.0,
        "trailing_atr_mult": 1.5,
        "description": "Оптимальный баланс сигналов и точности"
    },
    "conservative": {
        "name": "Консервативная",
        "rsi_range": [45, 70],
        "adx_min": 25,
        "require_smc": True,
        "require_volume": True,
        "require_divergence": True,
        "require_fibonacci": True,
        "require_trend_all": True,
        "min_rr_ratio": 3.0,
        "top_n": 150,
        "min_volatility": 2.0,
        "min_turnover": 200000,
        "atr_mult_sl": 2.5,
        "atr_mult_tp": 5.0,
        "breakeven_trigger_atr": 1.2,
        "trailing_atr_mult": 1.8,
        "description": "Максимальная точность"
    }
}

# ---------- Глобальные переменные (переопределяются стратегией) ----------
ATR_MULT_SL = 2.0
ATR_MULT_TP = 4.0
BREAKEVEN_TRIGGER_ATR = 1.0
TRAILING_ATR_MULT = 1.5
MIN_VOLATILITY_PCT = 3.0
MIN_TURNOVER_USD = 300000
RSI_MIN, RSI_MAX = 40, 75
ADX_MIN = 20
REQUIRE_SMC_BOS = True

# ---------- Интервал статусных уведомлений (в минутах) ----------
STATUS_INTERVAL_MINUTES = 120   # 2 часа

# ---------- Интервал между циклами сканирования (в секундах) ----------
SCAN_INTERVAL_SECONDS = 7200    # 2 часа

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def get_strategy_config(strategy_name: str) -> Dict[str, Any]:
    config = STRATEGY_CONFIGS.get(strategy_name, STRATEGY_CONFIGS["balanced"]).copy()
    global ATR_MULT_SL, ATR_MULT_TP, BREAKEVEN_TRIGGER_ATR, TRAILING_ATR_MULT
    global MIN_VOLATILITY_PCT, MIN_TURNOVER_USD, RSI_MIN, RSI_MAX, ADX_MIN, REQUIRE_SMC_BOS

    ATR_MULT_SL = config["atr_mult_sl"]
    ATR_MULT_TP = config["atr_mult_tp"]
    BREAKEVEN_TRIGGER_ATR = config["breakeven_trigger_atr"]
    TRAILING_ATR_MULT = config["trailing_atr_mult"]
    MIN_VOLATILITY_PCT = config["min_volatility"]
    MIN_TURNOVER_USD = config["min_turnover"]
    RSI_MIN, RSI_MAX = config["rsi_range"]
    ADX_MIN = config["adx_min"]
    REQUIRE_SMC_BOS = config["require_smc"]
    return config

# ==================== ПОИСК ПАР ====================

def get_volatile_pairs(quote_coin: str, top_n: int) -> List[str]:
    try:
        pairs_resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        pairs_resp.raise_for_status()
        pairs_data = pairs_resp.json()
        if pairs_data.get("error"):
            logger.error(f"Kraken API error: {pairs_data['error']}")
            return []
        all_pairs = pairs_data.get("result", {})
        logger.info(f"Получено {len(all_pairs)} пар от Kraken")
    except Exception as e:
        logger.error(f"Ошибка получения списка пар: {e}")
        return []

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

    logger.info(f"Найдено {len(candidates_names)} кандидатов после фильтрации")

    scored = []
    chunk_size = 50
    for i in range(0, len(candidates_names), chunk_size):
        chunk = candidates_names[i:i+chunk_size]
        try:
            tick_resp = requests.get(f"{BASE_URL}/Ticker", params={"pair": ",".join(chunk)}, timeout=20)
            tick_resp.raise_for_status()
            tick_data = tick_resp.json()
            if tick_data.get("error"):
                logger.warning(f"Ошибка в данных Ticker: {tick_data['error']}")
                time.sleep(0.5)
                continue
        except Exception as e:
            logger.warning(f"Ошибка получения Ticker для чанка: {e}")
            time.sleep(0.5)
            continue

        for pair_name, t in tick_data.get("result", {}).items():
            try:
                high_24h = safe_float(t.get("h", [0,0])[1])
                low_24h = safe_float(t.get("l", [0,0])[1])
                vwap_24h = safe_float(t.get("p", [0,0])[1])
                vol_24h = safe_float(t.get("v", [0,0])[1])
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
    result = [s[0] for s in scored[:top_n]]
    logger.info(f"Отобрано {len(result)} самых волатильных пар")
    return result

# ==================== ЗАПРОС ДАННЫХ ====================

def fetch_klines(pair: str, interval_minutes: int, min_bars: int, retries: int = 3) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            resp = requests.get(f"{BASE_URL}/OHLC", params={"pair": pair, "interval": interval_minutes}, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("error"):
                logger.warning(f"Kraken OHLC error for {pair}: {payload['error']}, attempt {attempt+1}")
                if attempt < retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                return pd.DataFrame()
            result = payload.get("result", {})
            rows = None
            for key, val in result.items():
                if key != "last":
                    rows = val
                    break
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=["start","open","high","low","close","vwap","volume","count"])
            for col in ["open","high","low","close","volume"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df["start"] = pd.to_numeric(df["start"], errors='coerce')
            df = df.dropna()
            df = df.drop_duplicates(subset="start").sort_values("start").reset_index(drop=True)
            return df.tail(min_bars + 10).reset_index(drop=True)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error for {pair}: {e}, attempt {attempt+1}")
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
            else:
                logger.error(f"Failed to fetch data for {pair} after {retries} attempts")
        except Exception as e:
            logger.error(f"Unexpected error for {pair}: {e}")
            break
    return pd.DataFrame()

# ==================== ИНДИКАТОРЫ ====================

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def macd(series: pd.Series, fast=12, slow=26, signal=9) -> Tuple[pd.Series, pd.Series]:
    macd_line = ema(series, fast) - ema(series, slow)
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(length).mean()

def rsi(series: pd.Series, length: int = RSI_LENGTH) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)

def adx(df: pd.DataFrame, length: int = ADX_LENGTH) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_w = tr.ewm(alpha=1/length, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/length, adjust=False).mean() / atr_w.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/length, adjust=False).mean() / atr_w.replace(0, np.nan)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    return dx.ewm(alpha=1/length, adjust=False).mean().fillna(0)

def find_recent_swing_high(df: pd.DataFrame, window: int = 5, exclude_last: int = 2) -> Optional[float]:
    if len(df) < window*2 + 1 + exclude_last:
        return None
    highs = df["high"]
    rolling_max = highs.rolling(window*2+1, center=True).max()
    is_swing = highs == rolling_max
    if not is_swing.any():
        return None
    candidates = is_swing.iloc[:-exclude_last]
    idx = candidates[candidates].index
    if len(idx) == 0:
        return None
    return float(highs.loc[idx[-1]])

def check_divergence(df: pd.DataFrame) -> bool:
    if len(df) < 20:
        return False
    close = df["close"]
    rsi_values = rsi(close)
    price_min_idx = close.iloc[-20:].idxmin()
    rsi_recent_min = rsi_values.iloc[-10:].min()
    rsi_recent_min_idx = rsi_values.iloc[-10:].idxmin()
    price_at_rsi_min = close.loc[rsi_recent_min_idx]
    price_lower = close.iloc[-1] < price_at_rsi_min
    rsi_higher = rsi_values.iloc[-1] > rsi_recent_min
    return price_lower and rsi_higher

def fibonacci_levels(df: pd.DataFrame) -> Dict[str, float]:
    high = df["high"].max()
    low = df["low"].min()
    diff = high - low
    if diff <= 0:
        return {}
    return {
        "0.236": high - diff*0.236,
        "0.382": high - diff*0.382,
        "0.5": high - diff*0.5,
        "0.618": high - diff*0.618,
        "0.786": high - diff*0.786,
    }

def check_fibonacci_support(df: pd.DataFrame, current_price: float, tolerance: float = 0.005) -> bool:
    fibs = fibonacci_levels(df)
    for level, price in fibs.items():
        if abs(current_price - price) / price < tolerance:
            return True
    return False

def analyze_volume(df: pd.DataFrame) -> bool:
    if len(df) < 20:
        return False
    volume = df["volume"]
    avg_volume = volume.rolling(20).mean()
    return volume.iloc[-1] > avg_volume.iloc[-1] * 1.5

def calculate_rr_ratio(results: Dict[str, Any], entry_result: Dict[str, Any]) -> float:
    close = entry_result["close"]
    daily_atr = results["1d"]["atr"]
    stop = close - daily_atr * ATR_MULT_SL
    target = close + daily_atr * ATR_MULT_TP
    risk = close - stop
    reward = target - close
    if risk <= 0:
        return 0.0
    return reward / risk

# ==================== АНАЛИЗ ТАЙМФРЕЙМА ====================

def analyze_timeframe(df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if df.empty or len(df) < params["min_bars"]:
        return None
    last_row = df.iloc[-1]
    if pd.isna(last_row[["open","high","low","close"]]).any():
        return None

    df = df.copy()
    df["ema_fast"] = ema(df["close"], params["ema_fast"])
    df["ema_slow"] = ema(df["close"], params["ema_slow"])
    df["macd_line"], df["macd_signal"] = macd(df["close"])
    df["atr"] = atr(df)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df)

    if len(df) < 3:
        return None
    last = df.iloc[-2]
    prev = df.iloc[-3]
    if last is None or prev is None:
        return None
    if any(pd.isna([last["ema_fast"], last["ema_slow"], last["rsi"], last["adx"]])):
        return None

    trend_up = bool(last["ema_fast"] > last["ema_slow"])
    macd_cross_up = bool(prev["macd_line"] <= prev["macd_signal"] and last["macd_line"] > last["macd_signal"])
    macd_cross_down = bool(prev["macd_line"] >= prev["macd_signal"] and last["macd_line"] < last["macd_signal"])
    ema_cross_down = bool(prev["ema_fast"] >= prev["ema_slow"] and last["ema_fast"] < last["ema_slow"])
    ema_cross_up = bool(prev["ema_fast"] <= prev["ema_slow"] and last["ema_fast"] > last["ema_slow"])
    swing_high = find_recent_swing_high(df.iloc[:-1], window=5)
    bos_up = bool(swing_high is not None and last["close"] > swing_high)

    return {
        "trend_up": trend_up,
        "macd_cross_up": macd_cross_up,
        "macd_cross_down": macd_cross_down,
        "ema_cross_down": ema_cross_down,
        "ema_cross_up": ema_cross_up,
        "bos_up": bos_up,
        "rsi": float(last["rsi"]),
        "adx": float(last["adx"]),
        "close": float(last["close"]),
        "open": float(last["open"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "atr": float(last["atr"]) if not pd.isna(last["atr"]) else 0.0,
        "bar_time": str(safe_int(last["start"])),
        "ema_fast": float(last["ema_fast"]),
        "ema_slow": float(last["ema_slow"]),
    }

# ==================== ЛОГИКА ВХОДА ====================

def check_confluence_entry(results: Dict[str, Any], entry_results: Dict[str, Any],
                          config: Dict[str, Any], df15: pd.DataFrame) -> Tuple[bool, str, Optional[Dict]]:
    if any(results.get(tf) is None for tf in TIMEFRAME_ORDER):
        return False, "Нет данных по трендовым ТФ", None

    if config["require_trend_all"]:
        if not all(results[tf]["trend_up"] for tf in TIMEFRAME_ORDER):
            return False, "Тренд не совпадает на всех ТФ", None
    else:
        trend_score = sum(1 for tf in TIMEFRAME_ORDER if results[tf]["trend_up"])
        if trend_score < 2:
            return False, f"Тренд совпадает только на {trend_score}/3 ТФ", None

    best_trigger = None
    best_score = 0
    for tf, entry_result in entry_results.items():
        if entry_result is None:
            continue
        if not (entry_result["macd_cross_up"] or entry_result["ema_cross_up"]):
            continue
        score = 0
        if entry_result["macd_cross_up"]:
            score += 2
        if entry_result["ema_cross_up"]:
            score += 1
        if entry_result["bos_up"]:
            score += 1
        if score > best_score:
            best_score = score
            best_trigger = entry_result.copy()
            best_trigger["tf"] = tf

    if best_trigger is None:
        return False, "Нет триггера на вход", None

    if not (config["rsi_range"][0] <= best_trigger["rsi"] <= config["rsi_range"][1]):
        return False, f"RSI вне диапазона ({best_trigger['rsi']:.1f})", None

    if results[ADX_REF_TF]["adx"] < config["adx_min"]:
        return False, f"ADX слишком низкий ({results[ADX_REF_TF]['adx']:.1f})", None

    if config["require_smc"] and not best_trigger["bos_up"]:
        return False, "Нет BOS", None

    if config["require_volume"] and not analyze_volume(df15):
        return False, "Недостаточный объем", None

    if config["require_divergence"] and not check_divergence(df15):
        return False, "Нет дивергенции", None

    if config["require_fibonacci"] and not check_fibonacci_support(df15, best_trigger["close"]):
        return False, "Нет поддержки Фибоначчи", None

    rr = calculate_rr_ratio(results, best_trigger)
    if rr < config["min_rr_ratio"]:
        return False, f"RR < минимального ({rr:.2f})", None

    return True, f"✅ Все условия выполнены (TF: {best_trigger['tf']})", best_trigger

# ==================== ЛОГИКА ВЫХОДА ====================

def check_exit(results: Dict[str, Any], pos: Dict[str, Any]) -> Tuple[bool, str]:
    r4h = results.get(TRIGGER_TF)
    if r4h is None:
        return False, ""

    if r4h["close"] <= pos["stop"]:
        if pos.get("trailing_active"):
            reason = "Трейлинг-стоп"
        elif pos.get("breakeven_moved"):
            reason = "Безубыток (Break-Even)"
        else:
            reason = "Stop-Loss"
        return True, reason

    if r4h["close"] >= pos["target"]:
        return True, "Take-Profit"

    if r4h["ema_cross_down"] or r4h["macd_cross_down"]:
        return True, "Сигнал разворота (4h)"

    return False, ""

def maybe_move_to_breakeven(pos: Dict[str, Any], current_close: float) -> bool:
    if pos.get("breakeven_moved"):
        return False
    atr_entry = pos.get("atr_entry", 0)
    if atr_entry <= 0:
        return False
    profit_in_atr = (current_close - pos["entry_price"]) / atr_entry
    if profit_in_atr >= BREAKEVEN_TRIGGER_ATR:
        new_stop = pos["entry_price"] * (1 + BREAKEVEN_BUFFER_PCT / 100)
        if new_stop > pos["stop"]:
            pos["stop"] = new_stop
            pos["breakeven_moved"] = True
            return True
    return False

def maybe_trail_stop(pos: Dict[str, Any], current_close: float, current_atr: float) -> bool:
    if not pos.get("breakeven_moved"):
        return False
    if current_atr <= 0:
        return False
    candidate_stop = current_close - current_atr * TRAILING_ATR_MULT
    if candidate_stop > pos["stop"]:
        pos["stop"] = candidate_stop
        pos["trailing_active"] = True
        return True
    return False

# ==================== TELEGRAM ====================

def load_subscribers() -> Dict[str, Any]:
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {"offset": 0, "chat_ids": []}

def save_subscribers(data: Dict[str, Any]) -> None:
    try:
        with open(SUBSCRIBERS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        logger.error(f"Ошибка сохранения подписчиков: {e}")

def poll_new_subscribers() -> None:
    if not TELEGRAM_BOT_TOKEN:
        return
    data = load_subscribers()
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = requests.get(url, params={"offset": data["offset"]+1, "timeout": 0}, timeout=15)
        resp.raise_for_status()
        updates = resp.json().get("result", [])
    except Exception as e:
        logger.error(f"Не удалось получить обновления Telegram: {e}")
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
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": chat_id, "text": WELCOME_TEXT},
                    timeout=15
                )
            except Exception as e:
                logger.error(f"Не удалось отправить приветствие {chat_id}: {e}")
        if text.startswith("/stop") and chat_id in data["chat_ids"]:
            data["chat_ids"].remove(chat_id)
    save_subscribers(data)
    logger.info(f"Подписчиков: {len(data['chat_ids'])}")

def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        logger.info(f"[NO TELEGRAM CONFIG] {text}")
        return
    data = load_subscribers()
    chat_ids = set(data.get("chat_ids", []))
    if TELEGRAM_CHAT_ID:
        chat_ids.add(TELEGRAM_CHAT_ID)
    if not chat_ids:
        logger.info(f"[NO SUBSCRIBERS] {text}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    still_active = []
    for chat_id in chat_ids:
        try:
            # ИСПРАВЛЕННЫЙ ВЫЗОВ: аргумент json принимает словарь, а не строку
            r = requests.post(
                url,
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=15
            )
            if r.status_code == 403:
                continue
            r.raise_for_status()
            still_active.append(chat_id)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение {chat_id}: {e}")
            still_active.append(chat_id)

    data["chat_ids"] = [c for c in data.get("chat_ids", []) if c in still_active or c == TELEGRAM_CHAT_ID]
    save_subscribers(data)

# ==================== УПРАВЛЕНИЕ СОСТОЯНИЕМ И СТАТУСАМИ ====================

def load_json(path: str, default: Any) -> Any:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except:
            pass
    return default

def save_json(path: str, data: Any) -> None:
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        logger.error(f"Ошибка сохранения {path}: {e}")

def log_trade(symbol: str, entry_price: float, exit_price: float,
              entry_time: str, exit_time: str, reason: str) -> float:
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

def should_send_status() -> bool:
    """Проверяет, нужно ли отправлять статусное сообщение (интервал в минутах)."""
    if STATUS_INTERVAL_MINUTES <= 0:
        return True
    try:
        if os.path.exists(LAST_STATUS_FILE):
            with open(LAST_STATUS_FILE, "r") as f:
                data = json.load(f)
                last_time = datetime.fromisoformat(data.get("last_status_time", "2000-01-01T00:00:00"))
                elapsed = (datetime.now(timezone.utc) - last_time).total_seconds() / 60
                if elapsed < STATUS_INTERVAL_MINUTES:
                    return False
    except Exception:
        pass
    try:
        with open(LAST_STATUS_FILE, "w") as f:
            json.dump({"last_status_time": datetime.now(timezone.utc).isoformat()}, f)
    except Exception:
        pass
    return True

# ==================== ОТПРАВКА СТАТУСА ====================

def send_status_message(scan_summary: list, pairs_count: int, open_positions: int,
                        found_buy: int, found_sell: int) -> None:
    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    lines = [
        f"📡 <b>Статус сканирования</b> — {now_str}",
        f"━━━━━━━━━━━━━━━━━━━━━",
        f"📊 <b>Общая статистика:</b>",
        f"• Отслеживается пар: {pairs_count}",
        f"• Открытых позиций: {open_positions}",
        f"• Входов за цикл: {found_buy}",
        f"• Выходов за цикл: {found_sell}",
    ]

    close_calls = [s for s in scan_summary if s["trend_score"] >= 2]
    close_calls.sort(key=lambda s: (s["trend_score"], s["adx_1d"]), reverse=True)

    if close_calls:
        lines.append(f"\n🎯 <b>Топ кандидатов на вход:</b>")
        for i, s in enumerate(close_calls[:3], 1):
            strength = "🟢 Strong" if s["trend_score"] == 3 else "🟡 Moderate" if s["adx_1d"] > 20 else "🔴 Weak"
            lines.append(
                f"{i}. <b>{s['pair']}</b>\n"
                f"   Тренд: {s['trend_score']}/3 | ADX: {s['adx_1d']:.0f} | RSI(4h): {s['rsi_4h']:.0f}\n"
                f"   Сила: {strength}"
            )
    else:
        lines.append(f"\n😴 <b>Кандидатов на вход нет</b>")

    if open_positions > 0:
        lines.append(f"\n💰 <b>Активные позиции:</b> {open_positions}")

    lines.append(f"\n━━━━━━━━━━━━━━━━━━━━━")
    if STATUS_INTERVAL_MINUTES > 0:
        lines.append(f"🔄 Следующее статусное сообщение через {STATUS_INTERVAL_MINUTES // 60} ч.")
    else:
        lines.append("🔄 Статус при каждом запуске.")
    send_telegram("\n".join(lines))

# ==================== РЕЖИМ SCAN (ОДНОКРАТНЫЙ) ====================

def run_scan(args: argparse.Namespace) -> None:
    config = get_strategy_config(args.strategy)
    logger.info(f"Стратегия: {config['name']} — {config['description']}")

    state = load_json(STATE_FILE, {})
    poll_new_subscribers()

    if not os.path.exists(TRADES_LOG_FILE):
        save_json(TRADES_LOG_FILE, [])
    if not os.path.exists(STATE_FILE):
        save_json(STATE_FILE, {})

    pairs = get_volatile_pairs(args.quote_coin, config["top_n"])
    logger.info(f"Отслеживаю {len(pairs)} пар (мин. волатильность {MIN_VOLATILITY_PCT}%)")
    if not pairs:
        logger.warning("Не найдено подходящих пар!")
        return

    found_buy, found_sell = 0, 0
    now_iso = datetime.now(timezone.utc).isoformat()
    scan_summary = []

    for pair in pairs:
        results = {}
        try:
            for tf in TIMEFRAME_ORDER:
                params = TIMEFRAME_PARAMS[tf]
                df = fetch_klines(pair, params["kraken_interval"], params["min_bars"] + 5)
                results[tf] = analyze_timeframe(df, params)
                time.sleep(args.request_delay)
        except Exception as e:
            logger.error(f"[{pair}] ошибка получения данных: {e}")
            continue

        if all(results.get(tf) is not None for tf in TIMEFRAME_ORDER):
            trend_score = sum(1 for tf in TIMEFRAME_ORDER if results[tf]["trend_up"])
            scan_summary.append({
                "pair": pair,
                "trend_score": trend_score,
                "rsi_4h": results["4h"]["rsi"],
                "adx_1d": results["1d"]["adx"],
            })

        pos = state.get(pair, {"position": "closed"})

        if pos.get("position") == "open":
            if results.get(TRIGGER_TF) is None or results.get("1d") is None:
                logger.debug(f"[{pair}] недостаточно данных для проверки открытой позиции")
                continue

            moved = maybe_move_to_breakeven(pos, results[TRIGGER_TF]["close"])
            if moved:
                msg = f"🔒 <b>Стоп переведён в безубыток</b>\nПара: <b>{pair}</b>\nНовый стоп: {pos['stop']:.6g}"
                send_telegram(msg)
                logger.info(f"[{pair}] стоп переведён в безубыток: {pos['stop']:.6g}")

            trailed = maybe_trail_stop(pos, results[TRIGGER_TF]["close"], results["1d"]["atr"])
            if trailed:
                logger.info(f"[{pair}] трейлинг-стоп подтянут: {pos['stop']:.6g}")

            exit_now, reason = check_exit(results, pos)
            if exit_now:
                exit_price = results[TRIGGER_TF]["close"]
                pnl_pct = log_trade(pair, pos["entry_price"], exit_price,
                                    pos["entry_time"], now_iso, reason)
                msg = (
                    f"🔴 <b>ВЫХОД (SELL)</b>\n"
                    f"Пара: <b>{pair}</b>\n"
                    f"Цена выхода: <b>{exit_price:.6g}</b>\n"
                    f"Причина: {reason}\n"
                    f"Результат: <b>{pnl_pct:+.2f}%</b>"
                )
                send_telegram(msg)
                logger.info(msg)
                state[pair] = {"position": "closed"}
                found_sell += 1
                save_json(STATE_FILE, state)

        else:
            has_all_data = all(results.get(tf) is not None for tf in TIMEFRAME_ORDER)
            trend_all_up = has_all_data and all(results[tf]["trend_up"] for tf in TIMEFRAME_ORDER)
            entry_results = {}
            if trend_all_up:
                for tf in ENTRY_TRIGGER_TFS:
                    try:
                        entry_params = TIMEFRAME_PARAMS[tf]
                        df_tf = fetch_klines(pair, entry_params["kraken_interval"], entry_params["min_bars"] + 5)
                        entry_results[tf] = analyze_timeframe(df_tf, entry_params)
                        time.sleep(args.request_delay)
                    except Exception as e:
                        logger.error(f"[{pair}] ошибка получения {tf} данных: {e}")
            else:
                entry_results = {tf: None for tf in ENTRY_TRIGGER_TFS}

            df15 = None
            if "15m" in entry_results and entry_results["15m"] is not None:
                try:
                    entry_params = TIMEFRAME_PARAMS["15m"]
                    df15 = fetch_klines(pair, entry_params["kraken_interval"], entry_params["min_bars"] + 5)
                except:
                    df15 = pd.DataFrame()

            ok, reason, trigger = check_confluence_entry(results, entry_results, config, df15)
            if ok and trigger is not None:
                close = trigger["close"]
                daily_atr = results["1d"]["atr"]
                stop = close - daily_atr * ATR_MULT_SL
                target = close + daily_atr * ATR_MULT_TP

                msg = (
                    f"🟢 <b>ВХОД (BUY) — тренд 4h/1d/1w + точный триггер</b>\n"
                    f"Пара: <b>{pair}</b>\n"
                    f"Цена входа: <b>{close:.6g}</b>\n"
                    f"RSI(15m): {trigger['rsi']:.1f}  ADX(1d): {results['1d']['adx']:.1f}\n"
                    f"Smart Money BOS: {'✅ пробит swing high' if trigger['bos_up'] else '—'}\n"
                    f"Stop-Loss: {stop:.6g}\n"
                    f"Take-Profit: {target:.6g}\n"
                    f"Причина: {reason}"
                )
                send_telegram(msg)
                logger.info(msg)

                state[pair] = {
                    "position": "open",
                    "entry_price": close,
                    "entry_time": now_iso,
                    "stop": stop,
                    "target": target,
                    "atr_entry": daily_atr,
                    "breakeven_moved": False,
                    "trailing_active": False,
                }
                found_buy += 1
                save_json(STATE_FILE, state)

    save_json(STATE_FILE, state)
    logger.info(f"Готово. Новых входов: {found_buy}, выходов: {found_sell}")

    open_positions = sum(1 for p in state.values() if p.get("position") == "open")
    if should_send_status():
        send_status_message(scan_summary, len(pairs), open_positions, found_buy, found_sell)
    else:
        logger.info(f"Статусное сообщение пропущено (интервал {STATUS_INTERVAL_MINUTES} мин)")

# ==================== РЕЖИМ REPORT ====================

def run_report(args: argparse.Namespace) -> None:
    poll_new_subscribers()
    if not os.path.exists(TRADES_LOG_FILE):
        save_json(TRADES_LOG_FILE, [])
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

# ==================== НЕПРЕРЫВНЫЙ РЕЖИМ (по умолчанию) ====================

def run_forever():
    """Запускает сканер в бесконечном цикле с интервалом SCAN_INTERVAL_SECONDS."""
    logger.info("🚀 Запуск сканера в НЕПРЕРЫВНОМ режиме...")
    logger.info(f"⏱️ Интервал между сканированиями: {SCAN_INTERVAL_SECONDS // 3600} час(ов)")
    logger.info(f"📊 Статус будет отправляться не чаще {STATUS_INTERVAL_MINUTES // 60} час(ов)")

    # Аргументы по умолчанию для непрерывного режима
    default_args = argparse.Namespace(
        mode="scan",
        period="3d",
        top_n=200,                 # сканируем максимум пар
        quote_coin="USD",
        request_delay=0.3,
        strategy="balanced"        # можно изменить на aggressive или conservative
    )

    while True:
        try:
            run_scan(default_args)
        except KeyboardInterrupt:
            logger.info("⏹️ Остановка по запросу пользователя.")
            break
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в цикле: {e}", exc_info=True)
            logger.info("⏳ Пауза 60 секунд перед повторной попыткой...")
            time.sleep(60)
        logger.info(f"⏳ Ожидание {SCAN_INTERVAL_SECONDS // 3600} час(ов) до следующего сканирования...")
        time.sleep(SCAN_INTERVAL_SECONDS)

# ==================== MAIN ====================

def main() -> None:
    parser = argparse.ArgumentParser(description="Kraken Signal Scanner – непрерывный режим по умолчанию")
    parser.add_argument("--mode", choices=["scan", "report"], default=None,
                        help="Режим работы (если не указан – непрерывный режим)")
    parser.add_argument("--period", choices=["3d", "month"], default="3d", help="Период отчёта")
    parser.add_argument("--top-n", type=int, default=200, help="Количество пар для сканирования")
    parser.add_argument("--quote-coin", default="USD", help="Базовая валюта")
    parser.add_argument("--request-delay", type=float, default=0.3, help="Задержка между запросами (сек)")
    parser.add_argument("--strategy", choices=["aggressive", "balanced", "conservative"],
                        default="balanced", help="Стратегия сканирования")
    args = parser.parse_args()

    # Если режим не указан или указан scan без дополнительных ключей – запускаем непрерывный режим
    if args.mode is None:
        run_forever()
    elif args.mode == "scan":
        run_scan(args)
    elif args.mode == "report":
        run_report(args)

if __name__ == "__main__":
    main()
