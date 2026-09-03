#!/usr/bin/env python3
"""
Универсальный бот (VPS): WebSocket + REST сканер.
ВЕРСИЯ V15 - "Гибрид" (V11 + V14.1).
- Возвращена проверка тренда 3/3 таймфреймов (из V11) через кэш.
- Сохранены все фиксы V14.1 (RLock, NoneType, unhashable, fallback).
"""

import json
import os
import time
import logging
import threading
import requests
import pandas as pd
import numpy as np
import websocket
from collections import deque
from datetime import datetime, timezone, timedelta

# ==================== КОНФИГУРАЦИЯ ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
BASE_URL = "https://api.kraken.com/0/public"
WS_URL = "wss://ws.kraken.com/v2"

TOP_N = 200
TOTAL_PAIRS = 700
SCAN_INTERVAL_SECONDS = 7200
MIN_TURNOVER_USD = 50000

TIMEFRAME = 15
MIN_BARS = 80
TRIGGER_TF = "4h"
TIME_STOP_DAYS = 7

MAX_OPEN_POSITIONS = 5
MAX_TRADES_PER_HOUR = 10
ENTRY_COOLDOWN_SECONDS = 3600
MIN_STOP_DISTANCE_PCT = 0.5
MAX_ENTRY_SLIPPAGE_PCT = 0.5
MAX_BREAKOUT_DISTANCE_PCT = 3.0

FEE_PCT = 0.25
SLIPPAGE_PCT = 0.05

ATR_MULT_SL = 3.0
ATR_MULT_TP = 6.0
BREAKEVEN_TRIGGER_ATR = 1.0
TRAILING_TRIGGER_ATR = 2.0
TRAILING_STEP_ATR = 1.0
ADX_MAX = 45

CLEANUP_AFTER_DAYS = 14

STATE_FILE = "/opt/kraken-scanner/kraken_ws_state.json"
TRADES_LOG_FILE = "/opt/kraken-scanner/trades_log.json"
MAX_STATUS_PAIRS = 20

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==================== ГЛОБАЛЬНОЕ СОСТОЯНИЕ ====================
state = {}
state_lock = threading.RLock()
trade_times = []
ohlc_buffers = {}
last_processed_closed = {}

# НОВОЕ: Кэш квалифицированных пар (тренд 3/3) для WebSocket
qualified_cache = set()
qualified_cache_lock = threading.RLock()

REST_PAIR_BY_WSNAME = {}
WSNAME_BY_RESTNAME = {}

# ==================== ФУНКЦИИ ====================
def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram отключен: нет токена или chat_id")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        logger.error(f"Ошибка отправки Telegram: {e}")

def save_state(state_data):
    with state_lock:
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            tmp_file = STATE_FILE + ".tmp"
            with open(tmp_file, 'w') as f:
                json.dump(state_data, f, indent=2)
            os.replace(tmp_file, STATE_FILE)
        except Exception as e:
            logger.error(f"Ошибка сохранения state: {e}")

def load_state():
    with state_lock:
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.critical(f"Не удалось загрузить state: {e}")
            try:
                os.replace(STATE_FILE, STATE_FILE + ".corrupt")
            except Exception:
                pass
            return {}

def log_trade(symbol, entry, exit_price, reason, strategy, entry_time=None):
    with state_lock:
        trades = []
        if os.path.exists(TRADES_LOG_FILE):
            try:
                with open(TRADES_LOG_FILE, 'r') as f: trades = json.load(f)
            except: trades = []
        os.makedirs(os.path.dirname(TRADES_LOG_FILE), exist_ok=True)
        now_iso = datetime.now(timezone.utc).isoformat()
        if not entry_time: entry_time = now_iso
        raw_pnl = (exit_price - entry) / entry * 100 if entry else 0
        net_pnl = raw_pnl - (FEE_PCT * 2) - SLIPPAGE_PCT
        trades.append({
            "symbol": symbol, "entry_price": entry, "exit_price": exit_price,
            "entry_time": entry_time, "exit_time": now_iso,
            "raw_pnl_pct": round(raw_pnl, 2), "net_pnl_pct": round(net_pnl, 2),
            "pnl_pct": round(net_pnl, 2), "result": "win" if net_pnl > 0 else "loss",
            "reason": reason, "strategy": strategy
        })
        with open(TRADES_LOG_FILE, 'w') as f: json.dump(trades, f, indent=2)
        return round(net_pnl, 2)

# ==================== ИНДИКАТОРЫ ====================
def ema(series, period): return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return true_range.rolling(period).mean()

def adx(df, period=14):
    plus_dm = df['high'].diff()
    minus_dm = df['low'].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0
    tr = atr(df, 1)
    plus_di = 100 * (plus_dm.ewm(alpha=1/period).mean() / tr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/period).mean() / tr)
    dx = 100 * np.abs((plus_di - minus_di) / (plus_di + minus_di))
    return dx.ewm(alpha=1/period).mean()

def analyze_timeframe(df, params):
    if df.empty or len(df) < params['min_bars']:
        return None
    df = df.copy()
    df['ema_fast'] = ema(df['close'], params['ema_fast'])
    df['ema_slow'] = ema(df['close'], params['ema_slow'])
    df['macd_line'] = ema(df['close'], 12) - ema(df['close'], 26)
    df['macd_signal'] = ema(df['macd_line'], 9)
    df['rsi'] = rsi(df['close'])
    df['adx'] = adx(df)
    df['atr'] = atr(df)
    if len(df) < 3:
        return None
    last = df.iloc[-2]
    prev = df.iloc[-3]
    if any(pd.isna([last['ema_fast'], last['ema_slow'], last['rsi'],
                    last['adx'], last['macd_line'], last['macd_signal']])):
        return None
    return {
        'trend_up': bool(last['ema_fast'] > last['ema_slow']),
        'macd_cross_up': bool(prev['macd_line'] <= prev['macd_signal'] and last['macd_line'] > last['macd_signal']),
        'ema_cross_down': bool(prev['ema_fast'] >= prev['ema_slow'] and last['ema_fast'] < last['ema_slow']),
        'ema_cross_up': bool(prev['ema_fast'] <= prev['ema_slow'] and last['ema_fast'] > last['ema_slow']),
        'rsi': float(last['rsi']),
        'adx': float(last['adx']),
        'close': float(last['close']),
        'high': float(last['high']),
        'low': float(last['low']),
        'open': float(last['open']),
        'atr': float(last['atr']) if not pd.isna(last['atr']) else 0.0,
        'macd_line': float(last['macd_line']),
        'macd_signal': float(last['macd_signal']),
        'ema_fast': float(last['ema_fast']),
        'ema_slow': float(last['ema_slow']),
    }

# ==================== СТРАТЕГИИ ====================
def detect_consolidation(df_daily):
    closed = df_daily.iloc[:-1]
    if len(closed) < 60: return None
    window = closed.tail(30)
    high = window['high'].max(); low = window['low'].min(); mean = window['close'].mean()
    if mean <= 0: return None
    range_pct = (high - low) / mean * 100
    adx_val = adx(closed).iloc[-1]
    if range_pct <= 15.0 and adx_val < 20:
        return {"days": len(window), "range_pct": range_pct, "adx": adx_val, "upper_level": high, "lower_level": low}
    return None

def check_breakout(df_daily, current_price):
    closed = df_daily.iloc[:-1]
    if len(closed) < 60: return None
    window = closed.tail(30)
    high = window['high'].max(); low = window['low'].min(); mean = window['close'].mean()
    if mean <= 0: return None
    range_pct = (high - low) / mean * 100
    if range_pct > 15.0: return None
    if current_price < high * 1.015: return None
    if current_price > high * (1 + MAX_BREAKOUT_DISTANCE_PCT / 100): return None
    if len(closed) < 21: return None
    vol_avg = closed['volume'].rolling(20).mean().iloc[-2]
    last_closed_volume = closed['volume'].iloc[-1]
    vol_ok = vol_avg > 0 and last_closed_volume > vol_avg * 1.8
    if not vol_ok: return None
    adx_val = adx(closed).iloc[-1]
    atr_val = atr(closed).iloc[-1]
    if pd.isna(adx_val) or adx_val >= 25: return None
    if pd.isna(atr_val) or atr_val <= 0: return None
    return {"days": len(window), "range_pct": range_pct, "adx": adx_val,
            "stop": current_price - atr_val * ATR_MULT_SL, "target": current_price + atr_val * ATR_MULT_TP}

def check_exit(results, pos):
    if not results.get(TRIGGER_TF) or not results.get('1d'): return False, ""
    r4h = results[TRIGGER_TF]; r1d = results['1d']
    if r4h['low'] <= pos['stop']: return True, "Stop-Loss"
    if r4h['high'] >= pos['target']: return True, "Take-Profit"
    if (pos.get('entry_price') and r4h['close'] >= pos['entry_price'] + (BREAKEVEN_TRIGGER_ATR * r1d['atr'])
        and r1d['atr'] > 0 and not pos.get('breakeven_moved')):
        pos['stop'] = pos['entry_price'] * 1.001; pos['breakeven_moved'] = True
        return False, "Безубыток"
    if (pos.get('entry_price') and r4h['close'] >= pos['entry_price'] + (TRAILING_TRIGGER_ATR * r1d['atr']) and r1d['atr'] > 0):
        new_stop = r4h['close'] - (TRAILING_STEP_ATR * r1d['atr'])
        if new_stop > pos['stop']: pos['stop'] = new_stop
        return False, "Трейлинг-стоп"
    if r4h['macd_cross_up'] is False and r4h.get('ema_cross_down', False): return True, "Разворот"
    return False, ""

# ==================== ДВИЖОК ====================
def can_enter(pair):
    now = time.time(); old_state = state.get(pair, {})
    if old_state.get('position') == 'open': return False
    if now - float(old_state.get('last_exit_ts', 0)) < ENTRY_COOLDOWN_SECONDS: return False
    open_positions = sum(1 for p in state.values() if p.get('position') == 'open')
    if open_positions >= MAX_OPEN_POSITIONS: return False
    active_trades = len([t for t in trade_times if now - t < 3600])
    if active_trades >= MAX_TRADES_PER_HOUR: return False
    return True

# ==================== МАППИНГ ПАР ====================
def build_asset_pairs():
    global REST_PAIR_BY_WSNAME, WSNAME_BY_RESTNAME
    try:
        resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        data = resp.json(); result = data.get("result", {})
        for rest_name, info in result.items():
            wsname = info.get("wsname")
            if wsname:
                REST_PAIR_BY_WSNAME[wsname] = rest_name
                WSNAME_BY_RESTNAME[rest_name] = wsname
        logger.info(f"Загружено {len(REST_PAIR_BY_WSNAME)} пар")
    except Exception as e: logger.error(f"Ошибка загрузки AssetPairs: {e}")

def get_filtered_pairs(top_n):
    try:
        pairs_resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        pairs_data = pairs_resp.json()
        candidates = []
        for pair_name, info in pairs_data.get("result", {}).items():
            wsname = info.get("wsname", "")
            if not wsname.endswith("/USD"): continue
            base = wsname.split('/')[0]
            if base in {"USDC", "USDT", "DAI", "PYUSD", "TUSD", "FDUSD"}: continue
            rest_name = REST_PAIR_BY_WSNAME.get(wsname)
            if rest_name: candidates.append(rest_name)
        if not candidates: return []
        scored = []
        chunk_size = 50
        for i in range(0, len(candidates), chunk_size):
            chunk = candidates[i:i+chunk_size]
            try:
                tick_resp = requests.get(f"{BASE_URL}/Ticker", params={"pair": ",".join(chunk)}, timeout=20)
                tick_data = tick_resp.json()
                if tick_data.get("error"): continue
            except Exception as e:
                logger.warning(f"Ошибка тикеров: {e}")
                continue
            for pair_name, t in tick_data.get("result", {}).items():
                try:
                    high_24h = float(t.get("h", [0, 0])[1]); low_24h = float(t.get("l", [0, 0])[1])
                    vwap_24h = float(t.get("p", [0, 0])[1]); vol_24h = float(t.get("v", [0, 0])[1])
                    turnover = vwap_24h * vol_24h
                    if turnover < MIN_TURNOVER_USD or low_24h <= 0: continue
                    volatility_pct = ((high_24h - low_24h) / low_24h) * 100
                    ws_name = WSNAME_BY_RESTNAME.get(pair_name)
                    if ws_name: scored.append((ws_name, volatility_pct, turnover))
                except: continue
            time.sleep(0.1)
        scored.sort(key=lambda x: x[1], reverse=True)
        return [s[0] for s in scored[:top_n]]
    except Exception as e:
        logger.error(f"Ошибка get_filtered_pairs: {e}")
        return []

def get_all_available_pairs(max_pairs):
    try:
        pairs_resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        pairs_data = pairs_resp.json()
        candidates = []
        for pair_name, info in pairs_data.get("result", {}).items():
            wsname = info.get("wsname", "")
            if not wsname.endswith("/USD"): continue
            base = wsname.split('/')[0]
            if base in {"USDC", "USDT", "DAI", "PYUSD", "TUSD", "FDUSD"}: continue
            candidates.append(wsname)
        return candidates[:max_pairs]
    except Exception as e:
        logger.error(f"Ошибка get_all_available_pairs: {e}")
        return []

# ==================== ЗАГРУЗКА ДАННЫХ ====================
def fetch_klines(pair, interval, min_bars):
    pair_name = REST_PAIR_BY_WSNAME.get(pair, pair.replace('/', ''))
    try:
        resp = requests.get(f"{BASE_URL}/OHLC", params={"pair": pair_name, "interval": interval}, timeout=20)
        data = resp.json()
        if data.get('error'): return []
        key = list(data.get('result', {}).keys())[0]
        raw = data['result'][key]
        df = pd.DataFrame(raw, columns=['time', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count'])
        df['time'] = pd.to_numeric(df['time'], errors='coerce')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)
        df.rename(columns={'time': 'start'}, inplace=True)
        return df.tail(min_bars).to_dict('records')
    except Exception as e:
        logger.error(f"Ошибка fetch_klines для {pair}: {e}")
        return []

def fetch_current_prices(pairs):
    prices = {}
    if not pairs: return prices
    rest_names = []
    for p in pairs:
        if not isinstance(p, str): continue
        rest_name = REST_PAIR_BY_WSNAME.get(p)
        if rest_name: rest_names.append(rest_name)
    if not rest_names: return prices
    chunk_size = 50
    for i in range(0, len(rest_names), chunk_size):
        chunk = rest_names[i:i+chunk_size]
        try:
            tick_resp = requests.get(f"{BASE_URL}/Ticker", params={"pair": ",".join(chunk)}, timeout=20)
            tick_data = tick_resp.json()
            if tick_data.get("error"): continue
        except Exception as e:
            logger.warning(f"Ошибка тикеров: {e}")
            continue
        for pair_name, t in tick_data.get("result", {}).items():
            try:
                ws_name = WSNAME_BY_RESTNAME.get(pair_name)
                if ws_name: prices[ws_name] = float(t.get("c", [0])[0])
            except: continue
        time.sleep(0.1)
    return prices

# ==================== WEBSOCKET ====================
def on_open(ws):
    logger.info(f"WebSocket подключен. Подписываемся на {len(PAIRS_WS)} пар...")
    ws.send(json.dumps({
        "method": "subscribe",
        "params": {"channel": "ohlc", "symbol": PAIRS_WS, "interval": TIMEFRAME}
    }))

def on_message(ws, message):
    global state, trade_times, qualified_cache
    try:
        data = json.loads(message)
        if data.get('channel') != 'ohlc' or data.get('type') != 'update':
            return
        for item in data.get('data', []):
            if item.get('time') is None:
                continue
            symbol = item.get('symbol', '')
            if symbol not in ohlc_buffers:
                continue
            try:
                new_candle = {
                    'start': int(item.get('time')),
                    'open': float(item.get('open')),
                    'high': float(item.get('high')),
                    'low': float(item.get('low')),
                    'close': float(item.get('close')),
                    'volume': float(item.get('volume')),
                }
            except (TypeError, ValueError):
                continue
            if ohlc_buffers[symbol] and ohlc_buffers[symbol][-1]['start'] == new_candle['start']:
                ohlc_buffers[symbol][-1] = new_candle
            else:
                ohlc_buffers[symbol].append(new_candle)
            exit_messages = []
            with state_lock:
                pos = state.get(symbol, {})
                if pos.get('position') == 'open':
                    if new_candle['low'] <= pos['stop']:
                        exit_price = min(float(new_candle['open']), float(pos['stop']))
                        pnl_pct = log_trade(symbol, pos['entry_price'], exit_price, "Stop-Loss", pos.get('strategy', 'unknown'), pos.get('entry_time'))
                        exit_messages.append(f"🔴 <b>СТОП-ЛОСС (WebSocket)</b>\nПара: {symbol}\nЦена: {exit_price:.8f}\nРезультат: <b>{pnl_pct:+.2f}%</b>")
                        pos.update({'position': 'closed', 'last_exit_ts': time.time(), 'last_exit_price': exit_price, 'last_exit_reason': 'stop-loss'})
                        state[symbol] = pos
                        save_state(state)
                    elif new_candle['high'] >= pos['target']:
                        exit_price = pos['target']
                        pnl_pct = log_trade(symbol, pos['entry_price'], exit_price, "Take-Profit", pos.get('strategy', 'unknown'), pos.get('entry_time'))
                        exit_messages.append(f"🟢 <b>ТЕЙК-ПРОФИТ (WebSocket)</b>\nПара: {symbol}\nЦена: {exit_price:.8f}\nРезультат: <b>{pnl_pct:+.2f}%</b>")
                        pos.update({'position': 'closed', 'last_exit_ts': time.time(), 'last_exit_price': exit_price, 'last_exit_reason': 'take-profit'})
                        state[symbol] = pos
                        save_state(state)
            for msg in exit_messages:
                send_telegram(msg)
            if len(ohlc_buffers[symbol]) < MIN_BARS + 1:
                continue
            closed_start = int(ohlc_buffers[symbol][-2]['start'])
            if last_processed_closed.get(symbol) == closed_start:
                continue
            last_processed_closed[symbol] = closed_start
            df = pd.DataFrame(list(ohlc_buffers[symbol]))
            df['ema_fast'] = ema(df['close'], 9)
            df['ema_slow'] = ema(df['close'], 21)
            df['macd_line'] = ema(df['close'], 12) - ema(df['close'], 26)
            df['macd_signal'] = ema(df['macd_line'], 9)
            df['atr'] = atr(df)
            df['rsi'] = rsi(df['close'])
            df['adx'] = adx(df)
            if len(df) < 3:
                continue
            signal_candle = df.iloc[-2]
            current_candle = df.iloc[-1]
            prev = df.iloc[-3]
            if pd.isna(signal_candle['ema_fast']) or pd.isna(signal_candle['rsi']):
                continue
            if not (prev['macd_line'] <= prev['macd_signal']
                    and signal_candle['macd_line'] > signal_candle['macd_signal']
                    and signal_candle['ema_fast'] > signal_candle['ema_slow']
                    and 40 <= signal_candle['rsi'] <= 75
                    and 20 <= signal_candle['adx'] <= ADX_MAX):
                continue
            # НОВОЕ: Проверка, что пара есть в кэше квалифицированных (тренд 3/3)
            with qualified_cache_lock:
                if symbol not in qualified_cache:
                    continue
            entry_price = float(current_candle['close'])
            atr_value = float(signal_candle['atr'])
            if pd.isna(atr_value) or atr_value <= 0:
                continue
            stop = entry_price - atr_value * ATR_MULT_SL
            target = entry_price + atr_value * ATR_MULT_TP
            if stop >= entry_price or target <= entry_price:
                continue
            stop_distance_pct = (entry_price - stop) / entry_price * 100
            if stop_distance_pct < MIN_STOP_DISTANCE_PCT:
                continue
            with state_lock:
                if not can_enter(symbol):
                    continue
                old_state = state.get(symbol, {})
                if old_state.get('last_signal_candle') == closed_start:
                    continue
                new_state = old_state.copy()
                new_state.update({
                    'position': 'open',
                    'entry_price': entry_price,
                    'stop': stop,
                    'target': target,
                    'entry_time': datetime.now(timezone.utc).isoformat(),
                    'last_signal_candle': closed_start,
                    'last_entry_ts': time.time(),
                    'strategy': 'ws_15m'
                })
                state[symbol] = new_state
                trade_times.append(time.time())
                save_state(state)
            send_telegram(f"🟢 <b>МГНОВЕННЫЙ ВХОД (WebSocket)</b>\nПара: {symbol}\nЦена: {entry_price:.8f}\nSL: {stop:.8f}\nTP: {target:.8f}")
    except Exception as e:
        logger.error(f"Ошибка WebSocket: {e}")

def on_error(ws, error): logger.error(f"WS ошибка: {error}")
def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WS закрыт ({close_status_code}). Переподключение...")
    time.sleep(5)

def run_websocket():
    ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    while True:
        try: ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e: logger.error(f"WS Критическая ошибка: {e}"); time.sleep(5)

# ==================== ФОНОВОЕ СКАНИРОВАНИЕ ====================
TIMEFRAME_PARAMS = {
    "15m": {"kraken_interval": 15, "min_bars": 80, "ema_fast": 9, "ema_slow": 21},
    "1h": {"kraken_interval": 60, "min_bars": 80, "ema_fast": 9, "ema_slow": 21},
    "4h": {"kraken_interval": 240, "min_bars": 80, "ema_fast": 9, "ema_slow": 21},
    "1d": {"kraken_interval": 1440, "min_bars": 150, "ema_fast": 20, "ema_slow": 50},
    "1w": {"kraken_interval": 10080, "min_bars": 50, "ema_fast": 10, "ema_slow": 30}
}

def background_scan_loop():
    global state, qualified_cache
    while True:
        try:
            volatile_pairs = get_filtered_pairs(TOP_N) or []
            all_pairs_for_consolidation = get_all_available_pairs(TOTAL_PAIRS) or []
            with state_lock:
                open_pairs = [p for p, pos in state.items() if pos.get('position') == 'open']
            management_pairs = list(dict.fromkeys(volatile_pairs + open_pairs))
            if not management_pairs:
                logger.error("Нет пар для обработки")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            pairs_for_prices = list(dict.fromkeys(volatile_pairs + all_pairs_for_consolidation + open_pairs))
            current_prices = fetch_current_prices(pairs_for_prices)
            found_buy, found_sell = 0, 0
            scan_summary = []; consolidation_list = []; consolidation_seen = set()
            now_iso = datetime.now(timezone.utc).isoformat()
            daily_cache = {}
            # ОБНОВЛЕНИЕ КЭША КВАЛИФИЦИРОВАННЫХ ПАР (тренд 3/3)
            for pair in management_pairs:
                if not isinstance(pair, str): continue
                try:
                    # Загружаем 4h, 1d, 1w
                    results = {}
                    for tf in ["4h", "1w"]:
                        params = TIMEFRAME_PARAMS[tf]
                        df_tf = pd.DataFrame(fetch_klines(pair, params['kraken_interval'], params['min_bars']))
                        if df_tf.empty: continue
                        results[tf] = analyze_timeframe(df_tf, params)
                    df_daily = pd.DataFrame(fetch_klines(pair, 1440, 150))
                    if not df_daily.empty:
                        results['1d'] = analyze_timeframe(df_daily, TIMEFRAME_PARAMS['1d'])
                    
                    # Если все 3 таймфрейма восходящие - добавляем в кэш
                    if (results.get('4h') and results.get('1d') and results.get('1w') and
                        results['4h']['trend_up'] and results['1d']['trend_up'] and results['1w']['trend_up']):
                        with qualified_cache_lock:
                            qualified_cache.add(pair)
                    else:
                        with qualified_cache_lock:
                            qualified_cache.discard(pair)
                except Exception as e:
                    logger.error(f"Ошибка обновления кэша для {pair}: {e}")
            # ПЕРВЫЙ ПРОХОД
            for idx, pair in enumerate(management_pairs):
                if not isinstance(pair, str): continue
                try:
                    df_daily_data = fetch_klines(pair, 1440, 150)
                    if not df_daily_data: continue
                    df_daily = pd.DataFrame(df_daily_data); daily_cache[pair] = df_daily
                    results = {}; results['1d'] = analyze_timeframe(df_daily, TIMEFRAME_PARAMS['1d'])
                    for tf in ["4h", "1w"]:
                        params = TIMEFRAME_PARAMS[tf]
                        df_tf = pd.DataFrame(fetch_klines(pair, params['kraken_interval'], params['min_bars']))
                        if df_tf.empty: continue
                        results[tf] = analyze_timeframe(df_tf, params)
                    time.sleep(0.3)
                    if not results.get('4h') or not results.get('1d'): continue
                    r4h = results['4h']; r1d = results['1d']
                    trend_score = 0
                    if r4h['ema_fast'] > r4h['ema_slow']: trend_score += 1
                    if r1d['ema_fast'] > r1d['ema_slow']: trend_score += 1
                    if results.get('1w') and results['1w']['ema_fast'] > results['1w']['ema_slow']: trend_score += 1
                    macd_gap_pct = (r4h['macd_line'] - r4h['macd_signal']) / r4h['close'] * 100 if r4h['close'] else 0.0
                    scan_summary.append({"pair": pair, "trend_score": trend_score, "rsi_4h": r4h['rsi'], "adx_1d": r1d['adx'], "macd_gap_pct": macd_gap_pct, "close_price": r4h['close']})
                    current_price = current_prices.get(pair, r4h['close'])
                    cons = detect_consolidation(df
