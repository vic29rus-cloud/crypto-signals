#!/usr/bin/env python3
"""
Универсальный бот (VPS): WebSocket + REST сканер.
ФИНАЛЬНАЯ ВЕРСИЯ: Разделены списки для Confluence (импульс) и Breakout (боковики).
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
from datetime import datetime, timezone

# ==================== КОНФИГУРАЦИЯ ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
BASE_URL = "https://api.kraken.com/0/public"
WS_URL = "wss://ws.kraken.com/v2"

TIMEFRAME = 15
MIN_BARS = 80
RSI_LENGTH = 14
ADX_LENGTH = 14
RSI_MIN, RSI_MAX = 40, 75
ADX_MIN = 20
ATR_MULT_SL, ATR_MULT_TP = 2.0, 4.0
BREAKEVEN_TRIGGER_ATR = 1.0
TRAILING_ATR_MULT = 1.5

# Количество пар для анализа
TOP_N = 200               # Волатильные для Confluence и WebSocket
CONSOLIDATION_PAIRS = 700 # ВСЕ пары (включая спокойные) для поиска боковиков

STATUS_INTERVAL_MINUTES = 120
SCAN_INTERVAL_SECONDS = 7200

STATE_FILE = "/opt/kraken-scanner/kraken_ws_state.json"
TRADES_LOG_FILE = "/opt/kraken-scanner/trades_log.json"
QUALIFIED_PAIRS_FILE = "/opt/kraken-scanner/qualified_pairs.json"

TIMEFRAME_PARAMS = {
    "15m": {"kraken_interval": 15,   "ema_fast": 9,  "ema_slow": 21, "min_bars": 80},
    "4h":  {"kraken_interval": 240,  "ema_fast": 21, "ema_slow": 55, "min_bars": 120},
    "1d":  {"kraken_interval": 1440, "ema_fast": 50, "ema_slow": 100, "min_bars": 150},
    "1w":  {"kraken_interval": 10080, "ema_fast": 8, "ema_slow": 20, "min_bars": 40},
}
TIMEFRAME_ORDER = ["4h", "1d", "1w"]
TRIGGER_TF = "4h"

EXCLUDE_BASE_SUBSTRINGS = ["UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S"]
STABLECOINS = {"USDC", "USDT", "DAI", "USD", "EUR", "GBP", "PYUSD", "TUSD", "FDUSD"}
MIN_TURNOVER_USD = 50000
MIN_VOLATILITY_PCT = 1.0

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
PAIRS_WS = []
PAIRS_VOLATILE = []
PAIRS_ALL = []  # Все пары для боковиков
ohlc_buffers = {}
state = {}
REST_PAIR_BY_WSNAME = {}
state_lock = threading.RLock()

# ==================== МАППИНГ ИМЕН ПАР ====================
def build_asset_pairs():
    global REST_PAIR_BY_WSNAME
    try:
        resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        data = resp.json()
        result = data.get("result", {})
        for rest_name, info in result.items():
            wsname = info.get("wsname")
            if wsname:
                REST_PAIR_BY_WSNAME[wsname] = rest_name
        logger.info(f"Построена карта пар: {len(REST_PAIR_BY_WSNAME)}.")
    except Exception as e:
        logger.error(f"Ошибка построения карты: {e}")

# ==================== ПОЛУЧЕНИЕ ВОЛАТИЛЬНЫХ ПАР (для Confluence) ====================
def get_filtered_pairs(max_pairs=TOP_N):
    try:
        pairs_resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        pairs_data = pairs_resp.json()
        if pairs_data.get("error"):
            logger.error(f"Kraken API error: {pairs_data['error']}")
            return []
        all_pairs = pairs_data.get("result", {})
        pair_map = {k: v.get("wsname") for k, v in all_pairs.items()}
        for rest_name, info in all_pairs.items():
            wsname = info.get("wsname")
            if wsname: REST_PAIR_BY_WSNAME[wsname] = rest_name
    except Exception as e:
        logger.error(f"Ошибка получения списка: {e}")
        return []

    candidates = []
    for kraken_name, info in all_pairs.items():
        wsname = info.get("wsname", "")
        if "/" not in wsname: continue
        base, quote = wsname.split("/")
        if quote != "USD": continue
        if any(x in base for x in EXCLUDE_BASE_SUBSTRINGS): continue
        if base in STABLECOINS: continue
        candidates.append(kraken_name)

    scored = []
    chunk_size = 50
    for i in range(0, len(candidates), chunk_size):
        chunk = candidates[i:i+chunk_size]
        try:
            tick_resp = requests.get(f"{BASE_URL}/Ticker", params={"pair": ",".join(chunk)}, timeout=20)
            tick_data = tick_resp.json()
            if tick_data.get("error"): continue
        except Exception as e:
            logger.warning(f"Ошибка тикеров: {e}"); continue

        for pair_name, t in tick_data.get("result", {}).items():
            try:
                high_24h = float(t.get("h", [0,0])[1])
                low_24h = float(t.get("l", [0,0])[1])
                vwap_24h = float(t.get("p", [0,0])[1])
                vol_24h = float(t.get("v", [0,0])[1])
                turnover = vwap_24h * vol_24h
                if turnover < MIN_TURNOVER_USD or low_24h <= 0: continue
                volatility_pct = (high_24h - low_24h) / low_24h * 100
                if volatility_pct < MIN_VOLATILITY_PCT: continue
                scored.append((pair_name, volatility_pct, turnover))
            except: continue
        
        time.sleep(0.5)

    scored.sort(key=lambda x: x[1], reverse=True)
    final_pairs = []
    for kraken_name, _, _ in scored[:max_pairs]:
        ws_name = pair_map.get(kraken_name)
        if ws_name: final_pairs.append(ws_name)
    return final_pairs

# ==================== ПОЛУЧЕНИЕ ВСЕХ ПАР (для Breakout) ====================
def get_all_available_pairs(max_pairs=CONSOLIDATION_PAIRS):
    try:
        pairs_resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        pairs_data = pairs_resp.json()
        if pairs_data.get("error"):
            logger.error(f"Kraken API error: {pairs_data['error']}")
            return []
        all_pairs = pairs_data.get("result", {})
        pair_map = {k: v.get("wsname") for k, v in all_pairs.items()}
    except Exception as e:
        logger.error(f"Ошибка получения списка: {e}")
        return []

    candidates = []
    for kraken_name, info in all_pairs.items():
        wsname = info.get("wsname", "")
        if "/" not in wsname: continue
        base, quote = wsname.split("/")
        if quote != "USD": continue
        if any(x in base for x in EXCLUDE_BASE_SUBSTRINGS): continue
        if base in STABLECOINS: continue
        candidates.append(kraken_name)

    # Берём первые N доступных пар (без фильтра волатильности)
    final_pairs = []
    for kraken_name in candidates[:max_pairs]:
        ws_name = pair_map.get(kraken_name)
        if ws_name: final_pairs.append(ws_name)
    return final_pairs

# ==================== ПОЛУЧЕНИЕ ТЕКУЩИХ ЦЕН ====================
def fetch_current_prices(pairs):
    prices = {}
    chunk_size = 50
    for i in range(0, len(pairs), chunk_size):
        chunk = pairs[i:i+chunk_size]
        rest_chunk = [REST_PAIR_BY_WSNAME.get(p, p.replace('/', '')) for p in chunk]
        try:
            tick_resp = requests.get(f"{BASE_URL}/Ticker", params={"pair": ",".join(rest_chunk)}, timeout=20)
            tick_data = tick_resp.json()
            if tick_data.get("error"): continue
            for rest_name, t in tick_data.get("result", {}).items():
                ws_name = None
                for k, v in REST_PAIR_BY_WSNAME.items():
                    if v == rest_name:
                        ws_name = k; break
                if ws_name:
                    prices[ws_name] = float(t.get("c", [0])[0])
        except Exception as e:
            logger.warning(f"Ошибка цен: {e}")
        time.sleep(0.1)
    return prices

# ==================== ИНДИКАТОРЫ ====================
def ema(series, length): return series.ewm(span=length, adjust=False).mean()
def macd(series, fast=12, slow=26, signal=9):
    macd_line = ema(series, fast) - ema(series, slow)
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line
def atr(df, length=14):
    high, low, close = df['high'], df['low'], df['close']
    prev_close = close.shift()
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(length).mean()
def rsi(series, length=RSI_LENGTH):
    delta = series.diff(); gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)
def adx(df, length=ADX_LENGTH):
    high, low, close = df['high'], df['low'], df['close']
    up_move = high.diff(); down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    prev_close = close.shift()
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr_w = tr.ewm(alpha=1/length, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/length, adjust=False).mean() / atr_w.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/length, adjust=False).mean() / atr_w.replace(0, np.nan)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    return dx.ewm(alpha=1/length, adjust=False).mean().fillna(0)

# ==================== АНАЛИЗ ====================
def analyze_timeframe(df, params):
    if df.empty or len(df) < params["min_bars"]: return None
    df = df.copy()
    df['ema_fast'] = ema(df['close'], params['ema_fast'])
    df['ema_slow'] = ema(df['close'], params['ema_slow'])
    df['macd_line'], df['macd_signal'] = macd(df['close'])
    df['atr'] = atr(df)
    df['rsi'] = rsi(df['close'])
    df['adx'] = adx(df)
    
    if len(df) < 3: return None
    last = df.iloc[-2]; prev = df.iloc[-3]
    if any(pd.isna([last['ema_fast'], last['ema_slow'], last['rsi'], last['adx'], last['macd_line'], last['macd_signal']])): 
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
        'atr': float(last['atr']) if not pd.isna(last['atr']) else 0.0,
        'macd_line': float(last['macd_line']),
        'macd_signal': float(last['macd_signal'])
    }

def detect_consolidation(df_daily):
    closed = df_daily.iloc[:-1]
    if len(closed) < 60:
        return None

    window = closed.tail(30)
    high = window['high'].max()
    low = window['low'].min()
    mean = window['close'].mean()

    if mean <= 0:
        return None

    range_pct = (high - low) / mean * 100
    adx_val = adx(closed).iloc[-1]

    if range_pct <= 15.0 and adx_val < 20:
        return {
            "days": len(window),
            "range_pct": round(range_pct, 2),
            "adx": round(float(adx_val), 2),
            "upper_level": high,
            "lower_level": low,
        }

    return None

def check_breakout(df_daily, current_price):
    if df_daily.empty or len(df_daily) < 60: return None
    closed = df_daily.iloc[:-1]
    if closed.empty or len(closed) < 60: return None
    window = closed.tail(30)
    if window.empty: return None
    high = window['high'].max(); low = window['low'].min(); mean = window['close'].mean()
    range_pct = (high - low) / mean * 100 if mean > 0 else 100
    if range_pct > 15.0: return None
    if current_price < high: return None
    vol_avg = closed['volume'].rolling(20).mean().iloc[-1]
    vol_ok = df_daily['volume'].iloc[-1] > vol_avg * 1.8
    if not vol_ok: return None
    adx_val = adx(closed).iloc[-1]
    atr_val = atr(closed).iloc[-1]
    stop = current_price - atr_val * ATR_MULT_SL
    target = current_price + atr_val * ATR_MULT_TP
    return {"close": current_price, "stop": stop, "target": target, "days": len(window)}

def check_exit(results, pos):
    r4h = results.get(TRIGGER_TF)
    if r4h is None: return False, ""
    if r4h['low'] <= pos['stop']:
        reason = "Трейлинг-стоп" if pos.get('trailing_active') else ("Безубыток" if pos.get('breakeven_moved') else "Stop-Loss")
        return True, reason
    if r4h['high'] >= pos['target']:
        return True, "Take-Profit"
    if r4h['ema_cross_down']: 
        return True, "Разворот (4h)"
    return False, ""

# ==================== TELEGRAM И СОСТОЯНИЕ ====================
def send_telegram(text):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
        result = r.json()
        if not result.get("ok"):
            logger.error(f"Telegram отклонил сообщение: {result.get('description')}")
        else:
            logger.info("Сообщение успешно отправлено в Telegram!")
    except Exception as e:
        logger.error(f"Ошибка отправки Telegram: {e}")

def load_state():
    with state_lock:
        try:
            with open(STATE_FILE, 'r') as f: return json.load(f)
        except: return {}

def save_state(state):
    with state_lock:
        try:
            tmp_file = STATE_FILE + ".tmp"
            with open(tmp_file, 'w') as f:
                json.dump(state, f, indent=2)
            os.replace(tmp_file, STATE_FILE)
        except IOError as e:
            logger.error(f"Ошибка сохранения: {e}")

def log_trade(symbol, entry, exit, reason, strategy):
    with state_lock:
        trades = []
        if os.path.exists(TRADES_LOG_FILE):
            try:
                with open(TRADES_LOG_FILE, 'r') as f: trades = json.load(f)
            except: trades = []
        pnl = (exit - entry) / entry * 100
        trades.append({
            "symbol": symbol, "entry_price": entry, "exit_price": exit,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "exit_time": datetime.now(timezone.utc).isoformat(),
            "pnl_pct": round(pnl, 2), "result": "win" if pnl > 0 else "loss",
            "reason": reason, "strategy": strategy
        })
        with open(TRADES_LOG_FILE, 'w') as f: json.dump(trades, f, indent=2)
        return round(pnl, 2)

# ==================== WEB SOCKET ====================
def on_open(ws):
    logger.info(f"WebSocket подключен. Подписываемся на {len(PAIRS_WS)} пар...")
    ws.send(json.dumps({"method": "subscribe", "params": {"channel": "ohlc", "symbol": PAIRS_WS, "interval": TIMEFRAME}}))

def on_message(ws, message):
    global state
    try:
        data = json.loads(message)
        if data.get("channel") != "ohlc" or data.get("type") != "update": return
        for item in data.get("data", []):
            symbol = item.get("symbol")
            if symbol not in ohlc_buffers: continue
            new_candle = {
                'start': item.get('time'), 'open': float(item.get('open')), 'high': float(item.get('high')),
                'low': float(item.get('low')), 'close': float(item.get('close')),
                'vwap': float(item.get('vwap')), 'volume': float(item.get('volume')), 'count': item.get('count')
            }
            if ohlc_buffers[symbol] and ohlc_buffers[symbol][-1]['start'] == new_candle['start']:
                ohlc_buffers[symbol][-1] = new_candle
            else:
                ohlc_buffers[symbol].append(new_candle)
            
            # Мгновенный контроль стопов/тейков
            with state_lock:
                pos = state.get(symbol)
                if pos and pos.get('position') == 'open':
                    if new_candle['low'] <= pos['stop']:
                        exit_price = pos['stop']
                        pnl_pct = log_trade(symbol, pos['entry_price'], exit_price, "Stop-Loss (WebSocket)", pos.get('strategy', 'unknown'))
                        send_telegram(f"🔴 <b>СТОП-ЛОСС (WebSocket)</b>\nПара: {symbol}\nЦена: {exit_price:.4f}\nРезультат: <b>{pnl_pct:+.2f}%</b>")
                        state[symbol] = {'position': 'closed'}; save_state(state)
                    elif new_candle['high'] >= pos['target']:
                        exit_price = pos['target']
                        pnl_pct = log_trade(symbol, pos['entry_price'], exit_price, "Take-Profit (WebSocket)", pos.get('strategy', 'unknown'))
                        send_telegram(f"🟢 <b>ТЕЙК-ПРОФИТ (WebSocket)</b>\nПара: {symbol}\nЦена: {exit_price:.4f}\nРезультат: <b>{pnl_pct:+.2f}%</b>")
                        state[symbol] = {'position': 'closed'}; save_state(state)

            # Анализ на вход
            if len(ohlc_buffers[symbol]) >= MIN_BARS:
                df = pd.DataFrame(list(ohlc_buffers[symbol]))
                df['ema_fast'] = ema(df['close'], 9); df['ema_slow'] = ema(df['close'], 21)
                df['macd_line'], df['macd_signal'] = macd(df['close']); df['atr'] = atr(df)
                df['rsi'] = rsi(df['close']); df['adx'] = adx(df)
                
                last = df.iloc[-2]; prev = df.iloc[-3]
                if pd.isna(last['ema_fast']) or pd.isna(last['rsi']): continue
                
                if (prev['macd_line'] <= prev['macd_signal'] and last['macd_line'] > last['macd_signal'] and
                    last['ema_fast'] > last['ema_slow'] and RSI_MIN <= last['rsi'] <= RSI_MAX and last['adx'] >= ADX_MIN):
                    close = last['close']; stop = close - last['atr'] * ATR_MULT_SL; target = close + last['atr'] * ATR_MULT_TP
                    
                    with state_lock:
                        if state.get(symbol, {}).get('position') != 'open':
                            send_telegram(f"🟢 <b>МГНОВЕННЫЙ ВХОД (WebSocket)</b>\nПара: {symbol}\nЦена: {close:.4f}\nSL: {stop:.4f}\nTP: {target:.4f}")
                            state[symbol] = {'position': 'open', 'entry_price': close, 'stop': stop, 'target': target, 'entry_time': datetime.now(timezone.utc).isoformat()}
                            save_state(state)
    except Exception as e:
        logger.error(f"Ошибка WebSocket: {e}")

def on_error(ws, error): logger.error(f"WS Ошибка: {error}")
def on_close(ws, code, msg):
    logger.warning(f"WS закрыт ({code}). Переподключение..."); time.sleep(5)

def run_websocket():
    ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    while True:
        try: ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e: logger.error(f"WS Критическая ошибка: {e}"); time.sleep(5)

# ==================== ФОНОВОЕ СКАНИРОВАНИЕ ====================
def fetch_klines(pair, interval, min_bars):
    pair_name = REST_PAIR_BY_WSNAME.get(pair, pair.replace('/', ''))
    try:
        resp = requests.get(f"{BASE_URL}/OHLC", params={"pair": pair_name, "interval": interval}, timeout=20)
        data = resp.json()
        if data.get('error'):
            logger.warning(f"Kraken вернул ошибку для {pair} ({interval}m): {data['error']}")
            return []
        result = data['result']; key = [k for k in result.keys() if k != 'last'][0]
        rows = result[key]
        df = pd.DataFrame(rows, columns=['start','open','high','low','close','vwap','volume','count'])
        for col in ['open','high','low','close','volume']: df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)
        return df.tail(min_bars).to_dict('records')
    except requests.exceptions.RequestException as e:
        logger.warning(f"Сетевая ошибка для {pair} ({interval}m): {e}")
        return []
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Ошибка формата данных для {pair} ({interval}m): {e}")
        return []
    except Exception as e:
        logger.error(f"Неизвестная ошибка для {pair} ({interval}m): {e}")
        return []

def background_scan_loop():
    global state
    while True:
        logger.info("=== Запуск фонового сканирования ===")
        
        # 1. Получаем волатильные пары для Confluence и WebSocket
        volatile_pairs = get_filtered_pairs(TOP_N)
        # 2. Получаем все пары для поиска боковиков (без фильтра волатильности)
        all_pairs_for_consolidation = get_all_available_pairs(CONSOLIDATION_PAIRS)

        if not volatile_pairs:
            logger.error("Не удалось получить волатильные пары!"); time.sleep(SCAN_INTERVAL_SECONDS); continue
        
        current_prices = fetch_current_prices(volatile_pairs + all_pairs_for_consolidation)
        found_buy, found_sell = 0, 0
        scan_summary = []
        consolidation_list = []
        now_iso = datetime.now(timezone.utc).isoformat()

        # === ЦИКЛ 1: Confluence (Волатильные пары) ===
        for idx, pair in enumerate(volatile_pairs):
            try:
                df_daily = pd.DataFrame(fetch_klines(pair, 1440, 100))
                if df_daily.empty:
                    logger.info(f"Пропуск {pair}: нет данных")
                
                results = {}
                for tf in TIMEFRAME_ORDER:
                    params = TIMEFRAME_PARAMS[tf]
                    df_tf = pd.DataFrame(fetch_klines(pair, params['kraken_interval'], params['min_bars']))
                    if df_tf.empty: logger.debug(f"[{pair}] Нет данных по {tf}")
                    results[tf] = analyze_timeframe(df_tf, params)
                    time.sleep(0.3)

                if all(results.get(tf) is not None for tf in TIMEFRAME_ORDER):
                    trend_score = sum(1 for tf in TIMEFRAME_ORDER if results[tf]['trend_up'])
                    r4h = results['4h']
                    macd_gap_pct = (r4h['macd_line'] - r4h['macd_signal']) / r4h['close'] * 100 if r4h['close'] else 0.0
                    
                    scan_summary.append({
                        "pair": pair, "trend_score": trend_score, "rsi_4h": r4h['rsi'],
                        "adx_1d": results['1d']['adx'], "macd_gap_pct": macd_gap_pct, "close_price": r4h['close']
                    })

                # Проверяем открытые позиции на выход
                pos = state.get(pair)
                if pos and pos.get('position') == 'open' and results.get(TRIGGER_TF) and results.get('1d'):
                    with state_lock:
                        if pos.get('entry_price') and results[TRIGGER_TF]['close'] >= pos['entry_price'] + (BREAKEVEN_TRIGGER_ATR * results['1d']['atr']) and not pos.get('breakeven_moved'):
                            pos['stop'] = pos['entry_price'] * 1.001; pos['breakeven_moved'] = True
                        
                        if pos.get('breakeven_moved') and results['1d']['atr'] > 0:
                            new_stop = results[TRIGGER_TF]['close'] - results['1d']['atr'] * TRAILING_ATR_MULT
                            if new_stop > pos['stop']: pos['stop'] = new_stop; pos['trailing_active'] = True
                        
                        exit_now, reason = check_exit(results, pos)
                        if exit_now:
                            exit_price = results[TRIGGER_TF]['close']
                            now_iso = datetime.now(timezone.utc).isoformat()
                            pnl_pct = log_trade(pair, pos['entry_price'], exit_price, reason, pos.get('strategy', 'unknown'))
                            send_telegram(f"🔴 <b>ВЫХОД</b>\nПара: {pair}\nЦена: {exit_price:.4f}\nПричина: {reason}\nРезультат: <b>{pnl_pct:+.2f}%</b>")
                            state[pair] = {'position': 'closed'}; save_state(state); found_sell += 1
            except Exception as e:
                logger.error(f"Ошибка в {pair}: {e}")
                continue

        # === ЦИКЛ 2: Breakout (Все пары, включая спокойные) ===
        for idx, pair in enumerate(all_pairs_for_consolidation):
            try:
                df_daily = pd.DataFrame(fetch_klines(pair, 1440, 100))
                if df_daily.empty: continue

                # Проверяем боковик
                if not df_daily.empty and len(df_daily) > 30:
                    cons = detect_consolidation(df_daily)
                    if cons:
                        consolidation_list.append({
                            "pair": pair,
                            "days": cons['days'],
                            "range_pct": cons['range_pct'],
                            "adx": cons['adx'],
                            "breakout_level": cons['upper_level']
                        })

                    # Проверяем пробой
                    current_price = current_prices.get(pair, df_daily['close'].iloc[-1])
                    breakout = check_breakout(df_daily, current_price)
                    
                    if breakout:
                        now_iso = datetime.now(timezone.utc).isoformat()
                        with state_lock:
                            if state.get(pair, {}).get('position') != 'open':
                                send_telegram(f"📦 <b>ПРОБОЙ БОКОВИКА (Breakout)</b>\nПара: {pair}\nЦена: {current_price:.4f}\nSL: {breakout['stop']:.4f}\nTP: {breakout['target']:.4f}\nДней в боковике: {breakout['days']}")
                                state[pair] = {'position': 'open', 'entry_price': current_price, 'stop': breakout['stop'], 'target': breakout['target'], 'entry_time': now_iso, 'strategy': 'breakout'}
                                save_state(state); found_buy += 1
            except Exception as e:
                logger.error(f"Ошибка в {pair}: {e}")
                continue

        # === ОТПРАВКА СТАТУСА ===
        open_pos = sum(1 for p in state.values() if p.get('position') == 'open')
        now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
        
        lines = [
            f"📡 <b>Статус сканирования</b> — {now_str}",
            f"━━━━━━━━━━━━━━━━━━━━━",
            f"📊 <b>Общая статистика:</b>",
            f"• Отслеживается пар (Confluence): {len(volatile_pairs)}",
            f"• Дополнительно проверено на боковик: {len(all_pairs_for_consolidation)}",
            f"• Всего в боковике найдено: {len(consolidation_list)}",
            f"• Открытых позиций: {open_pos}",
            f"• Входов за цикл: {found_buy}",
            f"• Выходов за цикл: {found_sell}",
        ]

        if open_pos > 0:
            lines.append(f"\n💰 <b>Открытые позиции:</b>")
            for pair, pos in state.items():
                if pos.get('position') == 'open':
                    entry_time = str(pos.get('entry_time', '?')).replace("T", " ")[:16]
                    lines.append(f"• <b>{pair}</b> | Вход: {pos.get('entry_price', 0):.6g} | Время: {entry_time} | SL: {pos.get('stop', 0):.6g} | TP: {pos.get('target', 0):.6g}")
        else:
            lines.append(f"\n💰 <b>Открытых позиций нет.</b>")

        close_calls = [s for s in scan_summary if s["trend_score"] >= 2]
        close_calls.sort(key=lambda s: s.get("macd_gap_pct", 999))

        if close_calls:
            lines.append(f"\n🎯 <b>Топ кандидатов на вход (Confluence):</b>")
            for i, s in enumerate(close_calls[:3], 1):
                gap = s.get("macd_gap_pct", 0)
                if gap < 0: proximity = "⏳ Близко к кроссу (ждём)"
                elif gap < 0.5: proximity = "🟡 Кросс недавно, ещё актуально"
                else: proximity = "⚠️ Кросс был давно, вход маловероятен скоро"
                
                lines.append(f"{i}. <b>{s['pair']}</b>\n   Тренд: {s['trend_score']}/3 | ADX: {s['adx_1d']:.0f} | RSI(4h): {s['rsi_4h']:.0f}\n   Ориентир входа (тек. цена): ~{s['close_price']:.6g}\n   {proximity}")
        else:
            lines.append(f"\n😴 <b>Кандидатов на вход (Confluence) нет</b>")

        if consolidation_list:
            lines.append(f"\n📦 <b>Монеты в длительном боковике (> 30 дней):</b>")
            consolidation_list.sort(key=lambda x: x["days"], reverse=True)
            for i, item in enumerate(consolidation_list, 1):
                lines.append(f"{i}. <b>{item['pair']}</b> – {item['days']} дн. | Диапазон: {item['range_pct']:.1f}% | ADX: {item['adx']:.0f} | Пробой выше: {item['breakout_level']:.6g}")
        else:
            lines.append(f"\n📦 <b>Монет в длительном боковике не найдено.</b>")

        lines.append(f"\n━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"🔄 Следующее статусное сообщение через 2 ч.")
        
        send_telegram("\n".join(lines))
        logger.info(f"Цикл завершен. Входов: {found_buy}, Выходов: {found_sell}")
        time.sleep(SCAN_INTERVAL_SECONDS)

# ==================== MAIN ====================
def main():
    global PAIRS_WS, PAIRS_VOLATILE, PAIRS_ALL, ohlc_buffers, state
    logger.info("Инициализация универсального бота (VPS)...")
    build_asset_pairs()
    state = load_state()

    logger.info("Запрос списков пар...")
    PAIRS_VOLATILE = []
    while not PAIRS_VOLATILE:
        PAIRS_VOLATILE = get_filtered_pairs(TOP_N)
        if not PAIRS_VOLATILE:
            logger.error("Не удалось получить волатильные пары! Жду 5 минут и пробую снова...")
            time.sleep(300)
    
    PAIRS_WS = PAIRS_VOLATILE
    logger.info(f"Топ-200 для WebSocket: {len(PAIRS_WS)} пар")

    ohlc_buffers = {pair: deque(maxlen=100) for pair in PAIRS_WS}
    
    logger.info("Загрузка истории для WebSocket...")
    for pair in PAIRS_WS:
        history = fetch_klines(pair, TIMEFRAME, 80)
        if history:
            ohlc_buffers[pair].extend(history)
        time.sleep(0.1)

    logger.info("Запуск фонового сканера (каждые 2 часа)...")
    scanner_thread = threading.Thread(target=background_scan_loop, daemon=True)
    scanner_thread.start()

    logger.info("Запуск WebSocket в реальном времени...")
    run_websocket()

if __name__ == "__main__":
    main()
