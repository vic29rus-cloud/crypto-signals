#!/usr/bin/env python3
"""
УНИВЕРСАЛЬНЫЙ БОТ: WebSocket + REST сканер (VPS)
1. WebSocket слушает ТОП-200 пар в реальном времени (вход по MACD на 15м).
2. Фоновый поток каждые 2 часа сканирует 700 пар на боковики (Breakout > 30 дней),
   проверяет тренды (Confluence), открытые позиции и отправляет статус в Telegram.
Работает полностью автономно без GitHub Actions.
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

# ==================== КОНФИГУРАЦИЯ (МОЖНО МЕНЯТЬ) ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8884457853:AAHXfn5ZxGDyyaaNeUNcdcbt30f7r9JQmtZC")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "762494040")
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

# *** НАСТРОЙКИ КОЛИЧЕСТВА ПАР ***
TOP_N = 200          # Пар в WebSocket (мгновенные сигналы)
TOTAL_PAIRS = 700    # Всего пар для фонового сканирования (включая боковики)
BREAKOUT_PAIRS = 500 # Дополнительно пар для боковиков (700 - 200)

# *** НАСТРОЙКИ ПЕРИОДОВ ***
STATUS_INTERVAL_MINUTES = 120   # Статус в Telegram каждые 2 часа
SCAN_INTERVAL_SECONDS = 7200    # Полное сканирование каждые 2 часа (2 * 60 * 60)

STATE_FILE = "/opt/kraken-scanner/kraken_ws_state.json"
TRADES_LOG_FILE = "/opt/kraken-scanner/trades_log.json"
QUALIFIED_PAIRS_FILE = "/opt/kraken-scanner/qualified_pairs.json"

# Параметры таймфреймов для фонового анализа
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

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
PAIRS_WS = []               # Топ-200 для WebSocket
PAIRS_ALL = []              # Все 700 пар
ohlc_buffers = {}           # Буферы для WebSocket
state = {}                  # Открытые позиции

# ==================== ФУНКЦИИ ПОЛУЧЕНИЯ СПИСКА ПАР ====================
def get_all_filtered_pairs(max_pairs=TOTAL_PAIRS):
    """Получает список всех пар, фильтрует по ликвидности и волатильности."""
    try:
        pairs_resp = requests.get(f"{BASE_URL}/AssetPairs", timeout=20)
        pairs_data = pairs_resp.json()
        if pairs_data.get("error"):
            logger.error(f"Kraken API error: {pairs_data['error']}")
            return []
        all_pairs = pairs_data.get("result", {})
        pair_map = {k: v.get("wsname") for k, v in all_pairs.items()}
    except Exception as e:
        logger.error(f"Ошибка получения списка пар: {e}")
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
        time.sleep(0.2)

    scored.sort(key=lambda x: x[1], reverse=True)
    final_pairs = []
    for kraken_name, _, _ in scored[:max_pairs]:
        ws_name = pair_map.get(kraken_name)
        if ws_name: final_pairs.append(ws_name)
    return final_pairs

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

# ==================== ФУНКЦИИ АНАЛИЗА ====================
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
    if any(pd.isna([last['ema_fast'], last['ema_slow'], last['rsi'], last['adx']])): return None
    
    return {
        'trend_up': bool(last['ema_fast'] > last['ema_slow']),
        'macd_cross_up': bool(prev['macd_line'] <= prev['macd_signal'] and last['macd_line'] > last['macd_signal']),
        'rsi': float(last['rsi']),
        'adx': float(last['adx']),
        'close': float(last['close']),
        'atr': float(last['atr']) if not pd.isna(last['atr']) else 0.0
    }

def check_confluence(results, entry_results, df15):
    if any(results.get(tf) is None for tf in TIMEFRAME_ORDER): return False, "Нет данных", None
    if not all(results[tf]['trend_up'] for tf in TIMEFRAME_ORDER): return False, "Тренд не совпал", None
    trigger = entry_results.get('15m')
    if trigger is None or not trigger['macd_cross_up']: return False, "Нет триггера 15m", None
    if not (RSI_MIN <= trigger['rsi'] <= RSI_MAX): return False, f"RSI {trigger['rsi']:.1f}", None
    if results['1d']['adx'] < ADX_MIN: return False, "ADX низкий", None
    if df15.empty or df15['volume'].iloc[-1] < df15['volume'].rolling(20).mean().iloc[-1] * 1.5: return False, "Мал объем", None
    
    close = trigger['close']; daily_atr = results['1d']['atr']
    stop = close - daily_atr * ATR_MULT_SL
    target = close + daily_atr * ATR_MULT_TP
    return True, "Confluence", {"close": close, "stop": stop, "target": target, "rsi": trigger['rsi'], "adx": results['1d']['adx']}

def check_breakout(df_daily, current_price):
    if df_daily.empty or len(df_daily) < 60: return None
    hist = df_daily.iloc[:-1]
    window = hist.tail(30)
    if window.empty: return None
    
    high = window['high'].max(); low = window['low'].min(); mean = window['close'].mean()
    range_pct = (high - low) / mean * 100 if mean > 0 else 100
    if range_pct > 15.0: return None  # слишком широкий диапазон - не боковик
    
    if current_price < high: return None  # не пробил уровень
    
    vol_ok = df_daily['volume'].iloc[-1] > df_daily['volume'].rolling(20).mean().iloc[-1] * 1.8
    if not vol_ok: return None
    
    adx_val = adx(df_daily).iloc[-1]
    if adx_val >= 20: return None
    
    stop = current_price - atr(df_daily).iloc[-1] * ATR_MULT_SL
    target = current_price + atr(df_daily).iloc[-1] * ATR_MULT_TP
    return {"close": current_price, "stop": stop, "target": target, "days": len(window)}

def check_exit(results, pos):
    r4h = results.get(TRIGGER_TF)
    if r4h is None: return False, ""
    if r4h['close'] <= pos['stop']:
        reason = "Трейлинг-стоп" if pos.get('trailing_active') else ("Безубыток" if pos.get('breakeven_moved') else "Stop-Loss")
        return True, reason
    if r4h['close'] >= pos['target']: return True, "Take-Profit"
    if r4h['macd_cross_up'] is False and r4h.get('ema_cross_down', False): return True, "Разворот"
    return False, ""

# ==================== TELEGRAM И СОСТОЯНИЕ ====================
def send_telegram(text):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        logger.error(f"Ошибка Telegram: {e}")

def load_state():
    try:
        with open(STATE_FILE, 'r') as f: return json.load(f)
    except: return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f: json.dump(state, f, indent=2)

def log_trade(symbol, entry, exit, reason, strategy):
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

# ==================== ФОНОВОЕ СКАНИРОВАНИЕ (2 ЧАСА) ====================
def fetch_klines(pair, interval, min_bars):
    pair_name = pair.replace('/', '')
    try:
        resp = requests.get(f"{BASE_URL}/OHLC", params={"pair": pair_name, "interval": interval}, timeout=20)
        data = resp.json()
        if data.get('error'): return []
        result = data['result']; key = [k for k in result.keys() if k != 'last'][0]
        rows = result[key]
        df = pd.DataFrame(rows, columns=['start','open','high','low','close','vwap','volume','count'])
        for col in ['open','high','low','close','volume']: df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)
        return df.tail(min_bars).to_dict('records')
    except: return []

def background_scan_loop():
    """Главный цикл, который работает каждые 2 часа."""
    global state
    while True:
        logger.info("=== Запуск фонового сканирования (700 пар) ===")
        all_pairs = get_all_filtered_pairs(TOTAL_PAIRS)
        if not all_pairs:
            logger.error("Не удалось получить пары!"); time.sleep(SCAN_INTERVAL_SECONDS); continue
        
        found_buy, found_sell = 0, 0
        scan_summary = []
        consolidation_list = []
        now_iso = datetime.now(timezone.utc).isoformat()

        # 1. Проверяем боковики и тренды для всех пар
        for idx, pair in enumerate(all_pairs):
            try:
                # 1.1 Берем дневные данные для боковика
                df_daily = pd.DataFrame(fetch_klines(pair, 1440, 100))
                
                # 1.2 Берем трендовые таймфреймы
                results = {}
                for tf in TIMEFRAME_ORDER:
                    params = TIMEFRAME_PARAMS[tf]
                    df_tf = pd.DataFrame(fetch_klines(pair, params['kraken_interval'], params['min_bars']))
                    results[tf] = analyze_timeframe(df_tf, params)
                    time.sleep(0.1) # защита от банов
                
                # 1.3 Поиск боковика (если есть дневные данные)
                if not df_daily.empty and len(df_daily) > 30:
                    close_price = df_daily['close'].iloc[-1]
                    breakout = check_breakout(df_daily, close_price)
                    if breakout:
                        consolidation_list.append({"pair": pair, "days": breakout['days']})
                        # Отправляем сигнал на пробой!
                        if state.get(pair, {}).get('position') != 'open':
                            send_telegram(f"📦 <b>ПРОБОЙ БОКОВИКА (Breakout)</b>\nПара: {pair}\nЦена: {close_price:.4f}\nSL: {breakout['stop']:.4f}\nTP: {breakout['target']:.4f}\nДней в боковике: {breakout['days']}")
                            state[pair] = {'position': 'open', 'entry_price': close_price, 'stop': breakout['stop'], 'target': breakout['target'], 'entry_time': now_iso, 'strategy': 'breakout'}
                            save_state(state); found_buy += 1

                # 1.4 Проверяем открытую позицию на выход (если есть)
                pos = state.get(pair)
                if pos and pos.get('position') == 'open' and results.get(TRIGGER_TF) and results.get('1d'):
                    # Переводим в безубыток
                    if pos.get('entry_price') and results[TRIGGER_TF]['close'] > pos['entry_price'] * 1.02 and not pos.get('breakeven_moved'):
                        pos['stop'] = pos['entry_price'] * 1.001; pos['breakeven_moved'] = True
                    
                    # Трейлинг
                    if pos.get('breakeven_moved') and results['1d']['atr'] > 0:
                        new_stop = results[TRIGGER_TF]['close'] - results['1d']['atr'] * TRAILING_ATR_MULT
                        if new_stop > pos['stop']: pos['stop'] = new_stop; pos['trailing_active'] = True
                    
                    # Выход
                    exit_now, reason = check_exit(results, pos)
                    if exit_now:
                        exit_price = results[TRIGGER_TF]['close']
                        log_trade(pair, pos['entry_price'], exit_price, reason, pos.get('strategy', 'unknown'))
                        send_telegram(f"🔴 <b>ВЫХОД</b>\nПара: {pair}\nЦена: {exit_price:.4f}\nПричина: {reason}")
                        state[pair] = {'position': 'closed'}; save_state(state); found_sell += 1

                # 1.5 Формируем статистику для отчета
                if all(results.get(tf) is not None for tf in TIMEFRAME_ORDER):
                    scan_summary.append({"pair": pair, "trend_score": sum(1 for tf in TIMEFRAME_ORDER if results[tf]['trend_up'])})
            except Exception as e:
                logger.error(f"Ошибка в {pair}: {e}")
                continue

        # 2. Отправляем статус каждые 2 часа
        open_pos = sum(1 for p in state.values() if p.get('position') == 'open')
        text = (f"📊 <b>Статус сканирования</b>\n"
                f"Всего пар: {len(all_pairs)}\n"
                f"Найдено боковиков: {len(consolidation_list)}\n"
                f"Открытых позиций: {open_pos}\n"
                f"Входов за цикл: {found_buy}\n"
                f"Выходов за цикл: {found_sell}")
        send_telegram(text)
        
        qualified = [s['pair'] for s in scan_summary if s['trend_score'] == 3]
        with open(QUALIFIED_PAIRS_FILE, 'w') as f:
            json.dump({"time": datetime.now(timezone.utc).isoformat(), "pairs": qualified}, f)
        
        logger.info(f"Цикл завершен. Входов: {found_buy}, Выходов: {found_sell}")
        time.sleep(SCAN_INTERVAL_SECONDS)

# ==================== MAIN (ЗАПУСК) ====================
def main():
    global PAIRS_WS, PAIRS_ALL, ohlc_buffers, state
    logger.info("Инициализация универсального бота (VPS)...")
    state = load_state()

    # Получаем 700 пар
    logger.info(f"Запрос списка {TOTAL_PAIRS} пар...")
    PAIRS_ALL = get_all_filtered_pairs(TOTAL_PAIRS)
    if not PAIRS_ALL:
        logger.error("Не удалось получить пары. Проверьте сеть или токен API.")
        return
    
    # Делим на ТОП-200 для WebSocket и остальные
    PAIRS_WS = PAIRS_ALL[:TOP_N]
    logger.info(f"Топ-200 для WebSocket: {len(PAIRS_WS)} пар")

    # Инициализируем буферы для WebSocket
    ohlc_buffers = {pair: deque(maxlen=100) for pair in PAIRS_WS}
    
    # Загружаем историю для WebSocket (иначе индикаторы не будут работать сразу)
    logger.info("Загрузка истории для WebSocket...")
    for pair in PAIRS_WS:
        history = fetch_klines(pair, TIMEFRAME, 80)
        if history:
            ohlc_buffers[pair].extend(history)
        time.sleep(0.1)

    # Запускаем фоновый сканер в отдельном потоке (каждые 2 часа)
    logger.info("Запуск фонового сканера (700 пар, каждые 2 часа)...")
    scanner_thread = threading.Thread(target=background_scan_loop, daemon=True)
    scanner_thread.start()

    # Запускаем WebSocket в основном потоке (бесконечно)
    logger.info("Запуск WebSocket в реальном времени...")
    run_websocket()

if __name__ == "__main__":
    main()
