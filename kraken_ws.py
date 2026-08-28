#!/usr/bin/env python3
"""
Настоящий WebSocket-скрипт для Kraken v2.
Постоянно слушает биржу, обновляет свечи в реальном времени
и проверяет условия входа/выхода.
"""

import json
import os
import time
import logging
import requests
import pandas as pd
import numpy as np
import websocket
from collections import deque
from datetime import datetime, timezone

# ==================== КОНФИГУРАЦИЯ ====================
# Безопасное хранение токена через переменные окружения (для Git)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8884457853:AAHXfn5ZxGDyyaaNeUNcdcbt30f7r9JQmtZC")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "762494040")
BASE_URL = "https://api.kraken.com/0/public"
WS_URL = "wss://ws.kraken.com/v2"

# Список пар для мониторинга (заменили MATIC на POL)
PAIRS = [
    "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "ADA/USD",
    "DOT/USD", "LINK/USD", "UNI/USD", "POL/USD", "AVAX/USD"
]

TIMEFRAME = 15  # 15 минут
MIN_BARS = 80
RSI_LENGTH = 14
ADX_LENGTH = 14
RSI_MIN, RSI_MAX = 40, 75
ADX_MIN = 20
ATR_MULT_SL, ATR_MULT_TP = 2.0, 4.0
BREAKEVEN_TRIGGER_ATR = 1.0
TRAILING_ATR_MULT = 1.5
BREAKEVEN_BUFFER_PCT = 0.1

STATE_FILE = "/opt/kraken-scanner/kraken_ws_state.json"
TRADES_LOG = "/opt/kraken-scanner/trades_log.json"

# Буфер для хранения свечей (последние 100 свечей на каждую пару)
ohlc_buffers = {pair: deque(maxlen=100) for pair in PAIRS}
state = {}

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==================== ИНДИКАТОРЫ ====================
def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def macd(series, fast=12, slow=26, signal=9):
    macd_line = ema(series, fast) - ema(series, slow)
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line

def atr(df, length=14):
    high, low, close = df['high'], df['low'], df['close']
    prev_close = close.shift()
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(length).mean()

def rsi(series, length=RSI_LENGTH):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)

def adx(df, length=ADX_LENGTH):
    high, low, close = df['high'], df['low'], df['close']
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    prev_close = close.shift()
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr_w = tr.ewm(alpha=1/length, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/length, adjust=False).mean() / atr_w.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/length, adjust=False).mean() / atr_w.replace(0, np.nan)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    return dx.ewm(alpha=1/length, adjust=False).mean().fillna(0)

# ==================== АНАЛИЗ (на последнем закрытом баре) ====================
def analyze_timeframe(df):
    if df.empty or len(df) < MIN_BARS:
        return None

    df = df.copy()
    df['ema_fast'] = ema(df['close'], 9)
    df['ema_slow'] = ema(df['close'], 21)
    df['macd_line'], df['macd_signal'] = macd(df['close'])
    df['atr'] = atr(df)
    df['rsi'] = rsi(df['close'])
    df['adx'] = adx(df)

    # Берем последний ЗАКРЫТЫЙ бар ([-2] - предпоследний, так как [-1] еще формируется)
    last = df.iloc[-2]
    prev = df.iloc[-3]
    if any(pd.isna([last['ema_fast'], last['ema_slow'], last['rsi'], last['adx']])):
        return None

    trend_up = bool(last['ema_fast'] > last['ema_slow'])
    macd_cross_up = bool(prev['macd_line'] <= prev['macd_signal'] and last['macd_line'] > last['macd_signal'])
    bos = False

    return {
        'trend_up': trend_up,
        'macd_cross_up': macd_cross_up,
        'bos_up': bos,
        'rsi': float(last['rsi']),
        'adx': float(last['adx']),
        'close': float(last['close']),
        'atr': float(last['atr']) if not pd.isna(last['atr']) else 0.0
    }

# ==================== TELEGRAM ====================
def send_telegram(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        logger.error(f"Ошибка отправки Telegram: {e}")

# ==================== СОСТОЯНИЕ ====================
def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ==================== ЗАГРУЗКА ИСТОРИИ (для старта буфера) ====================
def fetch_klines(pair):
    pair_name = pair.replace('/', '')
    try:
        resp = requests.get(f"{BASE_URL}/OHLC", params={"pair": pair_name, "interval": TIMEFRAME}, timeout=20)
        data = resp.json()
        if data.get('error'):
            logger.error(f"Ошибка OHLC для {pair}: {data['error']}")
            return []
        result = data['result']
        key = [k for k in result.keys() if k != 'last'][0]
        rows = result[key]
        df = pd.DataFrame(rows, columns=['start','open','high','low','close','vwap','volume','count'])
        for col in ['open','high','low','close','volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)
        return df.tail(100).to_dict('records')
    except Exception as e:
        logger.error(f"Ошибка получения данных для {pair}: {e}")
        return []

# ==================== ОБРАБОТЧИКИ WEBSOCKET ====================
def on_open(ws):
    logger.info("Соединение с WebSocket Kraken установлено. Подписываемся на свечи 15m...")
    subscribe_msg = {
        "method": "subscribe",
        "params": {
            "channel": "ohlc",
            "symbol": PAIRS,
            "interval": TIMEFRAME
        }
    }
    ws.send(json.dumps(subscribe_msg))
    logger.info(f"Подписка отправлена для {len(PAIRS)} пар")

def on_message(ws, message):
    global state
    try:
        data = json.loads(message)
        
        # Игнорируем системные сообщения (каналы heartbeat, status)
        if data.get("channel") != "ohlc" or data.get("type") != "update":
            return
        
        for item in data.get("data", []):
            symbol = item.get("symbol")
            if symbol not in ohlc_buffers:
                continue
            
            # Формат свечи: [time, open, high, low, close, vwap, volume, count]
            # В WebSocket Kraken в начале идет времени, затем числа
            new_candle = {
                'start': item[0],
                'open': float(item[1]),
                'high': float(item[2]),
                'low': float(item[3]),
                'close': float(item[4]),
                'vwap': float(item[5]),
                'volume': float(item[6]),
                'count': item[7]
            }
            
            # Проверяем, новая ли это свеча
            if ohlc_buffers[symbol] and ohlc_buffers[symbol][-1]['start'] == new_candle['start']:
                ohlc_buffers[symbol][-1] = new_candle # Обновляем текущую формирующуюся свечу
            else:
                ohlc_buffers[symbol].append(new_candle) # Добавляем новую свечу
            
            # Если свечей больше 80, можно проводить анализ (на закрытых барах)
            if len(ohlc_buffers[symbol]) >= MIN_BARS:
                df = pd.DataFrame(list(ohlc_buffers[symbol]))
                res = analyze_timeframe(df)
                
                if res:
                    # Проверяем условия для входа/выхода (копия логики из прошлого скрипта)
                    if (res['macd_cross_up'] and res['trend_up'] and
                        RSI_MIN <= res['rsi'] <= RSI_MAX and res['adx'] >= ADX_MIN):
                        
                        close = res['close']
                        stop = close - res['atr'] * ATR_MULT_SL
                        target = close + res['atr'] * ATR_MULT_TP

                        if state.get(symbol, {}).get('position') != 'open':
                            msg = (
                                f"🟢 <b>ВХОД BUY (WebSocket)</b>\n"
                                f"Пара: {symbol}\n"
                                f"Цена: {close:.4f}\n"
                                f"SL: {stop:.4f}\n"
                                f"TP: {target:.4f}\n"
                                f"RSI: {res['rsi']:.1f} | ADX: {res['adx']:.1f}"
                            )
                            send_telegram(msg)
                            state[symbol] = {
                                'position': 'open',
                                'entry_price': close,
                                'stop': stop,
                                'target': target,
                                'entry_time': datetime.now(timezone.utc).isoformat()
                            }
                            save_state(state)
                        else:
                            logger.info(f"Уже в позиции по {symbol}, пропускаем")
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")

def on_error(ws, error):
    logger.error(f"Ошибка WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WebSocket закрыт. Код: {close_status_code}, Причина: {close_msg}. Переподключение через 5 сек...")
    time.sleep(5)

# ==================== ЗАПУСК ====================
def main():
    global state
    logger.info("Запуск НАСТОЯЩЕГО WebSocket-бота Kraken v2")
    state = load_state()

    # Заранее подгружаем историю, чтобы индикаторы посчитались сразу
    logger.info("Загрузка истории свечей для инициализации буферов...")
    for pair in PAIRS:
        history = fetch_klines(pair)
        if history:
            ohlc_buffers[pair].extend(history)
            logger.info(f"Загружено {len(history)} свечей для {pair}")
        time.sleep(0.3) # Небольшая пауза, чтобы не забанили

    # Запускаем бесконечное соединение
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # run_forever автоматически переподключается
    while True:
        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error(f"Критическая ошибка цикла: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
