#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Заменяет блок footer с любым отступом на правильно отформатированный."""
import shutil
import sys
import py_compile

F = "/opt/bybit-scanner/bybit_scanner_v17.py"
shutil.copy(F, F + ".before_fix2")

with open(F, "r", encoding="utf-8") as fh:
    lines = fh.readlines()

# Найти начало: строка, у которой .strip() == "footer = ["
start = None
for i, ln in enumerate(lines):
    if ln.strip() == "footer = [":
        start = i
        break
if start is None:
    print("❌ Не нашёл 'footer = ['")
    sys.exit(1)

# Найти конец: следующая строка, у которой .strip() == "]"
end = None
for j in range(start + 1, len(lines)):
    if lines[j].strip() == "]":
        end = j
        break
if end is None:
    print("❌ Не нашёл закрывающую ']'")
    sys.exit(1)

NEW_BLOCK = (
    '    footer = [\n'
    '        "━━━━━━━━━━━━━━━━━━━━━",\n'
    '        f"🔄 Следующий статус через 2 ч · лимиты {MAX_OPEN_POSITIONS} поз / {MAX_TRADES_PER_HOUR} в час",\n'
    '        f"⚡ WS-tickers: {WS_TICKERS_QUOTA_CAND}c + {WS_TICKERS_QUOTA_CONS}cons + {WS_TICKERS_QUOTA_DIP}dip + WT",\n'
    '        f"🎯 Пороги: MACD≤{WS_HOT_NEAR_CROSS_PCT}% · пробой≤{WS_HOT_NEAR_BREAKOUT_PCT}%",\n'
    '        "🐂1D-bull · 💎RSI-dip · 🌊WT-dip · 💥SQZ · 🎯BOTTOM · 🛡PEAK · 🔧BTC-обход",\n'
    '        "🔥≤1% ⚡≤3% 🟢≤5% до пробоя · ⛔ пик · ⚠️ дрейф · 🥀 объём↓",\n'
    '    ]\n'
)

# Заменяем строки start..end включительно
lines[start:end + 1] = [NEW_BLOCK]

with open(F, "w", encoding="utf-8") as fh:
    fh.writelines(lines)

print(f"✅ Заменил строки {start + 1}..{end + 1}")

try:
    py_compile.compile(F, doraise=True)
    print("SYNTAX_OK")
except py_compile.PyCompileError as e:
    print("❌ СИНТАКСИЧЕСКАЯ ОШИБКА:", e)
    shutil.copy(F + ".before_fix2", F)
    print("Файл восстановлен из бэкапа.")
    sys.exit(1)

print("Готово. Бэкап:", F + ".before_fix2")
