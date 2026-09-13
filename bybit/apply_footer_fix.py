#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Автозамена footer-блока в bybit_scanner_v17.py. Идемпотентно."""
import re
import sys
import shutil
import py_compile

F = "/opt/bybit-scanner/bybit_scanner_v17.py"

NEW_FOOTER = '''    footer = [
        "━━━━━━━━━━━━━━━━━━━━━",
        f"🔄 Следующий статус через 2 ч · лимиты {MAX_OPEN_POSITIONS} поз / {MAX_TRADES_PER_HOUR} в час",
        f"⚡ WS-tickers: {WS_TICKERS_QUOTA_CAND}c + {WS_TICKERS_QUOTA_CONS}cons + {WS_TICKERS_QUOTA_DIP}dip + WT",
        f"🎯 Пороги: MACD≤{WS_HOT_NEAR_CROSS_PCT}% · пробой≤{WS_HOT_NEAR_BREAKOUT_PCT}%",
        "🐂1D-bull · 💎RSI-dip · 🌊WT-dip · 💥SQZ · 🎯BOTTOM · 🛡PEAK · 🔧BTC-обход",
        "🔥≤1% ⚡≤3% 🟢≤5% до пробоя · ⛔ пик · ⚠️ дрейф · 🥀 объём↓",
    ]
'''

shutil.copy(F, F + ".before_footer_fix")
with open(F, "r", encoding="utf-8") as fh:
    src = fh.read()

# Ищем блок footer от "    footer = [" до закрывающей "    ]" на том же уровне отступа
pattern = re.compile(r"    footer = \[.*?\n    \]\n", re.DOTALL)
matches = pattern.findall(src)
if not matches:
    print("❌ Не нашёл блок '    footer = [ ... ]' с отступом 4 пробела.")
    print("Проверь вручную строки вокруг 'footer = [' в файле.")
    sys.exit(1)

old = matches[0]
# Проверка: если уже применён (короткий) — ничего не делаем
if "1D-bull · 💎RSI-dip" in old:
    print("✅ Новый footer уже применён. Ничего не меняю.")
    sys.exit(0)

src = src.replace(old, NEW_FOOTER, 1)
print(f"✅ Старый footer: {len(old)} символов")
print(f"✅ Новый footer:  {len(NEW_FOOTER)} символов")

with open(F, "w", encoding="utf-8") as fh:
    fh.write(src)

print("\nПроверка синтаксиса...")
try:
    py_compile.compile(F, doraise=True)
    print("SYNTAX_OK")
except py_compile.PyCompileError as e:
    print("❌ СИНТАКСИЧЕСКАЯ ОШИБКА:", e)
    shutil.copy(F + ".before_footer_fix", F)
    print("Файл восстановлен из бэкапа.")
    sys.exit(1)

print("\nГотово. Бэкап:", F + ".before_footer_fix")
