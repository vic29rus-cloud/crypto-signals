def background_scan_loop():
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

        for idx, pair in enumerate(all_pairs):
            try:
                df_daily = pd.DataFrame(fetch_klines(pair, 1440, 100))
                
                results = {}
                for tf in TIMEFRAME_ORDER:
                    params = TIMEFRAME_PARAMS[tf]
                    df_tf = pd.DataFrame(fetch_klines(pair, params['kraken_interval'], params['min_bars']))
                    results[tf] = analyze_timeframe(df_tf, params)
                    time.sleep(0.1)
                
                # Проверка боковика
                if not df_daily.empty and len(df_daily) > 30:
                    close_price = df_daily['close'].iloc[-1]
                    breakout = check_breakout(df_daily, close_price)
                    
                    # Запись в список для статуса
                    if breakout:
                        # Считаем диапазон для отчета
                        window = df_daily.tail(30)
                        high = window['high'].max()
                        low = window['low'].min()
                        mean = window['close'].mean()
                        range_pct = (high - low) / mean * 100 if mean > 0 else 0
                        adx_val = adx(df_daily).iloc[-1] if not df_daily.empty else 0
                        
                        consolidation_list.append({
                            "pair": pair,
                            "days": breakout['days'],
                            "range_pct": range_pct,
                            "adx": adx_val,
                            "breakout_level": close_price
                        })
                        
                        # Сигнал входа
                        with state_lock:
                            if state.get(pair, {}).get('position') != 'open':
                                send_telegram(f"📦 <b>ПРОБОЙ БОКОВИКА (Breakout)</b>\nПара: {pair}\nЦена: {close_price:.4f}\nSL: {breakout['stop']:.4f}\nTP: {breakout['target']:.4f}\nДней в боковике: {breakout['days']}")
                                state[pair] = {'position': 'open', 'entry_price': close_price, 'stop': breakout['stop'], 'target': breakout['target'], 'entry_time': now_iso, 'strategy': 'breakout'}
                                save_state(state); found_buy += 1

                # Проверка трендов и сбор статистики
                if all(results.get(tf) is not None for tf in TIMEFRAME_ORDER):
                    trend_score = sum(1 for tf in TIMEFRAME_ORDER if results[tf]['trend_up'])
                    r4h = results['4h']
                    # Вычисляем близость MACD к кроссу
                    macd_gap_pct = (r4h['macd_line'] - r4h['macd_signal']) / r4h['close'] * 100 if r4h['close'] else 0.0
                    
                    scan_summary.append({
                        "pair": pair,
                        "trend_score": trend_score,
                        "rsi_4h": r4h['rsi'],
                        "adx_1d": results['1d']['adx'],
                        "macd_gap_pct": macd_gap_pct,
                        "close_price": r4h['close']
                    })

                # Проверка открытых позиций (выходы, безубыток, трейлинг)
                pos = state.get(pair)
                if pos and pos.get('position') == 'open' and results.get(TRIGGER_TF) and results.get('1d'):
                    with state_lock:
                        if pos.get('entry_price') and results[TRIGGER_TF]['close'] > pos['entry_price'] * 1.02 and not pos.get('breakeven_moved'):
                            pos['stop'] = pos['entry_price'] * 1.001; pos['breakeven_moved'] = True
                        
                        if pos.get('breakeven_moved') and results['1d']['atr'] > 0:
                            new_stop = results[TRIGGER_TF]['close'] - results['1d']['atr'] * TRAILING_ATR_MULT
                            if new_stop > pos['stop']: pos['stop'] = new_stop; pos['trailing_active'] = True
                        
                        exit_now, reason = check_exit(results, pos)
                        if exit_now:
                            exit_price = results[TRIGGER_TF]['close']
                            log_trade(pair, pos['entry_price'], exit_price, reason, pos.get('strategy', 'unknown'))
                            send_telegram(f"🔴 <b>ВЫХОД</b>\nПара: {pair}\nЦена: {exit_price:.4f}\nПричина: {reason}")
                            state[pair] = {'position': 'closed'}; save_state(state); found_sell += 1
            except Exception as e:
                logger.error(f"Ошибка в {pair}: {e}")
                continue

        # ======== ФОРМИРОВАНИЕ ПОДРОБНОГО СТАТУСА ========
        open_pos = sum(1 for p in state.values() if p.get('position') == 'open')
        now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
        
        lines = [
            f"📡 <b>Статус сканирования</b> — {now_str}",
            f"━━━━━━━━━━━━━━━━━━━━━",
            f"📊 <b>Общая статистика:</b>",
            f"• Отслеживается пар (Confluence): {len(all_pairs)}",
            f"• Дополнительно проверено на боковик: {TOTAL_PAIRS - TOP_N}",
            f"• Всего в боковике найдено: {len(consolidation_list)}",
            f"• Открытых позиций: {open_pos}",
            f"• Входов за цикл: {found_buy}",
            f"• Выходов за цикл: {found_sell}",
        ]

        # Открытые позиции
        if open_pos > 0:
            lines.append(f"\n💰 <b>Открытые позиции:</b>")
            for pair, pos in state.items():
                if pos.get('position') == 'open':
                    entry_time = str(pos.get('entry_time', '?')).replace("T", " ")[:16]
                    lines.append(f"• <b>{pair}</b> | Вход: {pos.get('entry_price', 0):.6g} | Время: {entry_time} | SL: {pos.get('stop', 0):.6g} | TP: {pos.get('target', 0):.6g}")
        else:
            lines.append(f"\n💰 <b>Открытых позиций нет.</b>")

        # Топ кандидатов
        close_calls = [s for s in scan_summary if s["trend_score"] >= 2]
        close_calls.sort(key=lambda s: s.get("macd_gap_pct", 999)) # Сортировка по близости к кроссу

        if close_calls:
            lines.append(f"\n🎯 <b>Топ кандидатов на вход (Confluence):</b>")
            for i, s in enumerate(close_calls[:3], 1):
                gap = s.get("macd_gap_pct", 0)
                if gap < 0:
                    proximity = "⏳ Близко к кроссу (ждём)"
                elif gap < 0.5:
                    proximity = "🟡 Кросс недавно, ещё актуально"
                else:
                    proximity = "⚠️ Кросс был давно, вход маловероятен скоро"
                
                lines.append(f"{i}. <b>{s['pair']}</b>\n   Тренд: {s['trend_score']}/3 | ADX: {s['adx_1d']:.0f} | RSI(4h): {s['rsi_4h']:.0f}\n   Ориентир входа (тек. цена): ~{s['close_price']:.6g}\n   {proximity}")
        else:
            lines.append(f"\n😴 <b>Кандидатов на вход (Confluence) нет</b>")

        # Боковики
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
