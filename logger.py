"""
Logger v11.2 - Con fees dinámicos y flag direccional
"""
import csv
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from config import LOGS_DIR


class SignalLogger:
    def __init__(self):
        self.trades_file = LOGS_DIR / "trades_log.csv"
        self.decisions_file = LOGS_DIR / "decisions_log.csv"

        if not self.trades_file.exists():
            with open(self.trades_file, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "timestamp", "utc", "event", "side", "price", "size_btc",
                    "directional_score", "amplitude_score", "pnl_usd", "fee_usd",
                    "net_pnl_usd", "duration_sec", "reason", "directional_win"
                ])
        if not self.decisions_file.exists():
            with open(self.decisions_file, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "timestamp", "utc", "price", "dir_score", "amp_score",
                    "decision", "reason", "aligned_count",
                    "feed_age_sec", "signal_age_sec"
                ])

        self.recent_decisions = deque(maxlen=50)
        self.recent_trades = deque(maxlen=100)

    def log_decision(self, price, dir_score, amp_score, decision, reason, aligned,
                     feed_age_sec: float = -1.0, signal_age_sec: float = -1.0):
        try:
            now = time.time()
            row = [now, datetime.now(timezone.utc).isoformat(), price,
                   round(dir_score, 6), round(amp_score, 6), decision, reason, aligned,
                   round(feed_age_sec, 2), round(signal_age_sec, 2)]
            with open(self.decisions_file, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(row)
            self.recent_decisions.append({
                "timestamp": now, "price": price, "dir_score": dir_score,
                "amp_score": amp_score, "decision": decision, "reason": reason
            })
        except Exception:
            pass

    def log_trade(self, event, side, price, size_btc, dir_score, amp_score,
                  pnl_usd=0.0, duration_sec=0.0, reason="",
                  is_directional_win=False, fee_rate=None,
                  fee_already_deducted: bool = False):
        try:
            from config import Config as _Cfg
            if fee_rate is None:
                # FIX v14.1.4 F1 (log trades 03:22-03:36): doble contabilidad de
                # fees. El logger sumaba ida+vuelta en la fila CLOSE (net =
                # pnl - fee*2) PERO engine_main._close() YA restaba el ciclo
                # completo ANTES de pasar pnl_usd=gross... y además el OPEN
                # escribía su propia media fee -> cada trade aparecía con ~1.5x
                # su coste real en el CSV (-$9.79 de fees sobre $54 de nocional
                # acumulado). Ahora: el ENGINE es la única fuente del coste
                # (taker_fee_rate x 2 por ciclo); el logger solo refleja.
                fee_rate = _Cfg().taker_fee_rate
            now = time.time()
            notional = price * size_btc
            fee_per_side = notional * fee_rate
            # FIX v14.1.4 F3 (coherente con engine_main._open): la fila OPEN es
            # SOLO evento informativo -> sin coste economico (el ciclo completo
            # lo refleja la fila CLOSE). Evita el doble conteo del CSV que
            # inflaba los fees ~1.5x al sumar OPEN+CLOSE.
            if event == "CLOSE":
                total_fee = fee_per_side * 2
            else:
                fee_per_side = 0.0
                total_fee = 0.0
            # FIX v14.1.4 F5: si el llamador ya descontó el coste del ciclo
            # (engine_main._close pasa net), NO volver a restarlo aqui. El
            # bruto se reconstruye re-sumando el fee para la consola.
            if fee_already_deducted:
                net = pnl_usd
                gross_shown = pnl_usd + total_fee
            else:
                net = pnl_usd - total_fee
                gross_shown = pnl_usd

            row = [
                now, datetime.now(timezone.utc).isoformat(), event, side,
                price, size_btc, round(dir_score, 6), round(amp_score, 6),
                # FIX v14.1.4 F5: la columna pnl_usd debe ser SIEMPRE el bruto
                # del trade (antes con fee_already_deducted guardaba el neto y
                # pnl/fee/net eran incoherentes entre filas del mismo ciclo).
                round(gross_shown, 6), round(total_fee, 6), round(net, 6),
                round(max(0.0, duration_sec), 2), reason,
                "1" if is_directional_win else "0"
            ]
            with open(self.trades_file, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(row)

            self.recent_trades.append({
                "timestamp": now, "event": event, "side": side, "price": price,
                "size_btc": size_btc, "pnl_usd": gross_shown, "fee_usd": total_fee,
                "net_pnl_usd": net, "duration_sec": duration_sec, "reason": reason,
                "directional_win": is_directional_win
            })

            if event == "CLOSE":
                win_tag = "✅ DIR WIN" if is_directional_win else "❌ DIR LOSS"
                print(f"   🧾 {win_tag} | Bruto: {gross_shown:+.4f} | Fee: -{total_fee:.4f} | "
                      f"NETO: {net:+.4f} | {duration_sec:.1f}s | {reason}")
        except Exception as e:
            print(f"⚠️  Error log: {e}")