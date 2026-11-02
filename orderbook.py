"""
OrderBook v14.1 - Libro con filtro anti-spoofing NO-destructivo + cache coherente

v13.8: get_clean_book() guarda el ultimo snapshot filtrado en `last_clean` para
que las señales anticipativas basadas en LIBRO se calculen UNA vez por delta.

FIXES v14.1 (auditoría):
  B5 (anti-spoof destructivo): `_filter` EXIGIA antigüedad >= lifetime_ms, de
     modo que tras cada snapshot/reconciliación el libro limpio quedaba VACÍO
     durante ~500ms y todas las señales de libro emitían NEUTRAL conf=0
     (microestructura muerta cíclicamente en cada reconexión). Ahora los niveles
     NUEVOS (<lifetime) se conservan pero marcados como "inmaduros": la señal
     OBI mantiene el espíritu anti-spoof excluyéndolos de su conteo, mientras
     wofi/book_slope/wall/thinning siguen viendo un libro completo.
  D6 (cache incoherente): la cache `last_clean` se invalidaba con cualquier
     delta entre lecturas y cada señal cortaba a niveles distintos (20 vs 30),
     mezclando cortes. Ahora la cache es del MAXIMO nivel pedido (30) y cada
     consumidor corta desde ella; se rellena una sola vez por tick del loop.
"""
import time
from typing import Dict, List, Optional, Tuple


class SimpleOBM:
    # FIX A3b: tamaño del corte top-N que WeightedOFI vigila (niveles que salen
    # de él por desplazamiento cuentan como cancel de liquidez).
    TOP_N = 20

    def __init__(self, lifetime_ms: int):
        self.lifetime_ms = lifetime_ms
        # price -> {"qty", "first_seen", "snap_ts"}
        # snap_ts != 0 => nivel llegado en el último snapshot (sospechoso de
        # spoof hasta cumplir la vida mínima; ver get_clean_book(include_new)).
        self.bids: Dict[float, dict] = {}
        self.asks: Dict[float, dict] = {}
        # FIX A3b: ultimo corte top-N POR LADO conocido por un snapshot real
        # (los niveles que salen del top-N por desplazamiento natural se
        # comparan contra él y cuentan como cancel, no desaparecen invisibles).
        self.last_top_b: Dict[float, float] = {}
        self.last_top_a: Dict[float, float] = {}
        # FIX A3c: secuencia en la que se aplico el ultimo snapshot REAL
        # (distinto del estado). WeightedOFI la usa para distinguir un delta
        # genuino de un re-snapshot periodico identico.
        self._last_snap_seq = -1
        # FIX v14.1 A3c-2 (test de flush): un snapshot periodico legitimo NO
        # reetiqueta niveles que ya existen con la misma cantidad. Si el mapa
        # precio->qty cambia pero los niveles "nuevos" son en realidad viejos
        # reetiquetados (snap_ts==now), el evento es real (flush/reconciliacion)
        # y WeightedOFI debe procesar sus deltas, no tragarlo como neutro.
        self._last_snap_event_seq = -1
        # v14.1: cache del ultimo snapshot limpio al maximo nivel pedido
        self.last_clean: Optional[Tuple[List, List]] = None
        self._clean_levels = 0
        self._clean_seq = -1        # secuencia del estado aplicado a la cache
        self._seq = 0                # cambia con cada snapshot/delta aplicado

    def note_top(self, bids_l: List, asks_l: List) -> None:
        """Registra el corte top-N actual para detección de salidas por
        desplazamiento (llamado por get_clean_book)."""
        self.last_top_b = {p: q for p, q in bids_l}
        self.last_top_a = {p: q for p, q in asks_l}

    def apply_snapshot(self, bids, asks, ts) -> bool:
        """Devuelve True si el snapshot es IDENTICO al estado actual (re-snapshot
        periódico de Bybit sin cambios reales): los consumidores de deltas
        (WeightedOFI/WallMomentum) deben IGNORARLO para no inventar cancels."""
        now = ts / 1000.0 if ts > 1e12 else ts
        new_b: Dict[float, dict] = {}
        for p, q in bids:
            price, qty = float(p), float(q)
            if qty <= 0:
                continue
            old = self.bids.get(price)
            if old is not None and abs(old["qty"] - qty) < 1e-12:
                # nivel idéntico: conserva antigüedad real (no es spoof nuevo)
                new_b[price] = {"qty": qty, "first_seen": old["first_seen"],
                                "snap_ts": 0.0}
            else:
                new_b[price] = {"qty": qty, "first_seen": now, "snap_ts": now}
        new_a: Dict[float, dict] = {}
        for p, q in asks:
            price, qty = float(p), float(q)
            if qty <= 0:
                continue
            old = self.asks.get(price)
            if old is not None and abs(old["qty"] - qty) < 1e-12:
                new_a[price] = {"qty": qty, "first_seen": old["first_seen"],
                                "snap_ts": 0.0}
            else:
                new_a[price] = {"qty": qty, "first_seen": now, "snap_ts": now}
        # FIX v14.1 A3c (verificacion): la identidad se compara por CONTENIDO
        # (precio->qty), no por el dict entero. `first_seen`/`snap_ts` difieren
        # en cada re-snapshot periodico de Bybit aunque no haya cambio real;
        # si se comparan incluidos, un flush total seguido de re-snapshot con
        # los mismos precios se leia como "añadidos masivos" (norm=+1 fantasma).
        ident_b = ({p: d["qty"] for p, d in new_b.items()}
                   == {p: d["qty"] for p, d in self.bids.items()})
        ident_a = ({p: d["qty"] for p, d in new_a.items()}
                   == {p: d["qty"] for p, d in self.asks.items()})
        identical = ident_b and ident_a
        if identical:
            # re-snapshot puro: conservar first_seen/snap_ts REALES (antiguedad
            # legitima) y NO invalidar cache ni secuencia (no hubo evento).
            return True
        # FIX A3c-2: ¿el snapshot trae niveles "nuevos" que en realidad son
        # viejos reetiquetados? Un snapshot periodico legitimo conserva la
        # antiguedad de los niveles idénticos; si aun asi el contenido cambio
        # (flush/reconciliacion con precios nuevos), es un EVENTO REAL y las
        # señales de delta deben procesarlo. Detectamos: cualquier nivel con
        # snap_ts==now => hubo reetiquetado => evento genuino.
        has_retagged = any(d["snap_ts"] == now for d in new_b.values()) or \
            any(d["snap_ts"] == now for d in new_a.values())
        self.bids = new_b
        self.asks = new_a
        self.last_clean = None
        self._clean_levels = 0
        self._seq += 1
        self._last_snap_seq = self._seq   # FIX A3c: marca del ultimo snapshot real
        if has_retagged:
            self._last_snap_event_seq = self._seq
        return False

    def apply_delta(self, bids, asks, ts):
        now = ts / 1000.0 if ts > 1e12 else ts
        for side_book, levels in ((self.bids, bids), (self.asks, asks)):
            for p, q in levels:
                price, qty = float(p), float(q)
                if qty == 0:
                    side_book.pop(price, None)
                elif price not in side_book:
                    side_book[price] = {"qty": qty, "first_seen": now,
                                        "snap_ts": 0.0}
                else:
                    side_book[price]["qty"] = qty
        self.last_clean = None   # invalidar cache (el estado cambió)
        self._seq += 1

    def _filter(self, book: Dict[float, dict], now: float,
                include_new: bool) -> Dict[float, float]:
        """FIX B5: `include_new=True` conserva niveles recientes (marcados por
        snapshot) en lugar de vaciar el libro ~lifetime_ms tras cada snapshot."""
        out: Dict[float, float] = {}
        for p, d in book.items():
            age_ms = (now - d["first_seen"]) * 1000.0
            if age_ms >= self.lifetime_ms:
                out[p] = d["qty"]
            elif include_new or d.get("snap_ts", 0.0) == 0.0:
                # delta legítimo reciente (no proviene de un snapshot): vale
                out[p] = d["qty"]
        return out

    def get_clean_book(self, levels=20, include_new: bool = True
                       ) -> Tuple[List, List]:
        """Snapshot ordenado y filtrado. Con levels>=25 rellena la cache
        `last_clean` (corte MAXIMO) que comparten todas las señales de libro.

        include_new=False => comportamiento anti-spoof estricto (solo niveles
        con antigüedad garantizada); lo usa exclusivamente calculate_obi.
        """
        now = time.time()
        cb = self._filter(self.bids, now, include_new)
        ca = self._filter(self.asks, now, include_new)
        n = max(levels, 30)
        bids_l = sorted(cb.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_l = sorted(ca.items(), key=lambda x: x[0])[:n]
        if levels >= 25 and include_new:
            self.last_clean = (bids_l, asks_l)
            self._clean_levels = n
            self._clean_seq = self._seq
            # FIX A3b: el corte top-N del lado mas fino queda registrado para
            # que WeightedOFI detecte niveles que salen del top por desplazamiento
            self.note_top(bids_l[:self.TOP_N], asks_l[:self.TOP_N])
        return bids_l, asks_l

    def cache_is_current(self) -> bool:
        return (self.last_clean is not None
                and self._clean_seq == self._seq
                and self._clean_levels >= 30)

    def get_best_bid_ask(self) -> Tuple[float, float]:
        if not self.bids or not self.asks:
            return 0.0, 0.0
        return max(self.bids), min(self.asks)

    def mid_price(self) -> float:
        bid, ask = self.get_best_bid_ask()
        return (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
