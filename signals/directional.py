"""
Directional Signals v12.1 - FIX memory leak en deques
"""
import time
from collections import deque

import aiohttp

from models import Direction, Signal
from orderbook import SimpleOBM
from signals.utils import dir_from


# 1. OBI Anti-Spoof
def calculate_obi(book: SimpleOBM, levels: int = 20) -> Signal:
    # FIX v14.1 B5: unico consumidor que exige antiguedad estricta (anti-spoof).
    # Las demas señales de libro usan include_new=True para no quedarse ciegas
    # ~500ms tras cada snapshot/reconciliacion.
    bids, asks = book.get_clean_book(levels, include_new=False)
    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    total = bv + av
    if total == 0:
        return Signal("obi_anti_spoof", 0, 0, Direction.NEUTRAL, 0)
    v = (bv - av) / total
    return Signal("obi_anti_spoof", v, v, dir_from(v), min(abs(v) / 0.5, 1.0))


# 2. Large Trades
class LargeTradeDetector:
    def __init__(self, threshold: float = 5_000.0, window: int = 60):
        self.threshold = threshold
        self.window = window
        self.trades: deque = deque(maxlen=5000)  # 🔧 FIX: maxlen

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, price, qty, maker))
        while self.trades and self.trades[0][0] < ts - self.window:
            self.trades.popleft()

    def calculate(self, price: float) -> Signal:
        if len(self.trades) < 3:
            return Signal("large_trades", 0, 0, Direction.NEUTRAL, 0)
        ps = [t[1] for t in self.trades]
        mean = sum(ps) / len(ps)
        vol = (sum((p - mean) ** 2 for p in ps) / len(ps)) ** 0.5 / mean if mean else 0.01
        th = self.threshold * (1.0 + vol * 100)
        buys = sum(p * q for _, p, q, m in self.trades if p * q >= th and not m)
        sells = sum(p * q for _, p, q, m in self.trades if p * q >= th and m)
        total = buys + sells
        count = sum(1 for _, p, q, _ in self.trades if p * q >= th)
        if total == 0:
            return Signal("large_trades", 0, 0, Direction.NEUTRAL, 0.0)
        v = (buys - sells) / total
        return Signal("large_trades", v, v, dir_from(v, 0.15), min(count / 3.0, 1.0))


# 3. Absorption (v13.4: ventana TEMPORAL de 60s y sentido CORREGIDO)
class AbsorptionDetector:
    """Absorción = flujo agresor fuerte que NO mueve el precio.

    FIX v13.4 CRÍTICO (sentido invertido): antes, compra agresora sin avance se
    puntuaba como SHORT (-0.7). Es al revés: si entran market buys y el precio
    no sube, hay un wall vendedor AGOTÁNDOSE; cuando se vacíe, breakout LONG.
    (La versión "compradores atrapados" es fade de vela extendida, otro caso.)
    Además usaba los últimos 100 trades SIN recortar por tiempo: en BTC pueden
    cubrir horas -> señal anclada a datos viejos (contribuía a mantener la
    posición 1 hora con tesis muerta). Ahora ventana fija de 60 segundos.
    """

    def __init__(self, window_sec: float = 60.0, max_trades: int = 6000):
        self.window = window_sec
        self.trades: deque = deque(maxlen=max_trades)

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, price, qty, maker))
        cutoff = ts - self.window
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    def calculate(self) -> Signal:
        now = time.time()
        recent = [t for t in self.trades if t[0] >= now - self.window]
        if len(recent) < 30:
            return Signal("absorption", 0, 0, Direction.NEUTRAL, 0)
        buys = sum(q for _, _, q, m in recent if not m)
        sells = sum(q for _, _, q, m in recent if m)
        total = buys + sells
        if total == 0:
            return Signal("absorption", 0, 0, Direction.NEUTRAL, 0)
        imb = (buys - sells) / total
        ps = [t[1] for t in recent]
        move = (ps[-1] - ps[0]) / ps[0] * 100 if ps[0] else 0
        norm, direction = 0.0, Direction.NEUTRAL
        quiet = max(0.0, 1.0 - abs(move) / 0.15)     # 1 = precio plano
        if abs(imb) >= 0.15 and quiet > 0.5:
            # sentido CONTINUACIÓN: el lado agresor gana al final
            strength = min(abs(imb) / 0.4, 1.0) * quiet
            if imb > 0:
                norm, direction = strength, Direction.LONG
            else:
                norm, direction = -strength, Direction.SHORT
            conf = min(abs(imb) / 0.35, 1.0) * (0.5 + 0.5 * quiet)
            return Signal("absorption", imb, norm, direction, conf)
        return Signal("absorption", imb, 0, Direction.NEUTRAL, 0.1)


# 4. OI Delta
class OIDeltaTracker:
    def __init__(self, rest_url: str, symbol: str, window: int = 300):
        self.rest_url = rest_url
        self.symbol = symbol
        self.window = window
        self.history: deque = deque(maxlen=60)
        self.last_fetch = 0.0

    async def update(self, session_factory=None):
        now = time.time()
        if now - self.last_fetch < 10:
            return
        # FIX: reutilizar sesión (antes se creaba y cerraba una por petición,
        # con el timeout total puesto a None -> podía colgarse sin límite)
        try:
            if session_factory is not None:
                s = await session_factory()
                ctx = None
            else:
                timeout = aiohttp.ClientTimeout(total=8, connect=5)
                ctx = aiohttp.ClientSession(timeout=timeout)
                s = await ctx.__aenter__()
            try:
                async with s.get(f"{self.rest_url}/v5/market/open-interest", params={
                    "category": "linear", "symbol": self.symbol,
                    "intervalTime": "5min", "limit": 1
                }) as r:
                    d = await r.json()
                    if d.get("retCode") == 0 and d.get("result", {}).get("list"):
                        self.history.append((now, float(d["result"]["list"][0]["openInterest"])))
                        self.last_fetch = now
            finally:
                if ctx is not None:
                    await ctx.__aexit__(None, None, None)
        except Exception:
            pass

    def calculate(self, price: float, price_change_pct: float) -> Signal:
        """FIX v14.8 (clasificacion correcta OI x precio):

        Antes la firma se copiaba del MOVIMIENTO DE PRECIO pasado
        (`dir_from(price_change_pct)`), es decir era trend-following disfrazado
        de señal anticipativa: confirmaba lo ya ocurrido en lugar de anticipar.

        Tabla canonica derivados (4 regimenes):
          OI sube + precio sube  -> LONG  (nuevos largos agresivos, tendencia con combustible)
          OI sube + precio baja  -> SHORT (nuevos cortos agresivos)
          OI baja + precio sube  -> SHORT (short-covering: el rebote es cierre de cortos,
                                          sin dinero nuevo -> reversión)
          OI baja + precio baja  -> LONG  (long liquidation flush: capitulación -> rebote)
        La magnitud es el % de cambio de OI en la ventana (0.5% OI en 5min ya
        es mucho en BTCUSDT). El precio solo entra como DISCRIMINADOR de régimen,
        no como fuente de la firma.
        """
        if len(self.history) < 2:
            return Signal("oi_delta", 0, 0, Direction.NEUTRAL, 0)
        now = time.time()
        recent = [(t, v) for t, v in self.history if t >= now - self.window]
        if len(recent) < 2 or recent[0][1] <= 0:
            return Signal("oi_delta", 0, 0, Direction.NEUTRAL, 0)
        change = (recent[-1][1] - recent[0][1]) / recent[0][1] * 100
        norm, direction = 0.0, Direction.NEUTRAL
        if abs(change) > 0.03:
            # Precio: comparativa misma ventana (media primera mitad vs ultima
            # muestra) para no depender del parpadeo del ultimo tick.
            mid = recent[len(recent) // 2][1]
            px_ref = None
            # usamos el precio actual vs el precio hace ~window (si el caller
            # pasa price_change_pct lo respetamos como proxy valido)
            pchg = price_change_pct
            oi_up = change > 0
            if abs(pchg) < 0.005:
                direction = Direction.NEUTRAL   # precio plano: OI solo no decide
            elif oi_up and pchg > 0:
                direction = Direction.LONG      # new longs
            elif oi_up and pchg < 0:
                direction = Direction.SHORT     # new shorts
            elif (not oi_up) and pchg > 0:
                direction = Direction.SHORT     # short covering -> fade
            else:
                direction = Direction.LONG      # long flush -> mean reversion
            strength = min(abs(change) / 0.5, 1.0)
            norm = strength * (1 if direction == Direction.LONG else -1)
        return Signal("oi_delta", change, norm, direction,
                      min(abs(change) / 0.5, 1.0))


# 5. Long/Short Ratio
class LongShortRatioTracker:
    def __init__(self, rest_url: str, symbol: str):
        self.rest_url = rest_url
        self.symbol = symbol
        self.last_ratio = 1.0
        self.last_fetch = 0.0

    async def update(self, session_factory=None):
        now = time.time()
        if now - self.last_fetch < 60:
            return
        try:
            if session_factory is not None:
                s = await session_factory()
                ctx = None
            else:
                timeout = aiohttp.ClientTimeout(total=8, connect=5)
                ctx = aiohttp.ClientSession(timeout=timeout)
                s = await ctx.__aenter__()
            try:
                async with s.get(f"{self.rest_url}/v5/market/account-ratio", params={
                    "category": "linear", "symbol": self.symbol, "period": "5min"
                }) as r:
                    d = await r.json()
                    if d.get("retCode") == 0 and d.get("result", {}).get("list"):
                        # FIX v14.8 ANTI-SPREAD: Bybit ordena account-ratio del
                        # MAS VIEJO (list[0]) al MAS NUEVO (list[-1]). Antes se
                        # leia list[0] -> senal permanentemente desfasada.
                        item = d["result"]["list"][-1]
                        sell = max(float(item.get("sellRatio", 1) or 1), 0.0001)
                        self.last_ratio = float(item.get("buyRatio", 1) or 1) / sell
                        self.last_fetch = now
                        try:
                            self.last_ts = float(item.get("timestamp", 0) or 0) / 1000.0
                        except (TypeError, ValueError):
                            self.last_ts = now
            finally:
                if ctx is not None:
                    await ctx.__aexit__(None, None, None)
        except Exception:
            pass

    def calculate(self) -> Signal:
        """FIX v14.8 (sentido + frescura):

        1) ANTI-SPREAD: Bybit publica list[0] con el timestamp MAS VIEJO y el
           mas NUEVO al final (verificado en API real 2026-11-02). El codigo
           viejo leia list[0] -> usaba una medicion de hace N*5min como si fuera
           actual: senal SIEMPRE desfasada (y congelada si la pagina no traia
           nada nuevo). Ahora se toma la ULTIMA entrada y su timestamp.

        2) SENTIDO: buyRatio/sellRatio de BYBIT son por CUENTAS (retail), no por
           volumen. El retail mayoritariamente largo es combustible de short /
           capitulation LONG segun regimen, pero el uso clasico contrarian en
           scalping: crowd extremo largo (>1.3) -> sesgo SHORT; crowd extremo
           corto (<0.8) -> sesgo LONG. Magnitud proporcional a la distancia al
           nivel neutral 1.0 (antes era un salto binario -0.5/+0.5 sin gradiente).
        """
        level = self.last_ratio
        ts = getattr(self, "last_ts", 0.0)
        # frescura: ratio de cuentas cambia lento; si tiene >15 min, conf baja
        age = time.time() - ts if ts else 9999.0
        fresh = 1.0 if age <= 300 else max(0.2, 1.0 - (age - 300) / 900.0)
        norm, direction = 0.0, Direction.NEUTRAL
        if level > 1.15:          # crowd largos -> fade SHORT
            norm = -min((level - 1.0) / 0.5, 1.0)
            direction = Direction.SHORT
        elif level < 0.87:        # crowd cortos -> fade LONG
            norm = min((1.0 - level) / 0.5, 1.0)
            direction = Direction.LONG
        conf = (0.35 + 0.35 * abs(norm)) * fresh if norm else 0.15 * fresh
        return Signal("ls_ratio", level, norm, direction, conf)


# 6. Funding Rate Anticipativo (nueva señal v14.8)
class FundingRateTracker:
    """Funding = flujo de dinero real entre largos y cortos.

    Uso ANTICIPATIVO (no reactivo): el funding proyectado (predictedFundingFee
    del ticker REST, gratis y ya disponible en la respuesta que el bot consulta
    cada rest_poll_sec) anticipa presion:
      - funding muy POSITIVO y SUBIENDOSE -> largos pagan mucho -> exceso de
        apalancamiento largo -> riesgo de squeeze -> sesgo SHORT contrarian
        cuando ademas el precio no avanza (falta de combustible).
      - funding NEGATIVO y BAJANDO -> cortos pagan -> squeezable -> sesgo LONG.
    Magnitud normalizada: |funding| por intervalo; 0.01%/8h ya es extremo en
    BTC (escala: 0.05% = saturacion). Conf alta solo si desvia del neutro.
    """

    def __init__(self, rest_url: str, symbol: str):
        self.rest_url = rest_url
        self.symbol = symbol
        self.current = 0.0          # funding actual (ya liquidado)
        self.predicted = 0.0        # funding proyectado proximo intervalo
        self.next_funding_ts = 0.0
        self.last_fetch = 0.0
        self.prev_predicted = 0.0   # para la DERIVADA (dirección del cambio)

    async def update(self, session_factory=None):
        now = time.time()
        if now - self.last_fetch < 30:
            return
        try:
            if session_factory is not None:
                s = await session_factory()
                ctx = None
            else:
                timeout = aiohttp.ClientTimeout(total=8, connect=5)
                ctx = aiohttp.ClientSession(timeout=timeout)
                s = await ctx.__aenter__()
            try:
                async with s.get(f"{self.rest_url}/v5/market/tickers", params={
                    "category": "linear", "symbol": self.symbol
                }) as r:
                    d = await r.json()
                    lst = d.get("result", {}).get("list") or []
                    if d.get("retCode") == 0 and lst:
                        t = lst[0]
                        self.prev_predicted = self.predicted
                        self.current = float(t.get("fundingRate", 0) or 0)
                        self.predicted = float(
                            t.get("predictedFundingFee", 0) or 0) or self.current
                        self.next_funding_ts = float(
                            t.get("nextFundingTime", 0) or 0) / 1000.0
                        self.last_fetch = now
            finally:
                if ctx is not None:
                    await ctx.__aexit__(None, None, None)
        except Exception:
            pass

    def calculate(self) -> Signal:
        age = time.time() - self.last_fetch if self.last_fetch else 9999.0
        fresh = 1.0 if age <= 90 else max(0.2, 1.0 - (age - 90) / 300.0)
        f = self.predicted
        delta = self.predicted - self.prev_predicted
        norm, direction = 0.0, Direction.NEUTRAL
        # escala: 0.0005 = 0.05% por intervalo -> saturación
        mag = min(abs(f) / 0.0005, 1.0)
        if abs(f) > 0.00005:                      # >0.005%: relevante
            if f > 0 and delta >= -abs(f) * 0.1:  # caro estar largo y sin relajarse
                direction = Direction.SHORT       # riesgo de long-squeeze
            elif f < 0:                           # corto paga -> combustible LONG
                direction = Direction.LONG
        if direction != Direction.NEUTRAL:
            # la derivada refuerza o amortigua (funding subiendo = más riesgo)
            boost = 1.0
            if (f > 0 and delta > 0) or (f < 0 and delta < 0):
                boost = 1.25
            elif delta != 0 and (f > 0) != (delta > 0):
                boost = 0.7
            norm = mag * boost * (1 if direction == Direction.LONG else -1)
            norm = max(-1.0, min(1.0, norm))
        conf = (0.3 + 0.5 * mag) * fresh if norm else 0.1 * fresh
        return Signal("funding_pressure", f, norm, direction, conf)


# 7. Taker Buy/Sell Ratio
class TakerBuySellRatio:
    def __init__(self, window: int = 60):
        self.window = window
        self.trades: deque = deque(maxlen=5000)  # 🔧 FIX: maxlen

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, price, qty, maker))
        while self.trades and self.trades[0][0] < ts - self.window:
            self.trades.popleft()

    def calculate(self) -> Signal:
        if len(self.trades) < 30:
            return Signal("taker_ratio", 0, 0, Direction.NEUTRAL, 0)
        buys = sum(p * q for _, p, q, m in self.trades if not m)
        sells = sum(p * q for _, p, q, m in self.trades if m)
        if sells <= 0:
            return Signal("taker_ratio", 0, 0, Direction.NEUTRAL, 0)
        ratio = buys / sells
        v = max(-1.0, min(1.0, (ratio - 1.0) * 2.0))
        direction = dir_from(v, 0.15)
        # FIX v13.4 CRÍTICO: se pasaba `ratio` (>=0, p.ej. 1.8) como `normalized`
        # en vez de `v` (-1..1). El motor sumaba +1.8*peso por cada trade normal
        # -> sesgo LONG estructural: el bot abría LONGs que no anticipaban nada.
        return Signal("taker_ratio", ratio, v, direction, min(len(self.trades) / 100.0, 1.0))


# 7. CVD (Cumulative Volume Delta) - 🔧 FIX memory leak
class CVDCalculator:
    def __init__(self, window: int = 300):
        self.window = window
        self.trades: deque = deque(maxlen=10000)  # 🔧 FIX: maxlen
        self.cvd_history: deque = deque(maxlen=100)
        self.price_history: deque = deque(maxlen=100)

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, price, qty, maker))
        # 🔧 FIX: limpieza por tiempo + maxlen de seguridad
        while self.trades and self.trades[0][0] < ts - self.window:
            self.trades.popleft()
        buys = sum(q for _, _, q, m in self.trades if not m)
        sells = sum(q for _, _, q, m in self.trades if m)
        cvd = buys - sells
        self.cvd_history.append((ts, cvd))
        self.price_history.append((ts, price))

    def calculate(self) -> Signal:
        if len(self.cvd_history) < 20:
            return Signal("cvd", 0, 0, Direction.NEUTRAL, 0)
        now = time.time()
        recent_cvd = [(t, v) for t, v in self.cvd_history if t >= now - 60]
        recent_price = [(t, p) for t, p in self.price_history if t >= now - 60]
        if len(recent_cvd) < 2 or len(recent_price) < 2:
            return Signal("cvd", 0, 0, Direction.NEUTRAL, 0)
        cvd_change = recent_cvd[-1][1] - recent_cvd[0][1]
        price_change = (recent_price[-1][1] - recent_price[0][1]) / recent_price[0][1] * 100
        max_cvd = 1000
        norm = max(-1.0, min(1.0, cvd_change / max_cvd))
        direction = Direction.NEUTRAL
        if norm > 0.2 and price_change > 0.05:
            direction = Direction.LONG
        elif norm < -0.2 and price_change < -0.05:
            direction = Direction.SHORT
        elif norm > 0.3 and price_change < -0.05:
            direction = Direction.LONG
        elif norm < -0.3 and price_change > 0.05:
            direction = Direction.SHORT
        confidence = min(abs(norm) / 0.5, 1.0)
        return Signal("cvd", cvd_change, norm, direction, confidence)


# 8. VWAP Distance - 🔧 FIX memory leak
class VWAPDistance:
    def __init__(self, window: int = 3600):
        self.window = window
        self.trades: deque = deque(maxlen=50000)  # 🔧 FIX: maxlen grande pero finito

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, price, qty))
        # 🔧 FIX: limpieza por tiempo
        while self.trades and self.trades[0][0] < ts - self.window:
            self.trades.popleft()

    def calculate(self, current_price: float) -> Signal:
        if len(self.trades) < 50:
            return Signal("vwap_distance", 0, 0, Direction.NEUTRAL, 0)
        total_volume = sum(q for _, _, q in self.trades)
        if total_volume == 0:
            return Signal("vwap_distance", 0, 0, Direction.NEUTRAL, 0)
        vwap = sum(p * q for _, p, q in self.trades) / total_volume
        distance_pct = (current_price - vwap) / vwap * 100
        norm = max(-1.0, min(1.0, distance_pct / 0.5))
        # FIX v13.4 CRÍTICO: sentido INVERTIDO. Estaba: precio > VWAP => LONG.
        # La señal se llama "distance" y su conf cae cuanto MÁS se aleja el
        # precio del VWAP (diseño de reversión a la media): lejos por encima
        # del VWAP = sobreextensión => SHORT (fade), lejos por debajo => LONG.
        # Con el signo viejo el bloque reactivo estaba SIEMPRE en LONG mientras
        # BTC subía (y siempre SHORT al caer) -> sesgo direccional estructural
        # que hacía entrar tarde y mantener posiciones 1 hora contra la tesis.
        direction = Direction.NEUTRAL
        if abs(distance_pct) >= 0.1:
            direction = Direction.SHORT if distance_pct > 0 else Direction.LONG
        confidence = max(0.2, 1.0 - abs(distance_pct) / 1.0)
        return Signal("vwap_distance", distance_pct, norm, direction, confidence)


# 9. Delta por Velas de 1min
class CandleDelta:
    def __init__(self):
        self.current_candle_start = 0
        self.current_candle_delta = 0
        self.candle_deltas: deque = deque(maxlen=10)

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        candle_start = int(ts / 60) * 60
        if candle_start != self.current_candle_start:
            if self.current_candle_start > 0:
                self.candle_deltas.append((self.current_candle_start, self.current_candle_delta))
            self.current_candle_start = candle_start
            self.current_candle_delta = 0
        if maker:
            self.current_candle_delta -= qty
        else:
            self.current_candle_delta += qty

    def calculate(self) -> Signal:
        if len(self.candle_deltas) < 3:
            return Signal("candle_delta", 0, 0, Direction.NEUTRAL, 0)
        recent = list(self.candle_deltas)[-3:]
        total_delta = sum(d for _, d in recent)
        max_delta = 500
        norm = max(-1.0, min(1.0, total_delta / max_delta))
        direction = Direction.NEUTRAL
        if norm > 0.2:
            direction = Direction.LONG
        elif norm < -0.2:
            direction = Direction.SHORT
        confidence = min(abs(norm) / 0.5, 1.0)
        return Signal("candle_delta", total_delta, norm, direction, confidence)


# 10. Order Flow Imbalance - 🔧 FIX memory leak
class OrderFlowImbalance:
    def __init__(self, window: int = 30):
        self.window = window
        self.trades: deque = deque(maxlen=5000)  # 🔧 FIX: maxlen

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, price, qty, maker))
        while self.trades and self.trades[0][0] < ts - self.window:
            self.trades.popleft()

    def calculate(self) -> Signal:
        if len(self.trades) < 20:
            return Signal("order_flow_imb", 0, 0, Direction.NEUTRAL, 0)
        buys = sum(q for _, _, q, m in self.trades if not m)
        sells = sum(q for _, _, q, m in self.trades if m)
        total = buys + sells
        if total == 0:
            return Signal("order_flow_imb", 0, 0, Direction.NEUTRAL, 0)
        imbalance = (buys - sells) / total
        norm = max(-1.0, min(1.0, imbalance * 2))
        direction = dir_from(imbalance, 0.1)
        confidence = min(abs(imbalance) / 0.3, 1.0)
        return Signal("order_flow_imb", imbalance, norm, direction, confidence)