"""
Amplitude Signals v12.1 - FIX deques grandes
"""
import time
from collections import deque

import aiohttp

from models import Direction, Signal
from orderbook import SimpleOBM


# 1. Book Depth
def calculate_book_depth(book: SimpleOBM, levels: int = 50) -> Signal:
    bids, asks = book.get_clean_book(levels)
    if len(bids) < 5 or len(asks) < 5:
        return Signal("book_depth", 0, 0, Direction.NEUTRAL, 0)
    tb = sum(q for _, q in bids[:10])
    ta = sum(q for _, q in asks[:10])
    ab = sum(q for _, q in bids)
    aa = sum(q for _, q in asks)
    if ab <= 0 or aa <= 0:
        return Signal("book_depth", 0, 0, Direction.NEUTRAL, 0)
    ratio = ((tb / ab) + (ta / aa)) / 2
    if ratio < 0.4:
        norm = 1.0
    elif ratio > 0.7:
        norm = 0.2
    else:
        norm = 1.0 - ((ratio - 0.4) / 0.3) * 0.8
    return Signal("book_depth", ratio, norm, Direction.NEUTRAL, 0.7)


# 2. Book Elasticity
class OrderBookElasticity:
    def calculate(self, book: SimpleOBM, price: float) -> Signal:
        if price <= 0:
            return Signal("book_elasticity", 0, 0, Direction.NEUTRAL, 0)
        bids, asks = book.get_clean_book(20)
        if not bids or not asks:
            return Signal("book_elasticity", 0, 0, Direction.NEUTRAL, 0)
        target = 100_000.0
        bi = ai = acc = 0.0
        for p, q in bids:
            acc += p * q
            if acc >= target:
                bi = (price - p) / price
                break
        acc = 0.0
        for p, q in asks:
            acc += p * q
            if acc >= target:
                ai = (p - price) / price
                break
        impact = (bi + ai) / 2
        return Signal("book_elasticity", impact, min(max(impact * 100, 0), 1.0),
                      Direction.NEUTRAL, 0.8)


# 3. ATR Expansion
class ATRExpansion:
    def __init__(self, window: int = 14):
        self.window = window
        self.prices: deque = deque(maxlen=window * 4)

    def add_price(self, ts: float, price: float):
        self.prices.append((ts, price))

    def calculate(self) -> Signal:
        n = self.window
        if len(self.prices) < n * 2 + 1:
            return Signal("atr_expansion", 0, 0, Direction.NEUTRAL, 0)
        ps = [p for _, p in self.prices]
        try:
            cur = sum(abs(ps[-i] - ps[-i - 1]) for i in range(1, n + 1)) / n
            prev = sum(abs(ps[-n - i] - ps[-n - i - 1]) for i in range(1, n + 1)) / n
        except IndexError:
            return Signal("atr_expansion", 0, 0, Direction.NEUTRAL, 0)
        if prev <= 0:
            return Signal("atr_expansion", 0, 0, Direction.NEUTRAL, 0)
        exp = (cur - prev) / prev * 100
        norm = 1.0 if exp > 30 else (0.7 if exp > 15 else (0.4 if exp > 0 else 0.1))
        return Signal("atr_expansion", exp, norm, Direction.NEUTRAL, 0.6)


# 4. Realized Volatility - 🔧 FIX: reducir maxlen
class RealizedVolatility:
    def __init__(self, window: int = 300):
        self.window = window
        self.prices: deque = deque(maxlen=2000)  # 🔧 FIX: antes 1000
        # FIX v14.1 C1 (b): ultima estimacion de vol por horizonte de decision.
        # El motor las usa para expected_move en USD REALES. Se inicializan con
        # un suelo conservador para que el primer tick no divida por cero ni
        # infle amplitud.
        self.last_vol_horizon_pct = 0.0
        self.last_vol_horizon_usd = 0.0

    def add_price(self, ts: float, price: float):
        self.prices.append((ts, price))

    def calculate(self) -> Signal:
        if len(self.prices) < 60:
            return Signal("realized_vol", 0, 0, Direction.NEUTRAL, 0)
        now = time.time()
        recent = [(t, p) for t, p in self.prices if t >= now - self.window]
        if len(recent) < 30:
            return Signal("realized_vol", 0, 0, Direction.NEUTRAL, 0)
        prices = [p for _, p in recent]
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        vol_step_pct = variance ** 0.5 * 100
        # FIX v14.1 C1 (a): la señal debe ser VOLATILIDAD POR UNIDAD DE TIEMPO.
        # Antes se normalizaba el PASO a paso contra 0.5% fijo: con feed vivo
        # (~2 ticks/seg) una vela muerta de 2 USD emitia realized_vol=0.97 y
        # sostenia amp~0.5 -> el gate de amplitud estaba SIEMPRE abierto en un
        # mercado plano (amp media del log: 0.33). Ahora: desviacion por raiz
        # del tiempo (minimo 1s de ventana efectiva), escalada al horizonte de
        # decision (60s) y normalizada contra un regimen de referencia del BTC
        # (0.15% en 1min ~ jornada tranquila; 0.5%/min ~ movimiento serio).
        span = max(recent[-1][0] - recent[0][0], 1.0)
        horizon = 60.0
        vol_horizon_pct = vol_step_pct * ((horizon / span) ** 0.5)
        # FIX v14.1 C1 (b): el valor crudo de la senal pasa a ser la vol en %
        # por el horizonte de decision, y exponemos la version en USD para que
        # el motor calcule expected_move REAL (antes `amp*100` era una ficcion
        # dimensional: exp=33$ con amp media 0.33 -> gate siempre vivo).
        self.last_vol_horizon_pct = vol_horizon_pct
        self.last_vol_horizon_usd = vol_horizon_pct / 100.0 * prices[-1]
        norm = min(vol_horizon_pct / 0.5, 1.0)
        return Signal("realized_vol", vol_horizon_pct, norm, Direction.NEUTRAL, 0.8)


# 5. Volume Spike Detector
class VolumeSpikeDetector:
    def __init__(self, window: int = 60):
        self.window = window
        self.volumes: deque = deque(maxlen=5000)  # 🔧 FIX: antes 200

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.volumes.append((ts, qty))

    def calculate(self) -> Signal:
        if len(self.volumes) < 100:
            return Signal("volume_spike", 0, 0, Direction.NEUTRAL, 0)
        now = time.time()
        recent_vol = sum(q for t, q in self.volumes if t >= now - 10)
        avg_vol = sum(q for t, q in self.volumes if t >= now - self.window) / 6
        if avg_vol == 0:
            return Signal("volume_spike", 0, 0, Direction.NEUTRAL, 0)
        spike_ratio = recent_vol / avg_vol
        norm = min((spike_ratio - 1.0) / 2.0, 1.0) if spike_ratio > 1.0 else 0.0
        return Signal("volume_spike", spike_ratio, norm, Direction.NEUTRAL, 0.7)


# 6. Volatility Ratio - 🔧 FIX: reducir maxlen
class VolatilityRatio:
    def __init__(self):
        self.prices: deque = deque(maxlen=3000)  # 🔧 FIX: antes 10000

    def add_price(self, ts: float, price: float):
        self.prices.append((ts, price))

    def calculate(self) -> Signal:
        if len(self.prices) < 300:
            return Signal("volatility_ratio", 0, 0, Direction.NEUTRAL, 0)
        now = time.time()
        short_prices = [p for t, p in self.prices if t >= now - 60]
        if len(short_prices) < 30:
            return Signal("volatility_ratio", 0, 0, Direction.NEUTRAL, 0)
        short_returns = [(short_prices[i] - short_prices[i-1]) / short_prices[i-1]
                        for i in range(1, len(short_prices))]
        short_vol = (sum(r**2 for r in short_returns) / len(short_returns)) ** 0.5
        long_prices = [p for t, p in self.prices if t >= now - 300]
        long_returns = [(long_prices[i] - long_prices[i-1]) / long_prices[i-1]
                       for i in range(1, len(long_prices))]
        long_vol = (sum(r**2 for r in long_returns) / len(long_returns)) ** 0.5
        if long_vol == 0:
            return Signal("volatility_ratio", 0, 0, Direction.NEUTRAL, 0)
        ratio = short_vol / long_vol
        norm = min(abs(ratio - 1.0) / 0.5, 1.0)
        return Signal("volatility_ratio", ratio, norm, Direction.NEUTRAL, 0.7)