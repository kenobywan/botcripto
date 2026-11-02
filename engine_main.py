"""
EngineMain v14.8 - + senal anticipativa funding_pressure, oi_delta reclasificada (tabla OI x precio), ls_ratio anti-spread
  - Bloque ANTICIPATIVO dirección (6): book_slope, wall_momentum, ofi_weighted,
    micro_price_dev, cvd_divergence, trade_arrival
  - Bloque ANTICIPATIVO amplitud (3): liquidity_thinning, spread_compression,
    volume_accel
  - Confluencia de precursores + amortiguación de contradicciones
FIX v13.8 (auditoría de calidad de las señales anticipativas y sus flujos):
  - signals/anticipation.py REESCRITO: 4 fallos de flujo (F1-F4) y 8 de cálculo
    (C1-C8) corregidos -> ver cabecera del módulo. Los más graves:
      F1 micro_dev se calculaba 2x por tick (EMA contaminada)
      C7 hint de liquidity_thinning INVERTIDO (bids finos daba LONG, es SHORT)
      C4 trade_arrival exigía racha de 8 takers (imposible: señal siempre 0)
      F4 volume_accel base incluía el burst propio (techo ~1.15x -> siempre 0)
      C3 cvd_divergence normalizador fijo 40 BTC (ciego según régimen)
      C8 spread_compression puntuaba amplitud con spread ANCHO
  - Flujo de libro: UN get_clean_book(30) compartido via cache last_clean
    (antes cada señal re-ordenaba el libro: 6x O(L log L) por delta WS)
  - amp_zombie en el LOG: la columna amp_score ahora registra amp_EFECTIVA
    (post-feed-gate), no la cruda -> un feed mudo ya no puede escribir 0.85
    fantasma en el CSV (log 23:50: HOLDs amp=0.85 con price congelado)
  - Auditoría STRONG_DIR: todo HOLD explica además ant/react/ampE/px20s$
    (movimiento real de precio que el override exige desde v13.7)
FIX v13.6 (reglas extraídas del análisis de logs en vivo 23:50-04:10):
  - amp_zombie: si el feed murió (>max_signal_age_sec), la amplitud se fuerza a
    0 -> HOLD con motivo explícito "feed_mudo(Xs)" en vez de amp=0.85 congelado
  - STRONG_DIR override: |dir|>=0.70 con >=5 señales alineadas ABRE SIN exigir
    amplitud (los ~35 HOLDs dir=-0.8/amp=0.06 regalaban tendencias enteras)
  - Contradicción asimétrica real: para bloquear, el reactor debe alcanzar
    max(umbral_fijo, |ant|*0.8) -> react=-0.73 ya no tumba a ant=+1.00
  - Ambigüedad: ant y react opuestos ambos >=0.5 con fuerza parecida = sin
    tesis -> ni abre ni cierra por reversión (corta el churn de fees)
  - REVERSAL_ANT endurecido: exige >=2 precursores ANTICIPATIVOS puros en
    contra frescos Y |d_score| opuesto >=0.45 (antes bastaba oscilar +-0.1)
  - Auditoría: cada HOLD loguea fa(edad feed) y ant_vivos() cuando no hay
    precursores frescos (el log anterior no permitía diagnosticar el porqué)
FIX v13.4 (calidad de entrada/salida):
  - taker_ratio: normalized era `ratio`>=0 -> sesgo LONG estructural (corregido)
  - absorption: sentido invertido + ventana temporal 60s (antes datos de horas)
  - vwap_distance: sentido de reversión corregido (era trend-following disfrazado)
  - atr_expansion publicado cada loop (antes NUNCA se publicaba: bloque amp sin 1 señal)
  - micro_price_dev recalculado en CADA tick de precio (antes solo cada 1s => TTL 6s lo mataba)
  - Gate de consistencia: no abrir si el bloque reactivo niega fuerte al anticipativo
FIX congelamiento (v12.6):
  - Señales ligeras calculadas de forma SYNC (sin lock/timeout que rompía el bus)
  - Broadcast web desacoplado del loop (fire-and-forget + coalescing)
  - Watchdog asyncio en vez de bloqueo en bucle vacío (price_5min_ago)
  - Heartbeat sin bloquear el event loop (buffer + escritura en hilo)
  - REST con aiohttp + cierre garantizado de sesiones (sin sockets colgados)
  - WS con reenvío automático de pings Bybit y reconexión si se estanca
"""
import asyncio
import json
import time
import gc
import os
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from config import Config
from engine import (SignalBus, calculate_engine, calculate_directional,
                    calculate_amplitude, DIR_WEIGHTS, AMP_WEIGHTS,
                    ANTICIP_DIR_WEIGHTS)
from logger import SignalLogger
from models import Direction, TradeDecision
from orderbook import SimpleOBM
# FIX v14.1 D1: RiskManager existia pero NUNCA se instanciaba ni usaba (modulo
# muerto sin limite diario / perdidas consecutivas / kill-switch).
from risk_manager import RiskManager
from rest_client import BybitRestClient
from signals import (
    calculate_obi,
    LargeTradeDetector, AbsorptionDetector,
    OIDeltaTracker, LongShortRatioTracker, FundingRateTracker, TakerBuySellRatio,
    CVDCalculator, VWAPDistance, CandleDelta, OrderFlowImbalance,
    calculate_book_depth, OrderBookElasticity, ATRExpansion,
    RealizedVolatility, VolumeSpikeDetector, VolatilityRatio,
    # v12.8 señales anticipativas
    BookSlope, WallMomentum, WeightedOFI, MicroPriceDeviation,
    CVDDivergence, TradeArrivalRun,
    LiquidityThinning, SpreadCompression, VolumeAcceleration,
)
from websocket_client import BybitWebSocket
from web_server import (start_web_server, bot_state, broadcast_state,
                        broadcast_price_tick, get_status)


class EngineMain:
    def __init__(self, config: Config):
        self.cfg = config
        self.bus = SignalBus()
        self.ws = BybitWebSocket(config.ws_public_url)
        self.rest = BybitRestClient(config)
        self.logger = SignalLogger()
        self.book = SimpleOBM(config.obi_lifetime_min_ms)
        # FIX v14.1 D1: RiskManager vivo (antes import inexistente y modulo muerto).
        # Aporta: limite de perdida diaria, max trades/dia, stop por perdidas
        # consecutivas y sizing dinamico opcional. El estado persiste en
        # state/risk_state.json (reset diario automatico UTC).
        self.risk = RiskManager(config)

        self.large = LargeTradeDetector(config.large_trade_usd_threshold)
        # FIX v13.4: ventana temporal de 60s (antes "100 trades" sin recorte por
        # tiempo = datos de horas -> señal anclada y sentido además invertido)
        self.absorption = AbsorptionDetector(window_sec=60.0)
        self.taker = TakerBuySellRatio()
        self.candle_delta = CandleDelta()
        self.oi = OIDeltaTracker(config.rest_url, config.symbol, config.oi_delta_window_sec)
        self.ls_ratio = LongShortRatioTracker(config.rest_url, config.symbol)
        # v14.8: señal anticipativa de funding (predictedFundingFee via ticker REST)
        self.funding = FundingRateTracker(config.rest_url, config.symbol)
        self.elasticity = OrderBookElasticity()
        self.atr = ATRExpansion()
        self.realized_vol = RealizedVolatility()
        self.volume_spike = VolumeSpikeDetector()
        self.volatility_ratio = VolatilityRatio()

        self.cvd = CVDCalculator()
        self.vwap = VWAPDistance()
        self.ofi = OrderFlowImbalance()

        # ---- v12.8 señales anticipativas (dirección) ----
        self.book_slope = BookSlope()
        self.wall_momentum = WallMomentum()
        self.wofi = WeightedOFI()
        self.micro_dev = MicroPriceDeviation()
        self.cvd_div = CVDDivergence()
        self.arrival_run = TradeArrivalRun()
        # ---- v12.8 señales anticipativas (amplitud) ----
        self.liq_thinning = LiquidityThinning()
        self.spread_comp = SpreadCompression()
        self.vol_accel = VolumeAcceleration(burst_sec=30.0)  # FIX v14.1 C2: antes la clase ignoraba el param y hardcodeaba 30; ahora se pasa explicitamente

        self._last_heavy_calc = 0.0
        self._heavy_calc_interval = 1.0
        self._last_book_signals = 0.0     # throttle de las señales de libro (1s)

        self._thread_pool = ThreadPoolExecutor(max_workers=2)
        self._ws_session: aiohttp.ClientSession | None = None
        self._heartbeat_buffer: deque = deque(maxlen=120)

        self.last_price = 0.0
        self.price_5min_ago = 0.0
        self.price_history = deque(maxlen=5000)
        self._trades_batch_count = 0
        self._micro_trades_seen = -1
        self._processing = False
        self._processing_since = 0.0
        self._last_close_time = 0.0
        self.last_decision = "HOLD"
        self.bot_state = "IDLE"
        self.last_signal_summary = ""
        self._last_reconcile = 0.0

        self._entry_time = None
        self._entry_price = None
        self._entry_signals = None
        self._entry_side = None

        self.prev_price = 0.0
        self._last_price_update = 0.0
        self._live_upnl = 0.0
        # FIX v13.3: máximo PnL favorable de la posición actual (para el corte
        # por "giveback": si regalas >55% del mejor momento, cierras con lo que hay)
        self._max_favorable_move = 0.0
        # FIX v14.2 (runner): deadline efectivo de la posicion. Arranca en
        # entry+max_trade_duration_sec; si al vencerlo la direccion sigue viva
        # se extiende en bloques hasta entry+max_runner_duration_sec (1h).
        self._position_deadline = None
        self._extension_count = 0
        # FIX v14.5: techo absoluto del runner como TIMESTAMP ABSOLUTO. Antes
        # se calculaba como entry+hard_cap; para posiciones huerfanas restauradas
        # (age inflado/0) eso podia quedar en el PASADO y desactivar toda
        # extension, o permitir que una huerfana renaciera con 2h nuevas.
        self._runner_hard_cap_ts = None

        self._last_loop_time = time.time()
        self._last_ws_message = time.time()
        self._loop_count = 0
        self._slow_loop_count = 0
        self._last_snapshot_write = 0.0

    def _check_processing_timeout(self):
        if self._processing and time.time() - self._processing_since > 5.0:
            self._processing = False

    async def run(self, stop_event: asyncio.Event):
        # FIX v13.2: start_web_server() ahora se llama DENTRO de este coroutine,
        # así detecta el loop en ejecución y monta uvicorn en él (no en un hilo
        # con otro loop -> era la causa del precio congelado).
        server_task = start_web_server()
        print(f"\n🚀 BTC/USDT v14.8 | {'DEMO' if self.cfg.demo_mode else 'LIVE'}")
        print(f"📊 25 señales (16 clásicas + 9 anticipativas) | Heartbeat + Emergencia ACTIVOS\n")

        subs = [f"orderbook.200.{self.cfg.symbol}",
                f"publicTrade.{self.cfg.symbol}",
                f"tickers.{self.cfg.symbol}"]
        self.ws.on(f"orderbook.200.{self.cfg.symbol}", self._on_book)
        self.ws.on(f"publicTrade.{self.cfg.symbol}", self._on_trade)
        self.ws.on(f"tickers.{self.cfg.symbol}", self._on_ticker)

        tasks = [
            asyncio.create_task(self.ws.connect(subs)),
            asyncio.create_task(self._loop(stop_event)),
            asyncio.create_task(self._background(stop_event)),
            asyncio.create_task(self._health_monitor(stop_event)),
            asyncio.create_task(self._heartbeat_writer(stop_event)),
            # FIX v13.5: el health monitor solo corría si Bybit dejaba el socket
            # "medio abierto" (conexión viva pero muda). Si la conexión FALLABA
            # (sin internet/DNS), ws_age subía y subía sin que nadie avisara ni
            # probara nada -> precio congelado 0.0 en silencio. Este watchdog
            # avisa, fuerza reconexiones e intenta recuperar el precio vía REST.
            asyncio.create_task(self._feed_watchdog(stop_event)),
        ]
        if server_task is not None:
            tasks.append(server_task)
        try:
            await stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await self.ws.close()
            if self._ws_session is not None and not self._ws_session.closed:
                await self._ws_session.close()
            self._thread_pool.shutdown(wait=False)
            print("✅ Bot detenido.")

    # ---- helpers anti-congelamiento ----
    def _publish_sync(self, signal):
        """Publicación SIN lock/awaits: el loop de eventos no puede bloquearse aquí."""
        self.bus.publish_nowait(signal)

    def _broadcast_price_soon(self, price: float, change: float, upnl: float):
        """Fire-and-forget con coalescing: nunca espera a los clientes web."""
        bot_state.last_price = price
        bot_state.price_change = change
        bot_state.price_update_time = time.time()
        bot_state.account.unrealized_pnl = upnl
        now = time.time()
        if now - bot_state._last_price_broadcast < bot_state._price_broadcast_interval:
            # FIX v13.1 (precio congelado): si el tick se programa DENTRO del
            # intervalo de throttle y no hay tarea pendiente, antes se descartaba
            # y no volvía a emitirse nada hasta el siguiente tick que cayera justo
            # tras el límite. Con feeds irregulares (WS caído + polling REST) eso
            # dejaba el precio del navegador clavado varios segundos. Ahora se
            # programa la emisión restante para cuando venza el cooldown.
            if getattr(bot_state, "_price_task", None) is None or bot_state._price_task.done():
                delay = bot_state._price_broadcast_interval - (now - bot_state._last_price_broadcast)
                try:
                    loop = asyncio.get_running_loop()
                    bot_state._price_task = loop.create_task(
                        self._delayed_price_broadcast(max(0.05, delay)))
                except RuntimeError:
                    pass
            return
        bot_state._last_price_broadcast = now
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        # evitar acumular tareas pendientes
        if getattr(bot_state, "_price_task", None) is not None and not bot_state._price_task.done():
            return
        bot_state._price_task = loop.create_task(broadcast_price_tick())

    async def _delayed_price_broadcast(self, delay: float):
        await asyncio.sleep(delay)
        bot_state._last_price_broadcast = time.time()
        try:
            await broadcast_price_tick()
        except Exception:
            pass

    async def _on_book(self, data):
        self._last_ws_message = time.time()
        msg = data.get("data", {}) or {}
        ts = data.get("ts", time.time() * 1000) / 1000.0
        if msg.get("type") == "snapshot":
            self.book.apply_snapshot(msg.get("b", []), msg.get("a", []), ts)
        else:
            self.book.apply_delta(msg.get("b", []), msg.get("a", []), ts)
        # FIX: cálculo directo y sin awaits -> un book delta no puede congelar el loop
        try:
            self._publish_sync(calculate_obi(self.book, self.cfg.obi_levels))
            self._publish_sync(calculate_book_depth(self.book, self.cfg.book_depth_levels))
            if self.last_price > 0:
                self._publish_sync(self.elasticity.calculate(self.book, self.last_price))
        except Exception:
            pass

    async def _on_trade(self, data):
        self._last_ws_message = time.time()
        for t in data.get("data", []) or []:
            ts = t.get("T", time.time() * 1000) / 1000.0
            price = float(t.get("p", 0) or 0)
            qty = float(t.get("v", 0) or 0)
            # FIX v14.1 A1 (convencion de agresor INVERTIDA): en Bybit v5 el campo
            # "S" es el lado del TAKER/agresor, no del maker. Antes S=="Sell" se
            # marcaba maker=True => todo el stack de flujo (CVD, trade_arrival,
            # cvd_divergence, volume_spike, candle_delta...) leia el flujo al reves
            # (racha de compradores agresivos => senal SHORT). Un trade con
            # agresor comprador (S=="Buy") es un buy del taker.
            maker = t.get("S", "") == "Buy"
            if price <= 0:
                continue

            self.large.add_trade(ts, price, qty, maker)
            self.absorption.add_trade(ts, price, qty, maker)
            self.taker.add_trade(ts, price, qty, maker)
            self.candle_delta.add_trade(ts, price, qty, maker)
            self.volume_spike.add_trade(ts, price, qty, maker)
            self.cvd.add_trade(ts, price, qty, maker)
            self.vwap.add_trade(ts, price, qty, maker)
            self.ofi.add_trade(ts, price, qty, maker)
            # v12.8 anticipativas (solo ingest, O(1) por trade)
            self.cvd_div.add_trade(ts, price, qty, maker)
            self.arrival_run.add_trade(ts, price, qty, maker)
            self.vol_accel.add_trade(ts, price, qty, maker)

            self._trades_batch_count += 1  # v13.7: marca que hubo flow nuevo (micro_dev)
            self.prev_price = self.last_price
            self.last_price = price
            self._last_price_update = time.time()
            self.price_history.append((ts, price))

            # FIX: era un `while` que vaciaba el deque por completo (O(n) en cada
            # trade y se colgaba si entraban mensajes con ts desordenado/atrasado).
            # Ahora solo se descartan entradas viejas y se guarda la más antigua.
            cutoff = ts - 300
            dropped = False
            while self.price_history and self.price_history[0][0] < cutoff:
                _, self.price_5min_ago = self.price_history.popleft()
                dropped = True
            if dropped and len(self.price_history) >= 2:
                self.price_5min_ago = self.price_history[0][1]

            self.realized_vol.add_price(ts, price)
            self.volatility_ratio.add_price(ts, price)
            self.atr.add_price(ts, price)
            self._update_live_upnl()

            price_change = price - self.prev_price if self.prev_price > 0 else 0.0
            # FIX: ya no hacemos `await update_price_only(...)` dentro del handler:
            # el broadcast iba en línea con cada trade y podía atorar el dispatch WS.
            self._broadcast_price_soon(price, price_change, self._live_upnl)

    async def _on_ticker(self, data):
        self._last_ws_message = time.time()
        d = data.get("data", {}) or {}
        if d.get("lastPrice"):
            new_price = float(d["lastPrice"])
            self.prev_price = self.last_price
            self.last_price = new_price
            self._last_price_update = time.time()
            self._update_live_upnl()
            price_change = new_price - self.prev_price if self.prev_price > 0 else 0.0
            self._broadcast_price_soon(new_price, price_change, self._live_upnl)

    def _update_live_upnl(self):
        if (self._entry_time is not None and self._entry_price is not None
                and self._entry_side is not None):
            sign = 1 if self._entry_side == "Buy" else -1
            move = (self.last_price - self._entry_price) * sign
            # FIX v13.3: registrar el mejor momento de la posición
            if move > self._max_favorable_move:
                self._max_favorable_move = move
            self._live_upnl = move * self.cfg.base_order_size_btc
            self.rest.account_info.unrealized_pnl = self._live_upnl
            if self._entry_price > 0:
                self.rest.account_info.unrealized_pnl_pct = (
                    (self.last_price - self._entry_price) / self._entry_price * 100 * sign)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Una sola sesión aiohttp reutilizada (antes se creaba/cerraba en cada llamada)."""
        if self._ws_session is None or self._ws_session.closed:
            timeout = aiohttp.ClientTimeout(total=8, connect=5)
            self._ws_session = aiohttp.ClientSession(timeout=timeout)
        return self._ws_session

    async def _background(self, stop_event: asyncio.Event):
        try:
            await self.rest.fetch_all()
            await self.oi.update(session_factory=self._get_session)
            await self.ls_ratio.update(session_factory=self._get_session)
            await self.funding.update(session_factory=self._get_session)
            print(f"💰 Balance: ${self.rest.account_info.balance_usdt:,.2f}\n")
            while not stop_event.is_set():
                await asyncio.sleep(self.cfg.rest_poll_sec)
                try:
                    await asyncio.wait_for(self.rest.fetch_all(), timeout=10.0)
                    await asyncio.wait_for(
                        self.oi.update(session_factory=self._get_session), timeout=5.0)
                    await asyncio.wait_for(
                        self.ls_ratio.update(session_factory=self._get_session), timeout=5.0)
                    await asyncio.wait_for(
                        self.funding.update(session_factory=self._get_session), timeout=5.0)
                except asyncio.TimeoutError:
                    print("⏱️  Background poll timeout (no bloquea el loop principal)")
                except Exception as e:
                    print(f"⚠️  Background: {e}")
        finally:
            if self._ws_session is not None and not self._ws_session.closed:
                await self._ws_session.close()

    def _reconcile(self):
        bybit_side = self.rest.account_info.position_side
        if bybit_side == "None" and self._entry_time is not None:
            self._entry_time = None
            self._entry_price = None
            self._entry_side = None
            self._entry_signals = None
            self._last_close_time = time.time()
        elif bybit_side != "None" and self._entry_time is None:
            # FIX v14.4 (huérfana): si Bybit tiene posición y el bot no (p. ej.
            # reinicio del proceso con la posición abierta), se restauran los
            # contadores internos usando el timestamp REAL de entrada del
            # exchange (position_idx.updatedTime), no time.time(): poner now
            # falseaba la duración (el runner/TIMEOUT contarían desde 0 aunque
            # la posición llevara horas) y dejaba _position_deadline=None, lo
            # que desactivaba silenciosamente las extensiones del runner
            # (can_extend exige deadline no-None). Si el REST no da updatedTime
            # (campo ausente/0) se usa now como último recurso.
            entry_px = self.rest.account_info.entry_price or self.last_price
            pos_ts = getattr(self.rest.account_info, "position_updated_ts", 0.0) or 0.0
            now_r = time.time()
            if pos_ts <= 0 or pos_ts > now_r + 5:
                pos_ts = now_r   # sin dato fiable: empezar a contar ahora
            # FIX v14.5 (huérfana >1h): updatedTime del exchange es el ultimo
            # cambio de la posicion (parcial, TP/SL), no la hora de apertura;
            # y si REST aun no lo ha rellenado (poll de 30s) caia en now ->
            # duration=0. En ambos casos una huérfana de 2h renacia como recien
            # abierta y el runner le regalaba hasta 2h nuevas. Techo honesto:
            # una posición restaurada solo puede estirarse hasta
            # max_runner_duration_sec DESDE la restauracion.
            runner_cap_from = now_r
            known_age = now_r - pos_ts
            if known_age > self.cfg.max_runner_duration_sec:
                # La posición ya nace mas alla del techo del runner: deadline
                # inmediato -> se cierra por TIMEOUT en el siguiente tick.
                self._position_deadline = now_r
            else:
                self._position_deadline = pos_ts + self.cfg.max_trade_duration_sec
            self._runner_hard_cap_ts = runner_cap_from + self.cfg.max_runner_duration_sec
            self._entry_time = pos_ts
            self._entry_price = entry_px
            self._entry_side = bybit_side
            self._extension_count = 0
            sign_r = 1 if bybit_side == "Buy" else -1
            self._max_favorable_move = max(
                0.0, (self.last_price - entry_px) * sign_r)

    def _make_decision(self, d_out, a_out):
        # FIX v14.1 B1 (doble filtro de confianza INCONSISTENTE): `aligned` se
        # contaba sobre d_out.signals (lista cruda del bloque), que incluye
        # señales con conf < min_signal_confidence ya descartadas del score:
        # los "ali=8" del log incluian ruido que no puntuo. Ahora aligned usa
        # EXACTAMENTE las señales que participaron en el blend. Ademas se exige
        # direccion NO NEUTRAL: wall_momentum/trade_arrival publican
        # max(conf,0.5) garantizado y una señal NEUTRAL con conf>=0.5 entraba
        # igual al conteo direccional (y podia activar strong_dir).
        # FIX v14.7 (log 03:29-03:34): el conteo seguia inflado porque admitia
        # señales DIRECCIONADAS pero CASI NEUTRAS (normalized ~0.02 con
        # confidence sintetica >=0.5: wall_momentum/trade_arrival publican
        # max(conf,0.5) garantizado). Con esas particulas el log mostraba
        # "SHORT ali=6 ant=3" mientras px20s iba +11.9$ SUBIENDO (dir_score
        # -0.50 era un espejo de reactivas tardias). Ahora solo cuentan las
        # señales con FUERZA REAL: |normalized| >= dir_min_strength a favor.
        _min_strength = getattr(self.cfg, "dir_min_strength", 0.25)
        active = [s for s in d_out.signals
                  if s.confidence >= self.cfg.min_signal_confidence
                  and s.direction != Direction.NEUTRAL
                  and abs(s.normalized) >= _min_strength
                  and s.direction == d_out.direction]
        aligned = len(active)
        combined_score = abs(d_out.score) * min(aligned, 4) / 2

        # FIX v14.1 D1 (kill-switch): RiskManager gatea la apertura ANTES de
        # evaluar cualquier otra condicion (limite diario de perdida, max
        # trades/dia, perdidas consecutivas). Sin esto el bot encadenaba
        # aperturas indefinidamente (log: 23 trades/-8.42 USD sin stop).
        risk_ok, risk_block, _risk_size = self.risk.can_open_trade(
            self.rest.account_info.balance_usdt,
            self.rest.account_info.consecutive_losses)
        if not risk_ok:
            return TradeDecision("HOLD", d_out.score, 0.0, 0.0,
                                 f"RISK_BLOCK({risk_block})", aligned), aligned

        # FIX v14.1 D3: comision con la UNICA fuente (cfg.taker_fee_rate).
        # Antes 0.0011 por lado -> break-even 2x mas caro que el PnL neto real
        # registrado al cerrar (0.00055*2), distorsionando el gate de rentabilidad.
        notional_est = self.last_price * self.cfg.base_order_size_btc
        estimated_fee = notional_est * self.cfg.taker_fee_rate * 2.0
        # FIX v14.1 C1 (gate coherente): el umbral de rentabilidad es el COSTE
        # REAL de la operacion x margen, en USD. Con fee ida+vuelta ~0.55$ y
        # margen 2x -> 1.1$: un movimiento esperado >=1.1$ si es rentable. El
        # antiguo `estimated_fee*3` con 0.0011 daba 3.3$ sobre un fee ficticio.
        # FIX v14.1.3 D1 (log 03:21-03:28): el margen 2x (~0.9$) era tan barato
        # que el gate de amplitud no filtraba nada; se sube a 3x el coste real.
        min_profitable_move = estimated_fee * self.cfg.amp_profit_margin_mult
        # FIX v14.1 C1: expected_move en USD REALES. `amp*100` era una ficcion
        # dimensional (amp media 0.33 -> "exp=33$" siempre > fee gate: la
        # amplitud no filtraba nada). Ahora: movimiento esperado ~ vol realizada
        # por horizonte de decision, con suelo de mercado muerto. Se compara
        # contra min_profitable_move (USD reales).
        # FIX v14.1.1 C1-bis (log 03:21-03:28): a) se medía la vol en el momento
        # del ULTIMO calculo publicado; si realized_vol estaba throttleado/congelado
        # se usaba un valor viejo (y al arrancar, cero -> solo el suelo de 11.6$,
        # que superaba el umbral de 1.1$ y dejaba el gate SIEMPRE vivo otra vez).
        # b) con 1 sigma del horizonte el objetivo quedaba POR DEBAJO del TP: las
        # aperturas se disparaban justo cuando px20s ya llevaba +19$/-25$ (entrar
        # en el pico local = antiicipación negativa residual). Ahora:
        #   - vol medida EN VIVO desde price_history (no depende del throttle),
        #   - objetivo = cuantil superior (z~1.28 = percentil 90) escalado al
        #     horizonte real hasta take_profit (~30s), coherente con TP 10-12$.
        _horizon_sec = self.cfg.amp_expected_horizon_sec
        vol_usd_60s = 0.0
        try:
            _nowt = time.time()
            _hist = [(t, p) for t, p in self.price_history if t >= _nowt - 120.0]
            if len(_hist) >= 30:
                _rets = [(_hist[i][1] - _hist[i-1][1]) / _hist[i][1]
                         for i in range(1, len(_hist))]
                _mean = sum(_rets) / len(_rets)
                _var = sum((r - _mean) ** 2 for r in _rets) / len(_rets)
                _span = max(_hist[-1][0] - _hist[0][0], 1.0)
                _vol60_pct = (_var ** 0.5) * ((_horizon_sec / _span) ** 0.5) * 100.0
                vol_usd_60s = _vol60_pct / 100.0 * _hist[-1][1]
        except Exception:
            vol_usd_60s = 0.0
        if vol_usd_60s <= 0.0:
            # fallback: ultima estimacion publicada por RealizedVolatility
            vol_usd_60s = getattr(self.realized_vol, "last_vol_horizon_usd", 0.0)
        # FIX v14.1.2 C1-ter (log 03:21-03:28): el suelo `amp_vol_floor_pct` se
        # interpreto como % del precio -> 0.015% x 77350 = 11.6 USD. En mercado
        # PLANO expected_move=11.6$ SIEMPRE > umbral de rentabilidad (~0.9$):
        # el gate quedaba estructuralmente vivo, la otra mitad del bug C1. El
        # suelo debe ser una vol minima medible (0.002%/min ~ ruido tick), no un
        # "objetivo" disfrazado; ademas nunca por debajo del break-even real.
        floor_usd = self.last_price * (self.cfg.amp_vol_floor_pct / 100.0)
        _target_scale = (self.cfg.amp_expected_target_horizon_sec / _horizon_sec) ** 0.5
        expected_move = max(vol_usd_60s * _target_scale * self.cfg.amp_expected_quantile_z,
                            floor_usd) * self.cfg.amp_to_usd_multiplier
        expected_move = max(expected_move, min_profitable_move)

        meta = getattr(d_out, "metadata", None) or {}
        react_score = float(meta.get("react_score", 0.0))
        anticip_score = float(meta.get("anticip_score", 0.0))

        # FIX v13.6 CRÍTICO (amp_zombie): a_out.score provenía de señales de
        # amplitud CONGELADAS. En el log 23:50 se ven HOLDs con amp=0.85 y
        # price idéntico durante minutos: realized_vol/volume_spike/etc. tienen
        # TTL largo y seguían puntuando aunque ya no se publicaban. Ahora la
        # amplitud efectiva exige feed vivo; si el feed murió, amp=0 -> amp_gate
        # bloquea aperturas y el dashboard ve 0.00 en vez de un 0.85 falso.
        feed_age_now = self.bus.feed_age()
        amp_effective = 0.0 if feed_age_now > self.cfg.max_signal_age_sec else a_out.score

        # FIX v13.4 CRÍTICO (no entra bien long/short): gate de CONSISTENCIA.
        # Antes bastaba un blend > 0.25 aunque las capas se contradijeran: con
        # anticipación +0.9 y reacción -0.6 el blend seguía positivo y el bot
        # abrían contra la mitad del mercado. Ahora: si el bloque reactivo NIEGA
        # fuerte al anticipativo (signos opuestos y ambos >0.3), no se abre.
        # FIX v13.5 (umbral dinámico, caso real 04:04:12 ant=+1.00/react=-0.76
        # -> HOLD eterno durante una caída): contradecir a un precursor MUY
        # FUERTE exige una reacción aún más fuerte. Con precursores >=0.8 en
        # contra hacen falta reacciones >=0.85 para bloquear; así los cruces
        # legítimos (reactiva tardía girando) no anulan al líder anticipativo.
        if abs(anticip_score) >= 0.8:
            react_block_thr = 0.85
        elif abs(anticip_score) >= 0.5:
            react_block_thr = 0.55
        else:
            react_block_thr = 0.30
        # FIX v13.6 (regla 4 del análisis): una reacción débil no puede anular
        # a un precursor fuerte. Caso real 23:53: ant=+1.00 / react=-0.73 ->
        # HOLD eterno mientras el mercado caía; y 23:52 ant=-1.00/react=+0.36
        # bloqueaba con una reacción ridícula. La regla de 0.85/0.85 era
        # simétrica-invertida: exigía mucho al REACTOR solo cuando el
        # anticipativo era fuerte, pero bastaba react=-0.73 para tumbar
        # ant=+1.00 porque el umbral fijo 0.3 del anticipativo ya se cumplía.
        # Ahora el umbral del reactor es SIEMPRE >= |ant|*0.8: contradecir al
        # líder exige estar casi tan fuerte como él.
        react_block_thr = max(react_block_thr, abs(anticip_score) * 0.8)
        contradiction = (anticip_score * react_score < 0
                         and abs(anticip_score) >= 0.3
                         and abs(react_score) >= max(0.3, react_block_thr))

        # FIX v13.6 (regla 4b): AMBIGUEDAD. Si ambos bloques se contradicen con
        # fuerza parecida, NO HAY TESIS: ni entrada nueva ni cierre por reversión
        # anticipada. En el log 23:52-23:53 el bot alternaba aperturas LONG con
        # cierres REVERSAL en plena zona lateral (churn pagando fees). La
        # ambigüedad se resuelve con QUIETUD, no con el lado que salió sorteo.
        # FIX v13.6.1 (test unitario CASO C): la banda de paridad <=0.35 era tan
        # ancha que ant=+0.9/react=-0.60 caía "ambigua" y bloqueaba al LÍDER
        # claro -> HOLD eterno con convicción del 90%. Ahora solo es ambigüedad
        # si NINGUNO domina: ambos >= umbral Y el líder supera al rezagado por
        # menos de 0.2 (paridad real). Con diferencia >=0.2 manda el líder y el
        # gate de contradicción decide.
        _ant_abs, _react_abs = abs(anticip_score), abs(react_score)
        ambiguity = (anticip_score * react_score < 0
                     and _ant_abs >= self.cfg.ambiguity_min_both
                     and _react_abs >= self.cfg.ambiguity_min_both
                     and abs(_ant_abs - _react_abs) < self.cfg.ambiguity_leader_margin)

        # FIX v14.6 CRÍTICO (log 03:29-03:34): fresh_map se sacaba de
        # get_fresh(max_signal_age_sec=4.0s), pero 3 de los 6 precursores se
        # publican con THROTTLE DE 1s dentro del propio loop de ~2.5s: entre
        # dos publicaciones su edad real es 2.5-3.5s y cruzaban el corte de
        # 4s JUSTO A TIEMPO de no contar nunca. Resultado medido en el log:
        # precursor_fresco(1<3) o (2<3) en ~90% de los ticks -> el bot casi
        # jamás abría pese a dir=+0.77/-0.73 con 8 señales alineadas.
        # Ahora la ventana de frescura del gate es max(signal_age,
        # gate_window_sec) — suficiente para cubrir un ciclo de throttle sin
        # dejar pasar microestructura realmente vieja.
        _fresh_win = max(self.cfg.max_signal_age_sec,
                         getattr(self.cfg, "fresh_gate_window_sec", 6.0))
        fresh_map = self.bus.get_fresh(_fresh_win)
        # FIX v14.7 (log 03:29-03:34): el gate de confluencia contaba señales
        # frescas por `confidence`, pero wall_momentum/trade_arrival publican
        # conf=max(conf,0.5) garantizado aunque su normalized sea ~0 -> el
        # "ant=3" del log era en parte artano. Ahora se exige la MISMA fuerza
        # minima (|normalized| >= dir_min_strength) que en `aligned`.
        fresh_anticip_aligned = sum(
            1 for name, s in fresh_map.items()
            if name in ANTICIP_DIR_WEIGHTS
            and s.confidence >= self.cfg.min_signal_confidence
            and abs(s.normalized) >= _min_strength
            and s.direction == d_out.direction)

        # FIX v13.6 CRÍTICO (regla 1 del análisis de logs): la dirección FUERTE
        # por sí sola abre, sin exigir amplitud. En el log 23:50-04:10 hay ~35
        # HOLDs con dir=-0.70..-0.87 y amp=0.06 -> el bot "veía" la caída pero
        # no entraba; luego re-entraba tarde en el rebote (perder-perder).
        # FIX v13.7 (log 00:12: LONGs con STRONG_DIR @amp=0.05-0.10 MIENTRAS el
        # precio BAJABA 77329->77236): el override era demasiado barato. Ahora
        # exige además (a) amplitud mínima > ruido y (b) MOVIMIENTO REAL DEL
        # PRECIO A FAVOR en los últimos 20s. Un score direccional alto con
        # libro plano y precio quieto o contradiciéndolo es sesgo de señales
        # correlacionadas, no una tesis: no se abre contra él.
        strong_dir_override = False
        if (abs(d_out.score) >= self.cfg.strong_dir_override_score
                and aligned >= self.cfg.strong_dir_min_aligned
                and amp_effective >= self.cfg.strong_dir_amp_floor):
            _lb = self.cfg.strong_dir_lookback_sec
            _ref = None
            for _t, _p in reversed(self.price_history):
                if time.time() - _t <= _lb:
                    _ref = _p
                else:
                    break
            if _ref is not None:
                _move_usd = (self.last_price - _ref)
                # FIX v14.1 B4 (anti-anticipacion): antes el override EXIGIA
                # movimiento PASADO a favor (`_moved_with`), es decir comprar
                # despues de que el precio ya subio / vender tras caer -> corr
                # (dir_apertura, pnl) = -0.11 en el log: systema compraba techos
                # y vendia suelos. Ahora STRONG_DIR se activa por conviccion del
                # blend + alineacion + amplitud, y el precio pasado solo VETA la
                # entrada si ya se movio demasiado EN CONTRA (perdio la tesis).
                _adverse = (-_move_usd if d_out.direction == Direction.LONG
                            else _move_usd)
                _moved_with = (_adverse <= self.cfg.strong_dir_max_adverse_usd)
                strong_dir_override = _moved_with

        if (combined_score >= self.cfg.min_directional_score
                and (strong_dir_override or amp_effective >= self.cfg.min_amplitude_score)
                and aligned >= self.cfg.min_aligned_signals
                and fresh_anticip_aligned >= self.cfg.min_fresh_anticip_signals
                and not contradiction and not ambiguity
                and d_out.direction != Direction.NEUTRAL
                and (strong_dir_override or expected_move >= min_profitable_move)):
            action = "LONG" if d_out.direction == Direction.LONG else "SHORT"
            reason = (f"dir={d_out.score:+.2f} amp={amp_effective:.2f} ali={aligned} "
                      f"ant={fresh_anticip_aligned} exp={expected_move:.0f}"
                      + (" STRONG_DIR" if strong_dir_override else ""))
        else:
            action = "HOLD"
            reasons = []
            # FIX v13.6: HOLD forzado si feed mudo o ambigüedad (antes solo se
            # veía como motivo, ahora también bloquea; p.ej. ant=+1.00/react=-0.75
            # con ambos fuertes -> mercado sin tesis clara, no es señal de nada).
            if feed_age_now > self.cfg.max_signal_age_sec:
                reasons.append(f"feed_mudo({feed_age_now:.1f}s)")
            if ambiguity:
                reasons.append(f"ambiguo(ant={anticip_score:+.2f}/"
                               f"react={react_score:+.2f})")
            if combined_score < self.cfg.min_directional_score:
                reasons.append(f"combo({combined_score:.2f})")
            # FIX v14.0.1: STRONG_DIR y precursor_fresco son puertas ALTERNATIVAS.
            # STRONG_DIR es la excepcion de direccion fuerte (dir>=0.70 + precio
            # moviendose a favor) que ya abrio en el log 01:04 (SHORT ali=9).
            # No debe aparecer como motivo de bloqueo cuando no aplica, ni
            # bloquear de hecho: si hay confluencia de precursores (ant>=3),
            # esa puerta basta. Antes el log mostraba ambas reglas peleando
            # (p.ej. dir=+0.73 ant=2<3 -> HOLD inexplicable).
            _strong_candidate = abs(d_out.score) >= self.cfg.strong_dir_override_score
            if (not strong_dir_override and not _strong_candidate
                    and amp_effective < self.cfg.min_amplitude_score):
                reasons.append(f"amp({amp_effective:.2f})")
            if aligned < self.cfg.min_aligned_signals:
                reasons.append(f"alineadas({aligned})")
            if (fresh_anticip_aligned < self.cfg.min_fresh_anticip_signals
                    and not strong_dir_override):
                reasons.append(f"precursor_fresco({fresh_anticip_aligned}<"
                               f"{self.cfg.min_fresh_anticip_signals})")
            if contradiction:
                reasons.append(f"contradiccion(ant={anticip_score:+.2f}/"
                               f"react={react_score:+.2f})")
            # FIX v13.6 (regla 5): el log anterior no permitía auditar POR QUÉ
            # caducaban los precursores. Ahora se registran edad del feed, nº de
            # precursores VIVOS y su edad máxima -> se distingue "no hay
            # microestructura" de "las señales light dejaron de publicarse".
            reasons.append(f"fa({feed_age_now:.1f}s)")
            # FIX v14.6: registrar CUALES son los precursores que SI cuentan
            # (antes era imposible auditar por qué el conteo saltaba 3->1).
            # FIX v14.7: mismo filtro de fuerza |normalized| >= dir_min_strength
            # que el gate real, para que la auditoria refleje el conteo exacto.
            _cntd = [n for n, s in fresh_map.items()
                     if n in ANTICIP_DIR_WEIGHTS
                     and s.confidence >= self.cfg.min_signal_confidence
                     and abs(s.normalized) >= _min_strength
                     and s.direction == d_out.direction]
            if len(_cntd) < self.cfg.min_fresh_anticip_signals:
                _missing = sorted(set(ANTICIP_DIR_WEIGHTS) - set(_cntd))
                reasons.append("ant_cnt[" + ",".join(_cntd) + "]")
                reasons.append("ant_no[" + ",".join(_missing) + "]")
            _ant_alive = [n for n in fresh_map if n in ANTICIP_DIR_WEIGHTS]
            if not _ant_alive:
                try:
                    _all_ant = [s for n, s in self.bus.get_all_latest().items()
                                if n in ANTICIP_DIR_WEIGHTS]
                except Exception:
                    _all_ant = []
                reasons.append(f"ant_vivos({len(_all_ant)})")
            if expected_move < min_profitable_move:
                reasons.append(f"exp({expected_move:.0f}<{min_profitable_move:.0f})")
            reason = " | ".join(reasons) or "sin señal"

        # FIX v13.8 CRÍTICO (audita el gate de STRONG_DIR): en el log 00:12 el
        # override pedía movimiento a favor y nunca se veía CUAL era -> imposible
        # depurar por qué LONGs con dir=+0.8 se quedaban en HOLD. Ahora el
        # motivo real siempre queda registrado:
        #   precio20s : +/-USD movido en la ventana strong_dir_lookback_sec
        #   ant/react : capas del blend (siempre, no solo en contradicción)
        #   ampE      : amplitud EFFECTIVA (post-feed-gate), no la cruda
        try:
            _lb = self.cfg.strong_dir_lookback_sec
            _ref = None
            _nowt = time.time()
            for _t, _p in reversed(self.price_history):
                if _nowt - _t <= _lb:
                    _ref = _p
                else:
                    break
            _px_move = (self.last_price - _ref) if _ref is not None else 0.0
        except Exception:
            _px_move = 0.0
        reason += (f" | ant={anticip_score:+.2f}/react={react_score:+.2f}"
                   f" | ampE={amp_effective:.2f} | px{int(self.cfg.strong_dir_lookback_sec)}s"
                   f"={_px_move:+.1f}$")

        # FIX v14.1 D5: `expected_move_pct` recibia la amplitud [0,1] (campo
        # obsoleto y dimensionalmente falso). Ahora lleva los USD reales de
        # expected_move calculados arriba; el dashboard usa la misma cifra.
        return TradeDecision(action, d_out.score, amp_effective, expected_move, reason), aligned

    async def _open(self, action, d, a, dir_signals, manual=False):
        self._check_processing_timeout()
        if self._processing:
            return

        bid, ask = self.book.get_best_bid_ask()
        if bid > 0 and ask > 0:
            spread_pct = (ask - bid) / bid * 100
            if spread_pct > 0.05:
                print(f"⚠️ Spread alto: {spread_pct:.3f}%, no abro")
                return

        # FIX v14.1 D1: segunda revision del kill-switch justo antes de enviar
        # la orden (el gate de _make_decision puede haber quedado desactualizado
        # entre decision y ejecucion; p.ej. cierre perdido en el intertanto).
        risk_ok, risk_block, order_size = self.risk.can_open_trade(
            self.rest.account_info.balance_usdt,
            self.rest.account_info.consecutive_losses)
        if not risk_ok:
            print(f"🛑 RISK_BLOCK: {risk_block} — no abro {action}")
            return

        self._processing = True
        self._processing_since = time.time()
        self.bot_state = "ENTERING"
        try:
            print(f"\n{'='*60}")
            print(f"🔵 ABRIR {action} | {order_size} BTC")
            ok = await asyncio.wait_for(
                self.rest.open_long(order_size) if action == "LONG"
                else self.rest.open_short(order_size),
                timeout=10.0)
            if not ok:
                self.bot_state = "IDLE"
                return
            avg = await asyncio.wait_for(self.rest.refresh_position(), timeout=10.0)
            entry_price = avg if avg > 0 else self.last_price
            self._entry_time = time.time()
            self._entry_price = entry_price
            self._entry_side = "Buy" if action == "LONG" else "Sell"
            self._entry_signals = dir_signals
            self._live_upnl = 0.0
            self._max_favorable_move = 0.0
            # FIX v14.2 (runner): deadline inicial = techo normal; se extiende
            # solo si la direccion sigue viva al vencerlo.
            # FIX v14.3 G1 (NameError): esta linea se escribio usando `cfg`, pero
            # `cfg = self.cfg` solo existe dentro de _trade_logic; en _open era un
            # nombre libre -> NameError en CADA apertura: el bot nunca podia abrir
            # posicion (ni automatica ni manual). El exception quedaba oculto por
            # `except asyncio.TimeoutError` y el `finally` reseteaba _processing,
            # asi que parecia "no opera" sin ningun mensaje.
            self._position_deadline = self._entry_time + self.cfg.max_trade_duration_sec
            self._extension_count = 0
            # FIX v14.5: techo del runner = entrada + hard_cap (absoluto).
            self._runner_hard_cap_ts = (self._entry_time
                                        + self.cfg.max_runner_duration_sec)
            self.rest.account_info.position_side = self._entry_side
            self.rest.account_info.entry_price = entry_price
            self.rest.account_info.position_size = order_size
            # FIX v14.1 D1: registrar apertura en RiskManager (trailing/BE/SL
            # efectivo + base para register_close).
            self.risk.register_open(self._entry_side, entry_price, order_size)
            self.bot_state = "IN_POSITION"
            # FIX v14.1.4 F3 (log trades 03:22-03:36): el CSV de aperturas se
            # usaba como registro ECONOMICO (fee_usd=-0.212592, net_pnl=-fee) y
            # al sumar filas OPEN+CLOSE el ciclo aparecía con ~1.5x su coste
            # real. Ahora la fila OPEN es solo EVENTO INFORMATIVO: fee/net a 0;
            # el coste del ciclo lo refleja exclusivamente la fila CLOSE.
            self.logger.log_trade("OPEN", action, entry_price,
                                  order_size, d, a,
                                  pnl_usd=0.0, duration_sec=0.0, reason="")
            print(f"✅ ABIERTA: {action} @ {entry_price:,.2f}")
        except asyncio.TimeoutError:
            self.bot_state = "IDLE"
        finally:
            self._processing = False

    async def _close(self, reason, d, a):
        self._check_processing_timeout()
        if self._processing:
            return
        self._processing = True
        self._processing_since = time.time()
        # FIX v14.4: marca CLOSING (antes _close no cambiaba bot_state y el
        # gate de /api/trade no podía detectar una orden de cierre en vuelo).
        self.bot_state = "CLOSING"
        try:
            print(f"\n{'='*60}")
            print(f"🔴 CERRAR: {reason}")
            ok = await asyncio.wait_for(self.rest.close_position(), timeout=10.0)
            if not ok:
                self.bot_state = "IN_POSITION"
                return
            duration = max(0.0, time.time() - (self._entry_time or time.time()))
            d_sign = 1 if self._entry_side == "Buy" else -1
            entry = self._entry_price or self.last_price
            # FIX v14.1 D1: usar el tamaño REAL de la apertura registrada en
            # RiskManager (con sizing dinamico podia diferir de base_order_size).
            size = self.risk.entry_size_btc or self.cfg.base_order_size_btc
            gross = (self.last_price - entry) * d_sign * size
            is_directional_win = gross > 0
            # FIX v14.1.4 F5 (log trades 03:22-03:36): el CSV registraba la fila
            # CLOSE con pnl BRUTO y su propio fee -> al sumar filas del ciclo el
            # coste aparecia inflado. Ahora se descuenta aqui el coste REAL del
            # ciclo (precio de ENTRADA, no last_price: el fill de cierre es a
            # mercado pero el nocional contractual es el abierto) y se pasa el
            # NETO ya descontado al logger (pnl_neto_del_csv), de modo que
            # CSV == contabilidad interna == kill-switch diario.
            notional_entry = entry * size
            cycle_fee = notional_entry * self.cfg.taker_fee_rate * 2
            net = gross - cycle_fee
            self.logger.log_trade("CLOSE", self._entry_side or "NONE", self.last_price,
                                  size, d, a, pnl_usd=net,
                                  duration_sec=duration, reason=reason,
                                  is_directional_win=is_directional_win,
                                  fee_already_deducted=True)
            # FIX v14.1.4 F4: nocional real del ciclo para record_trade_result
            # (antes leia entry_price/position_size YA reseteados).
            self.rest._last_closed_notional = notional_entry
            self.rest.record_trade_result(net, is_directional_win)
            # FIX v14.1 D1: alimentar el kill-switch diario con el resultado neto.
            self.risk.register_close(net)
            self.rest.account_info.position_side = "None"
            self.rest.account_info.position_size = 0.0
            self._entry_time = self._entry_price = self._entry_side = self._entry_signals = None
            self._position_deadline = None   # FIX v14.2 (runner)
            self._extension_count = 0        # FIX v14.2 (runner)
            self._runner_hard_cap_ts = None  # FIX v14.5
            self._last_close_time = time.time()
            self._live_upnl = 0.0
            self._max_favorable_move = 0.0
            self.bot_state = "COOLDOWN"
            print(f"✅ CERRADA | Neto: {net:+.4f}")
        except asyncio.TimeoutError:
            # FIX v14.4: si el cierre timeout, NO dejar colgado "CLOSING" (el
            # gate de /api/trade esperaría 3s en vano); restaurar estado real.
            self.bot_state = "IN_POSITION" if self._entry_time else "IDLE"
        finally:
            self._processing = False
    async def _trade_logic(self, decision, dir_signals, d_score, a_score):
        cfg = self.cfg
        if time.time() - self._last_reconcile > cfg.reconcile_interval_sec:
            self._reconcile()
            self._last_reconcile = time.time()

        pos = self.rest.account_info.position_side

        if bot_state.mode == "MANUAL":
            # FIX v14.4 (mando de golpe): en MANUAL el botón LONG/SHORT/CERRAR se
            # ejecuta YA: sin cooldown, sin gate de feed y SIN exigir que la
            # decisión automática acompañe. Única protección: kill-switch de
            # riesgo (se revalida dentro de _open) y spread extremo (_open).
            wanted = bot_state.position
            if wanted in ("LONG", "SHORT") and pos == "None" and self._entry_time is None:
                await self._open(wanted, d_score, a_score, dir_signals, manual=True)
            elif wanted == "CLOSE" and pos != "None":
                await self._close("MANUAL", d_score, a_score)
                bot_state.position = "None"
            return

        if pos != "None" and self._entry_time is not None:
            duration = time.time() - self._entry_time
            # FIX v13.5 CRÍTICO (PnL fantasma del log 23:42): si el feed está
            # mudo, last_price es viejo y move/decisiones son LOTERÍA sobre
            # datos zombie. Con >60s sin señales NUEVAS se bloquean TODAS las
            # decisiones de cierre excepto TIMEOUT (que usa reloj real) y el
            # reconciliation periódico; así no se cierra contra un precio que
            # no existe y no se abren entradas nuevas (gate abajo).
            feed_dead = self.bus.feed_age() > 60.0

            sign = 1 if self._entry_side == "Buy" else -1
            move = (self.last_price - (self._entry_price or self.last_price)) * sign

            # ---- FIX v13.3 CRÍTICO: stop dinámico por deterioro de tesis ----
            # Antes, con la dirección en contra pero el PnL por debajo del SL duro
            # (-$120) y por encima del fee-break-even, el bot podía AGUANTAR la
            # posición hasta 900s o incluso 1 hora si el timeout no disparaba
            # (p. ej. feed congelado). Ahora: si >=N precursores frescos apuntan
            # al lado contrario y llevamos >=reversal_grace_sec dentro, se corta
            # SIN esperar al stop fijo ni a que el PnL cubra comisiones.
            # FIX v13.5 (feed mudo): si hace >max_signal_age_sec*2 que NO llega
            # ninguna señal nueva, el score está CONGELADO (datos zombie) y
            # cerrar "por reversión" es una lotería sobre microestructura vieja
            # -> solo se permite el corte si el feed está vivo. El TIMEOUT/SL
            # duro siguen activos igualmente.
            if (duration >= cfg.reversal_grace_sec and decision.action != "HOLD"
                    and not feed_dead
                    and self.bus.feed_age() <= cfg.max_signal_age_sec * 2):
                opposing = Direction.LONG if self._entry_side == "Sell" else Direction.SHORT
                dec_dir = Direction.LONG if decision.action == "LONG" else Direction.SHORT
                # FIX v13.6 CRÍTICO (churn del log 23:50-04:10): los 5 trades
                # cerrados fueron TODOS REVERSAL_ANT con PnL de lotería (+0.01,
                # -0.35, +0.59, +0.07, +0.21 USD sobre ~$5.5 netos perdidos en
                # fees). El corte se disparaba con 3 señales opuestas FRESCAS...
                # pero solo exigía 1 precursor fresco ALINEADO para abrir; es
                # decir, bastaba que el blend oscilara +-0.1 en un tick para
                # dar la vuelta a la posición. Ahora el cierre por anticipación
                # exige MÁS convicción que la apertura: precursores frescos
                # opuestos >= min_opposing_signals_to_flip Y al menos
                # min_fresh_anticip_signals de ellos son ANTICIPATIVOS puros
                # (no reactivas tardías), Y el score direccional opuesto supera
                # el umbral fuerte (d_score en contra >= reversal_min_dir).
                dmeta = {}  # FIX v13.6: TradeDecision no lleva metadata; la
                # ambigüedad ya se evaluó en _make_decision y via `decision.reason`
                # (marca "ambiguo(...)"). Aquí se recalcula barato desde d_out.
                if dec_dir == opposing and "ambiguo(" not in decision.reason:
                    # FIX v13.5: contar SOLO precursores frescos (edad real), no
                    # cualquier señal viva con TTL largo (oi_delta vive 180s y
                    # falseaba el conteo -> cierres REVERSAL_ANT prematuros como
                    # el trade de 31s @ +0.014 USD que regaló la tendencia).
                    fresh_map = self.bus.get_fresh(max(cfg.max_signal_age_sec, cfg.fresh_gate_window_sec))  # FIX v14.6
                    opp_fresh = sum(
                        1 for name, s in fresh_map.items()
                        if s.direction == opposing
                        and s.confidence >= cfg.min_signal_confidence
                        and abs(s.normalized) >= 0.25)
                    # FIX v13.6: al menos N de las señales opuestas frescas
                    # deben ser ANTICIPATIVAS puras (bloque líder), no solo
                    # reactivas tardías que giran DESPUÉS del movimiento.
                    opp_ant_fresh = sum(
                        1 for name, s in fresh_map.items()
                        if name in ANTICIP_DIR_WEIGHTS
                        and s.direction == opposing
                        and s.confidence >= cfg.min_signal_confidence
                        and abs(s.normalized) >= 0.25)
                    dir_against = abs(d_score) if (
                        (d_score > 0) == (opposing == Direction.LONG)) else 0.0
                    # reversal confirmado por anticipación -> cierre inmediato
                    if (opp_fresh >= cfg.min_opposing_signals_to_flip
                            and opp_ant_fresh >= cfg.min_reversal_anticip_fresh
                            and dir_against >= cfg.reversal_min_dir_score):
                        # FIX v14.1.4 E2 (log trades 03:22-03:36): REVERSAL_ANT
                        # era la UNICA salida activa y cortaba en plena pérdida
                        # (-$35, +$40 contra SHORT, -$64 HARD_STOP). Exigencia
                        # nueva: solo se corta por reversión si ADEMAS de la
                        # confluencia opuesta, la tesis original ya NO se
                        # sostiene — i.e. el movimiento favorable maximo jamas
                        # alcanzo un margen decente Y el move actual es negativo
                        # o el dir_score propio sigue fuerte en contra. Evita
                        # cerrar lucros que aun maduran hacia TARGET/GIVEBACK.
                        thesis_alive = (move > 0
                                        and self._max_favorable_move >= cfg.giveback_min_peak_usd * 0.7)
                        if not thesis_alive:
                            await self._close(f"REVERSAL_ANT_{move:.0f}USD", d_score, a_score)
                        return

            # FIX v14.1.4 E3 (log trade 03:27:41->03:31:21): LONG aguantado
            # hasta HARD_STOP a -$64 porque stop_loss_usd=120 es inalcanzable
            # (rango sesion +-40$) y REVERSAL_ANT dependia de la loteria de
            # precursores. SL temprano: si la perdida supera soft_stop_loss_usd
            # se corta SIN esperar gracia/duracion (solo feed vivo).
            if (move <= -cfg.soft_stop_loss_usd and duration >= 5.0
                    and not feed_dead):
                await self._close(f"SOFT_SL_{move:.0f}USD", d_score, a_score)
                return

            if move <= -cfg.stop_loss_usd and duration >= 5.0 and not feed_dead:
                # FIX v13.5 (PnL fantasma): el SL usa last_price; con feed muerto
                # el precio puede ser de hace horas -> el "move" registrado es
                # mentira (log 23:42: OPEN @77243 y luego tick a 77999 = +$3.8k
                # inexistentes). Exigimos feed vivo o frescura REST reciente.
                await self._close(f"STOP_{move:.0f}USD", d_score, a_score)
                return
            # FIX v13.7 CRÍTICO (log trades 00:12): la posición Sell se abrió a
            # las 00:12 y aguantó ~60 USD de pérdida porque REVERSAL_ANT exige
            # duracion>=gracia(30s)+decision+precursores y GIVEBACK/TARGET/
            # REVERSAL exigen min_trade_duration (180s): en los primeros 3 min
            # NO EXISTÍA NINGÚN corte por pérdida <-$stop_loss. HARD_STOP ya:
            # si move <= -max_hold_loss_usd se corta pase lo que pase con los
            # gates (feed vivo; con feed muerto manda STOP_ normal igual).
            if (move <= -cfg.max_hold_loss_usd and duration >= 5.0
                    and not feed_dead):
                await self._close(f"HARD_STOP_{move:.0f}USD", d_score, a_score)
                return
            if (move >= cfg.target_move_usd and duration >= cfg.min_trade_duration_sec
                    and not feed_dead):
                # FIX v14.1.4 E1: salida por BENEFICIO legitima (cubre fees x3
                # con TP=12$ y fee ciclo ~0.85$). Ya no es inalcanzable: el TP
                # viejo de 250$ garantia que NINGUNA salida lucrativa existiera.
                await self._close(f"TARGET_{move:.0f}USD", d_score, a_score)
                return
            # trailing/breakeven ligero: si fue +giveback_min_peak_usd y devolvió
            # >ratio del máximo, cerrar con lo ganado (evita regalar una vela
            # entera). FIX v14.1.4 E1: antes exigía peak > target*0.6 (=150$ con
            # TP viejo) -> jamas disparaba; ahora umbral explicito realista.
            if (not feed_dead
                    and self._max_favorable_move >= cfg.giveback_min_peak_usd
                    and move < self._max_favorable_move * (1.0 - cfg.giveback_give_ratio)
                    and duration >= cfg.reversal_grace_sec):
                # solo corta si AUN queda beneficio neto tras fees (movimiento
                # en contra NO se corta aqui: para eso estan REVERSAL_ANT/SL)
                est_cycle_fee = self.last_price * cfg.base_order_size_btc * cfg.taker_fee_rate * 2
                if move >= est_cycle_fee * cfg.reversal_min_profit_mult:
                    await self._close(f"GIVEBACK_{move:.0f}USD", d_score, a_score)
                    return
            # ---- FIX v14.2 (runner): TIMEOUT extensible mientras la direccion
            # siga viva. Antes max_trade_duration_sec (900s) cortaba tendencias
            # sanas; ahora, al vencer el deadline, si el dir_score sigue a favor
            # >= runner_min_dir_score, hay >= runner_min_anticip_fresh
            # precursores ANTICIPATIVOS frescos alineados, no es ambiguo y el
            # movimiento actual conserva una fraccion del pico favorable, se
            # EXTIENDE un bloque mas (extension_block_sec) hasta el techo
            # absoluto max_runner_duration_sec (1h). En cualquier otro caso
            # (direccion en contra, ambigua o muerta; rally degradado; feed
            # muerto; techo alcanzado) cierra con TIMEOUT como antes.
            if duration >= cfg.max_trade_duration_sec:
                # FIX v14.5: el techo es un timestamp absoluto (valido tanto
                # para aperturas propias como para huerfanas restauradas, cuyo
                # _entry_time puede ser mentira). Con hard_cap=7200s el runner
                # puede sostener la tesis VIVAMAS de 1 hora.
                now_ext = time.time()
                hard_cap_left = ((self._runner_hard_cap_ts - now_ext)
                                 if self._runner_hard_cap_ts is not None
                                 else (cfg.max_runner_duration_sec - duration))
                can_extend = (cfg.use_direction_runner
                              and not feed_dead
                              and hard_cap_left > 1.0
                              and self._position_deadline is not None
                              and now_ext >= self._position_deadline)
                extended = False
                if can_extend:
                    same_dir = Direction.LONG if self._entry_side == "Buy" else Direction.SHORT
                    dec_dir_r = Direction.LONG if decision.action == "LONG" \
                        else (Direction.SHORT if decision.action == "SHORT" else Direction.NEUTRAL)
                    dir_with = abs(d_score) if ((d_score > 0) == (same_dir == Direction.LONG)) else 0.0
                    fresh_map_r = self.bus.get_fresh(max(cfg.max_signal_age_sec, cfg.fresh_gate_window_sec))  # FIX v14.6
                    ant_with = sum(1 for n, s in fresh_map_r.items()
                                   if n in ANTICIP_DIR_WEIGHTS
                                   and s.direction == same_dir
                                   and s.confidence >= cfg.min_signal_confidence
                                   and abs(s.normalized) >= 0.25)
                    if same_dir == Direction.LONG:
                        dec_ok = dec_dir_r != Direction.SHORT   # LONG o HOLD vale
                    else:
                        dec_ok = dec_dir_r != Direction.LONG    # SHORT o HOLD vale
                    thesis_ok = (dir_with >= cfg.runner_min_dir_score
                                 and ant_with >= cfg.runner_min_anticip_fresh
                                 and "ambiguo(" not in decision.reason
                                 and dec_ok)
                    degrade_ok = (move >= 0
                                  or move >= self._max_favorable_move * cfg.runner_price_degrade_frac)
                    if thesis_ok and degrade_ok:
                        self._position_deadline = min(
                            self._position_deadline + cfg.extension_block_sec,
                            (self._runner_hard_cap_ts
                             if self._runner_hard_cap_ts is not None
                             else self._entry_time + cfg.max_runner_duration_sec))
                        self._extension_count += 1
                        extended = True
                        print(f"⏳ EXTENSION #{self._extension_count}: tesis viva "
                              f"(dir={dir_with:.2f} ant={ant_with}) -> nueva meta "
                              f"{self._position_deadline - time.time():.0f}s")
                if not extended:
                    await self._close(f"TIMEOUT_{move:.0f}USD", d_score, a_score)
                    return

            # Reversal clásico (con exigencia de cubrir fees) -> se mantiene para
            # salidas LUCRATIVAS; el corte por anticipación de arriba ya cubre el
            # caso perdedor.
            if duration >= cfg.min_trade_duration_sec and decision.action != "HOLD":
                opposing = Direction.LONG if self._entry_side == "Sell" else Direction.SHORT
                dec_dir = Direction.LONG if decision.action == "LONG" else Direction.SHORT
                # FIX v13.6: el reversal clásico también queda bloqueado en
                # zona ambigua (churn) y exige dirección opuesta clara.
                if (dec_dir == opposing and "ambiguo(" not in decision.reason
                        and abs(d_score) >= cfg.min_directional_score):
                    # FIX v14.1.4 F2: fee con la UNICA fuente (cfg.taker_fee_rate);
                    # antes hardcodeado 0.0011 -> umbral de lucro x2 ficticio.
                    est_cycle_fee = (self.last_price * cfg.base_order_size_btc) \
                        * cfg.taker_fee_rate * 2.0
                    if move >= est_cycle_fee * cfg.reversal_min_profit_mult:
                        await self._close(f"REVERSAL_{move:.0f}USD", d_score, a_score)
            return

        if time.time() - self._last_close_time < cfg.cooldown_after_close_sec:
            self.bot_state = "COOLDOWN"
            return

        # FIX v13.5 CRÍTICO (no entra bien): prohibido ABRIR con feed muerto o
        # congelado. En el log 23:42 el bot abrió SHORT con dir=-0.23 y amp=0.85
        # INMEDIATAMENTE tras 12 ticks de scores idénticos (datos zombie de las
        # 04:10, ~20h antes). Con bus.feed_age()>4s no hay microestructura real.
        if self.bus.feed_age() > cfg.max_signal_age_sec:
            self.bot_state = "FEED_MUTE"
            return

        if decision.action in ("LONG", "SHORT") and pos == "None" and self._entry_time is None:
            await self._open(decision.action, d_score, a_score, dir_signals)
        else:
            self.bot_state = "WAITING_SIGNAL"

    async def _calc_heavy_signals(self):
        now = time.time()
        if now - self._last_heavy_calc < self._heavy_calc_interval:
            return
        self._last_heavy_calc = now
        # FIX: sync, sin lock ni wait_for que se saltaban señales
        try:
            self._publish_sync(self.cvd.calculate())
            self._publish_sync(self.vwap.calculate(self.last_price))
            self._publish_sync(self.ofi.calculate())
        except Exception:
            pass

    def _update_state_snapshot(self, d_out, a_out, decision, aligned, dir_s, amp_s):
        """Actualiza bot_state en memoria (O(1), sin I/O)."""
        price_change = self.last_price - self.prev_price if self.prev_price > 0 else 0.0
        # FIX v13.3: edad del feed -> el dashboard puede distinguir "bot vivo con
        # feed mudo" de "valores anclados". También alimenta signal_age para que
        # las barras se congelen VISUALMENTE solo si lo están en realidad.
        now_ts = time.time()
        signal_age = max(0.0, now_ts - self._last_ws_message)
        bot_state.signal_age_sec = signal_age
        bot_state.loop_count = self._loop_count
        bot_state.ws_connected = bool(getattr(self.ws, "is_connected", False))
        bot_state.last_price = self.last_price
        bot_state.price_change = price_change
        bot_state.price_update_time = self._last_price_update
        bot_state.directional_score = d_out.score
        bot_state.amplitude_score = a_out.score
        bot_state.directional_signals = dir_s
        bot_state.amplitude_signals = amp_s
        bot_state.decision = decision.action
        # FIX v14.1 D5: el dashboard mostraba `a_out.score` crudo (amplitud
        # [0,1]) en el campo "expected_move", mientras el log guardaba
        # amp_effective y la decision usaba USD reales -> tres cifras
        # distintas para lo mismo. Ahora es decision.expected_move_pct, que
        # desde esta version lleva los USD reales del gate C1.
        bot_state.expected_move = decision.expected_move_pct
        # FIX v14.5: NO pisar el mando manual. En MANUAL, si el operador pulso
        # LONG hace 200ms y la snapshot del loop corre antes de que _open
        # termine, esto restauraba position="None" y el branch MANUAL de
        # _trade_logic nunca veia el mando -> boton "ignorado". El mando vive
        # hasta que el propio motor lo consume (_trade_logic / _close).
        if bot_state.mode != "MANUAL":
            bot_state.position = self.rest.account_info.position_side
        bot_state.bot_state = self.bot_state
        bot_state.last_signal = self.last_signal_summary
        acc = self.rest.account_info
        bot_state.account.balance_usdt = acc.balance_usdt
        bot_state.account.position_side = acc.position_side
        bot_state.account.position_size = acc.position_size
        bot_state.account.entry_price = acc.entry_price
        bot_state.account.unrealized_pnl = acc.unrealized_pnl
        bot_state.account.unrealized_pnl_pct = acc.unrealized_pnl_pct
        bot_state.account.session_wr = acc.session_wr
        bot_state.account.total_fees = acc.total_fees
        bot_state.account.session_trades = acc.session_trades
        bot_state.account.session_wins = acc.session_wins
        bot_state.account.session_losses = acc.session_losses
        bot_state.account.consecutive_losses = acc.consecutive_losses
        bot_state.account.session_pnl = acc.session_pnl
        bot_state.recent_decisions = list(self.logger.recent_decisions)
        new_trades = list(self.logger.recent_trades)
        if new_trades != bot_state.recent_trades:
            bot_state._trades_version += 1
        bot_state.recent_trades = new_trades
        bot_state.position_opened_ts = self._entry_time
        bot_state._state_dirty = True

    def _maybe_persist_snapshot(self):
        """FIX v13.3: persiste el último estado bueno a state/dashboard_snapshot.json
        (en thread pool, cada 15s). web_server lo lee como fallback si el feed muere;
        antes nadie lo escribía y el fallback estaba muerto."""
        now = time.time()
        if now - self._last_snapshot_write < self.cfg.state_save_interval_sec:
            return
        if self.last_price <= 0:
            return
        self._last_snapshot_write = now
        snap = {k: v for k, v in get_status().items() if k != "type"}
        try:
            asyncio.get_running_loop().run_in_executor(
                self._thread_pool, self._write_snapshot_file, snap)
        except RuntimeError:
            pass

    @staticmethod
    def _write_snapshot_file(snap: dict):
        try:
            from pathlib import Path
            p = Path(__file__).parent / "state" / "dashboard_snapshot.json"
            p.parent.mkdir(exist_ok=True)
            tmp = p.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(snap, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, p)
        except Exception:
            pass

    async def _loop(self, stop_event: asyncio.Event):
        tick = 0
        while not stop_event.is_set():
            try:
                loop_start = time.time()
                now = loop_start
                self._loop_count += 1

                loop_gap = loop_start - self._last_loop_time
                if loop_gap > 3.0 and tick > 5:
                    self._slow_loop_count += 1
                    if self._slow_loop_count >= 5:
                        self._slow_loop_count = 0
                        gc.collect()
                self._last_loop_time = loop_start

                ws_gap = time.time() - self._last_ws_message
                if ws_gap > 20.0 and self.last_price > 0:
                    print(f"⚠️ Sin WS por {ws_gap:.0f}s")

                if self.last_price > 0:
                    change = 0.0
                    if self.price_5min_ago > 0:
                        change = (self.last_price - self.price_5min_ago) / self.price_5min_ago * 100
                    # FIX: señales ligeras en línea, sin lock/timeout (antes se
                    # perdían todas si el bus estaba ocupado y el bot "no operaba")
                    try:
                        self._publish_sync(self.large.calculate(self.last_price))
                        self._publish_sync(self.absorption.calculate())
                        self._publish_sync(self.taker.calculate())
                        self._publish_sync(self.candle_delta.calculate())
                        self._publish_sync(self.realized_vol.calculate())
                        self._publish_sync(self.volume_spike.calculate())
                        self._publish_sync(self.volatility_ratio.calculate())
                        # FIX v13.4 CRÍTICO: atr_expansion se CALCULABA pero nunca
                        # se PUBLICABA en el bus -> el bloque reactivo de amplitud
                        # perdía 1 de sus 6 señales (y la que mejor mide expansión
                        # de rango vela a vela). El score de amplitud salía corto.
                        self._publish_sync(self.atr.calculate())
                        # FIX v13.4: micro_price_dev es la señal anticipativa más
                        # rápida (el micro precede al siguiente tick) pero estaba
                        # throttleada a 1s y caducaba por TTL -> ahora se recalcula
                        # en cada batch de trades. O(1), sin I/O.
                        if self.book.bids and self.book.asks:
                            # FIX v13.7: solo recalcular si HUBO TRADES nuevos desde
                            # el anterior calculo. Con libro/trades quietos la EMA
                            # "envejecia" y seguia puntuando un sesgo fantasma
                            # (log 00:12: dev +0.95 con precio congelado).
                            if self._micro_trades_seen != self._trades_batch_count:
                                self._micro_trades_seen = self._trades_batch_count
                                self._publish_sync(self.micro_dev.calculate(
                                    self.book, self.last_price))
                    except Exception:
                        pass

                    await self._calc_heavy_signals()

                    try:
                        self._publish_sync(self.oi.calculate(self.last_price, change))
                        self._publish_sync(self.ls_ratio.calculate())
                        self._publish_sync(self.funding.calculate())
                    except Exception:
                        pass

                    # v12.8: señales anticipativas basadas en LIBRO (throttle 1s;
                    # se recalculan sobre snapshots de book, no por cada delta)
                    if now - self._last_book_signals >= 1.0:
                        self._last_book_signals = now
                        try:
                            # FIX v13.8 F1: micro_dev NO se recalcula aqui -> se
                            # hizo DOS veces por tick (batch + libro), contaminando
                            # la EMA y puntuando doble. Su fuente unica es el
                            # batch de trades (arriba, con gate _trades_batch_count).
                            # FIX v13.8: UN solo get_clean_book compartido via
                            # cache last_clean (antes 6 re-ordenaciones del libro).
                            self.book.get_clean_book(30)   # rellena cache
                            self._publish_sync(self.book_slope.calculate(self.book))
                            self._publish_sync(self.wall_momentum.calculate(self.book))
                            self._publish_sync(self.wofi.calculate(self.book))
                            self._publish_sync(self.liq_thinning.calculate(self.book))
                            self._publish_sync(self.spread_comp.calculate(self.book, self.last_price))
                        except Exception:
                            pass
                    # v12.8: anticipativas basadas en TRADES
                    try:
                        self._publish_sync(self.cvd_div.calculate())
                        self._publish_sync(self.arrival_run.calculate())
                        self._publish_sync(self.vol_accel.calculate())
                    except Exception:
                        pass

                d_out = calculate_directional(self.bus, self.cfg.min_signal_confidence)
                a_out = calculate_amplitude(self.bus, self.cfg.min_signal_confidence)
                decision, aligned = self._make_decision(d_out, a_out)

                dir_s = {s.name: {"normalized": s.normalized, "confidence": s.confidence,
                                  "direction": s.direction.name} for s in d_out.signals}
                amp_s = {s.name: {"normalized": s.normalized, "confidence": s.confidence}
                         for s in a_out.signals}

                if d_out.direction != Direction.NEUTRAL:
                    self.last_signal_summary = (
                        f"{d_out.direction.name} "
                        f"(dir={d_out.score:+.2f}, amp={a_out.score:.2f}, ali={aligned})")
                else:
                    self.last_signal_summary = "NEUTRAL"

                if tick % 5 == 0:
                    # FIX v13.5: feed_age en el log -> se puede AUDITAR si un
                    # HOLD/SHORT provenía de datos frescos o congelados (los
                    # logs viejos no lo permitían; los scores idénticos durante
                    # 20h pasaron inadvertidos).
                    self.logger.log_decision(
                        self.last_price, d_out.score, a_out.score,
                        decision.action, decision.reason, aligned,
                        feed_age_sec=self.bus.feed_age(),
                        signal_age_sec=time.time() - self._last_ws_message)
                    self._print_status(decision, d_out, a_out, aligned)

                await self._trade_logic(decision, dir_s, d_out.score, a_out.score)
                self.last_decision = decision.action

                # FIX: snapshot en memoria + broadcast fire-and-forget con throttle.
                # Antes: `await wait_for(update_bot_state(...), 0.5)` bloqueaba el
                # loop del bot hasta que TODOS los clientes web respondieran.
                self._update_state_snapshot(d_out, a_out, decision, aligned, dir_s, amp_s)
                self._maybe_persist_snapshot()
                state_task = getattr(bot_state, "_state_task", None)
                if state_task is None or state_task.done():
                    try:
                        bot_state._state_task = asyncio.get_running_loop().create_task(
                            broadcast_state())
                    except RuntimeError:
                        pass

                tick += 1
                loop_time = time.time() - loop_start
                sleep_time = max(0.1, self.cfg.loop_interval_sec - loop_time)
                await asyncio.sleep(sleep_time)

            except Exception as e:
                print(f"❌ Loop: {e}")
                traceback.print_exc()
                await asyncio.sleep(1)

    async def _feed_watchdog(self, stop_event: asyncio.Event):
        """FIX v13.5: vigila el FEED de datos independientemente del estado del socket.

        Los logs mostraban ws_age creciendo indefinido con price=0.0 y sin ningún
        aviso: si la conexión WS FALLA (en vez de quedarse medio-abierta), nadie
        recuperaba el precio ni lo reportaba al dashboard. Ahora:
          - >10s sin feed: marca 'feed_mudo' en bot_state (visible en dashboard)
          - cada 30s: intenta REST ticker para des-congelar el precio
          - log explícito del fallo (antes era silencioso)
        """
        last_rest_try = 0.0
        warned = False
        while not stop_event.is_set():
            await asyncio.sleep(2)
            ws_age = time.time() - self._last_ws_message
            muted = ws_age > 10.0
            if muted != getattr(bot_state, "feed_muted", False):
                bot_state.feed_muted = muted
            if not muted:
                warned = False
                continue
            if not warned:
                print(f"📡 Feed mudo desde {ws_age:.0f}s "
                      f"(conectado={self.ws.is_connected}); intentando REST...")
                warned = True
            now = time.time()
            if now - last_rest_try >= 30.0:
                last_rest_try = now
                try:
                    r = await asyncio.wait_for(
                        self.rest._call("get_tickers", category="linear",
                                        symbol=self.cfg.symbol), timeout=8.0)
                    lst = ((r or {}).get("result") or {}).get("list") or []
                    if lst and lst[0].get("lastPrice"):
                        px = float(lst[0]["lastPrice"])
                        if px > 0:
                            self.prev_price = self.last_price
                            self.last_price = px
                            self._last_price_update = time.time()
                            self._update_live_upnl()
                            self._broadcast_price_soon(px, px - self.prev_price,
                                                       self._live_upnl)
                            print(f"🔄 Precio recuperado vía REST: {px:,.2f}")
                except Exception as e:
                    print(f"⚠️ REST fallback falló: {e}")

    async def _health_monitor(self, stop_event: asyncio.Event):
        while not stop_event.is_set():
            await asyncio.sleep(10)

            loop_age = time.time() - self._last_loop_time
            if loop_age > 5:
                print(f"🚨 Loop parado por {loop_age:.0f}s")

            ws_age = time.time() - self._last_ws_message
            if ws_age > 30:
                print(f"🚨 WebSocket muerto por {ws_age:.0f}s")
                # FIX: si el WS está "conectado" pero mudo (half-open TCP), el bot
                # se congelaba sin datos. Forzamos reconexión.
                try:
                    await self.ws.force_reconnect()
                except Exception as e:
                    print(f"⚠️ force_reconnect: {e}")

            # FIX v13.5: si el socket aparece "conectado" pero no llega NADA en
            # 90s (medio-abierto persistente que el watchdog interno no cerró),
            # lo tiramos desde fuera para forzar handshake nuevo y resubscribir.
            if ws_age > 90 and self.ws.is_connected:
                print(f"🔁 Feed mudo {ws_age:.0f}s con socket 'vivo' -> reconexión forzada externa")
                try:
                    await self.ws.force_reconnect()
                except Exception as e:
                    print(f"⚠️ force_reconnect externa: {e}")

            if loop_age > 60 and self._entry_time is not None:
                print(f"🚨 EMERGENCIA: Cerrando posición tras {loop_age:.0f}s")
                try:
                    await asyncio.wait_for(self.rest.close_position(), timeout=10.0)
                    self._entry_time = None
                    self._entry_price = None
                    self._entry_side = None
                    self._position_deadline = None   # FIX v14.2 (runner)
                    self._extension_count = 0        # FIX v14.2 (runner)
                    self._runner_hard_cap_ts = None  # FIX v14.5
                    self.bot_state = "COOLDOWN"
                    self._last_close_time = time.time()
                except Exception as e:
                    print(f"❌ Error emergencia: {e}")

            if self._loop_count > 0 and self._loop_count % 100 == 0:
                print(f"📊 {self._loop_count} loops")

    async def _heartbeat_writer(self, stop_event: asyncio.Event):
        """FIX: buffer en memoria + escritura en thread pool -> nunca bloquea el event loop."""
        last_flush = 0.0
        while not stop_event.is_set():
            self._heartbeat_buffer.append(
                f"{time.time():.2f} | loop={self._loop_count} | "
                f"price={self.last_price:.1f} | "
                f"ws_age={time.time()-self._last_ws_message:.1f}s | "
                f"state={self.bot_state} | "
                f"entry={'Y' if self._entry_time else 'N'}\n")
            now = time.time()
            if now - last_flush >= 10.0 and self._heartbeat_buffer:
                lines = "".join(self._heartbeat_buffer)
                self._heartbeat_buffer.clear()
                last_flush = now
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        self._thread_pool, self._flush_heartbeat, lines)
                except Exception:
                    pass
            await asyncio.sleep(1)

    @staticmethod
    def _flush_heartbeat(lines: str):
        try:
            with open("heartbeat.log", "a") as f:
                f.write(lines)
            # FIX v13.5: rotación simple (los logs analizados tenían 24KB de HOLDs
            # inútiles y el fichero nunca se limpiaba -> difícil detectar fallos).
            import os as _os
            if _os.path.getsize("heartbeat.log") > 2_000_000:
                _os.replace("heartbeat.log", "heartbeat.log.old")
        except Exception:
            pass

    def _print_status(self, decision, d_out, a_out, aligned):
        G, R, Y, X = "\033[92m", "\033[91m", "\033[93m", "\033[0m"
        acc = self.rest.account_info
        icon = "🟢" if decision.action == "LONG" else ("🔴" if decision.action == "SHORT" else "⚪")
        color = G if decision.action == "LONG" else (R if decision.action == "SHORT" else Y)
        states = {"IDLE": "⚪", "WAITING_SIGNAL": "🔍", "ENTERING": "🔄",
                  "IN_POSITION": "📈", "COOLDOWN": "⏳"}
        state_icon = states.get(self.bot_state, "?")
        proc = "🔒" if self._processing else " "

        print(f"\n{'='*60}")
        print(f"{proc}{state_icon} {self.bot_state:15s} | "
              f"{icon}{color}{decision.action:5s}{X} | {self.last_price:,.2f}")
        print(f"   📡 {self.last_signal_summary}")
        print(f"   💰 ${acc.balance_usdt:,.2f} | "
              f"WR: {acc.session_wins}/{acc.session_trades} ({acc.session_wr:.1f}%) | "
              f"PnL: {acc.session_pnl:+.4f}")
        print(f"   📊 {len(d_out.signals)}/10 dir + {len(a_out.signals)}/6 amp | Ali {aligned}")
        if self._entry_time:
            age = time.time() - self._entry_time
            print(f"   📌 {self._entry_side} {self.cfg.base_order_size_btc} BTC @ "
                  f"{self._entry_price:,.2f} | uPnL: {self._live_upnl:+.4f} | {age:.0f}s")