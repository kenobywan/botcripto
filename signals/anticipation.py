"""
Anticipation Signals v13.8 - Señales ANTICIPATIVAS de dirección y amplitud

REESCRITURA COMPLETA tras auditoría de calidad (v13.7 -> v13.8).

Fallos de la versión anterior que se corrigen aquí:

  FLUJOS DE DATOS
  F1. micro_price_dev se recalculaba DOS veces por tick (batch de trades +
      bloque de libro) -> EMA contaminada dos veces por segundo, doble conteo.
  F2. wall_momentum guardaba `_prev = {**wb, **wa}` SIN separar lados: un precio
      igual en bid y ask (posible cerca del touch) aplastaba entradas; y al
      mutar el mapa se comparaban muros contra sí mismos entre snapshots.
  F3. liquidity_thinning / spread_compression usaban half-lives largos (20s/
      120s) y medían ratio CONTRA SU PROPIA MEDIA MÓVIL: cuando el libro lleva
      minutos fino, "fino" deja de ser señal (auto-adaptación errónea: la base
      perseguía el estado actual y norm colapsaba a ~0 justo cuando más importa).
  F4. volume_accel comparaba burst(5s) vs base(60s) pero la base incluye el
      propio burst -> dilución mecánica: techo realista accel≈1.15-1.3 para
      bursts normales; umbral 1.2 dejaba la señal casi siempre en 0.

  CÁLCULO
  C1. micro_price_dev normalizaba a ±0.02% FIJO: en BTC ese desvío es enorme
      (ruido típico 0.001-0.005%). Ahora escala adaptativa por desviación media.
  C2. ofi_weighted: las cancels de niveles que DESAPARECEN del snapshot no se
      contaban (solo deltas de precios aún presentes); el near-OFI se quedaba
      ciego ante flush de liquidez. Además restaba adds de asks como si fueran
      cancels (signo OK) pero sin distinguir add/cancel por lado.
  C3. cvd_divergence: normalizador fijo dc/40 BTC — irrelevante en régimen de
      bajo volumen y saturado en régimen alto. Ahora z-score sobre histórico
      de deltas de ventana.
  C4. trade_arrival: `usd += pr*q` acumulado pero NUNCA usado (código muerto);
      min_run=8 exigía 8 trades seguidos del mismo lado en 6s (casi imposible
      en BTC spot-perp líquido: racha típica 3-5). Ahora run-weighted por USD
      agresor y min_run=4.
  C5. book_slope: `norm = v*2.5` con v=br-ar donde br,ar∈[0,1]: el rango útil
      real es ±0.2; amplificar x2.5 convierte ruido en señales plenas. Ahora
      calibrado por percentil histórico.
  C6. wall_momentum devolvía conf=0.15 cuando tot<=0 -> con min_conf=0.40 esas
      publicaciones se FILTRABAN y la señal desaparecía del bus (TTL 6s), en vez
      de puntuar neutro. Ahora emite NEUTRAL conf=0.5 explícito.
  C7. LiquidityThinning hint LONG si rb<ra: el lado DELGADO cede -> si bids son
      más finos el precio cae -> debe ser SHORT (estaba INVERTIDO).
  C8. SpreadCompression emitía NEUTRAL con conf 0.7 SIEMPRE, incluso con ratio
      >1 (spread ANCHO): puntaba como amplitud alta en mercados expandidos.
      Ahora norm solo sube con compresión real y añade confirmación de
      apilado cerca del touch (coiling = spread fino + tamaño creciente).

Convención: normalized en [-1, 1] para direccionales; [0, 1] para amplitud.
Las amplitúdicas emiten direction_hint en metadata cuando aplica.
"""
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

from models import Direction, Signal
from orderbook import SimpleOBM
from signals.utils import dir_from


# ---------------------------------------------------------------- helpers
def _sides(book: SimpleOBM, levels: int) -> Tuple[List, List]:
    """Snapshot limpio compartida via cache del libro (v13.8: una sola vez por
    delta de WS en vez de re-ordenar el libro 6x).

    FIX v14.1 D6: la cache se usa solo si está al corriente del estado actual
    (`cache_is_current`, verificado por secuencia) y SIEMPRE es un corte de 30
    niveles; cada consumidor corta desde ella. Antes `last_clean` podía servir
    un corte de 20 a quien pedía 30 (wofi top-20 vs wall top-30 mezclados) y
    cualquier delta entre lecturas re-ordenaba el libro completo otra vez."""
    if book.cache_is_current():
        bids, asks = book.last_clean
        return bids[:levels], asks[:levels]
    bids, asks = book.get_clean_book(max(levels, 30))
    return bids[:levels], asks[:levels]


def _ewma_scale(prev: float, x: float, alpha: float) -> float:
    return prev + alpha * (abs(x) - prev)


# ================================================================ DIRECCIÓN

# 1. Book Slope / presión posicional (CALIBRADO v13.8)
class BookSlope:
    """Compara volumen 'cerca del touch' vs 'profundo' en cada lado.

    v13.8: normalización por percentil histórico (la desviación media de |v|),
    no multiplicador fijo x2.5 que amplificaba ruido a señal plena.
    """

    def __init__(self, near_levels: int = 5, far_levels: int = 20):
        self.near = near_levels
        self.far = far_levels
        self._scale = 0.05          # escala adaptativa de |v|
        self._alpha = 1 - 0.5 ** (1.0 / 30.0)   # half-life 30s

    def calculate(self, book: SimpleOBM) -> Signal:
        bids, asks = _sides(book, self.far)
        if len(bids) < self.near + 1 or len(asks) < self.near + 1:
            return Signal("book_slope", 0, 0, Direction.NEUTRAL, 0)
        bn = sum(q for _, q in bids[:self.near])
        bf = sum(q for _, q in bids[self.near:self.far])
        an = sum(q for _, q in asks[:self.near])
        af = sum(q for _, q in asks[self.near:self.far])
        if (bn + bf + an + af) <= 0:
            return Signal("book_slope", 0, 0, Direction.NEUTRAL, 0)
        br = bn / max(bn + bf, 1e-9)
        ar = an / max(an + af, 1e-9)
        v = br - ar                    # (+) respaldo comprador pegado al touch
        # escala adaptativa: señal plena = 2x la desviación típica reciente
        self._scale = max(_ewma_scale(self._scale, v, self._alpha), 0.02)
        norm = max(-1.0, min(1.0, v / (self._scale * 2)))
        conf = min(abs(v) / (self._scale * 1.5), 1.0)
        return Signal("book_slope", v, norm, dir_from(norm, 0.12), conf)


# 2. Wall Momentum (muros que CRECEN) — mapas separados v13.8
class WallMomentum:
    """Niveles con >=X BTC que ENGORDAN entre snapshots consecutivos.

    v13.8 FIX F2/C6: `_prev_b` y `_prev_a` separados (antes `{**wb, **wa}`
    fusionaba ambos lados); publicación garantizada con conf>=min_conf incluso
    en neutro.

    FIX v14.1 A4 (dos fallos de anticipación):
      a) un muro NUEVO (precio ausente en el snapshot anterior) puntúA desde su
         primera aparición: apilar 25 BTC es exactamente lo que esta señal debe
         anticipar; antes contaba 0 hasta que el muro engordara OTRA vez.
      b) el gate `poll_sec` devolvía NEUTRAL conf=0.5 en CUALQUIER llamada
         dentro del segundo, congelando crecimiento real detectado entre ticks.
         Ahora la lectura inmediata se ACUMULA en un pending que se publica al
         vencer la ventana (no se descarta).
    """

    def __init__(self, min_wall_btc: float = 15.0, poll_sec: float = 1.0):
        self.min_wall = min_wall_btc
        self.poll = poll_sec
        self._prev_b: Dict[float, float] = {}
        self._prev_a: Dict[float, float] = {}
        self._last_ts = 0.0
        # FIX A4b: acumulador entre ventanas de polling
        self._pend_gb = self._pend_sb = 0.0
        self._pend_ga = self._pend_sa = 0.0
        # FIX v14.7 (log 03:29-03:34): sin base, la PRIMERA lectura compara los
        # muros actuales contra {} -> "crecimiento" fantasma de TODO el muro
        # visible (norm saturado). Ahora la primera lectura fija base y emite
        # neutro; además se renueva base tras huecos >5s (reconexión WS) para no
        # inventar crecimiento con liquidez nueva post-corte.
        self._primed = False

    @staticmethod
    def _diff(cur: Dict[float, float], prev: Dict[float, float],
              new_weight: float = 1.0) -> Tuple[float, float]:
        """(growth, shrink) comparando CONJUNTOS completos de precios.

        new_weight: peso del tamaño inicial de un muro RECIÉN APARECIDO
        (FIX A4a: antes se ignoraba por completo)."""
        growth = shrink = 0.0
        for p, q in cur.items():
            if p in prev:
                d = q - prev[p]
                if d > 0:
                    growth += d
                else:
                    shrink += -d
            else:
                growth += q * new_weight
        # muros que DESAPARECIERON = cancel total (shrink), antes invisibles
        for p, q in prev.items():
            if p not in cur:
                shrink += q
        return growth, shrink

    def calculate(self, book: SimpleOBM) -> Signal:
        now = time.time()
        bids, asks = _sides(book, 30)
        wb = {p: q for p, q in bids if q >= self.min_wall}
        wa = {p: q for p, q in asks if q >= self.min_wall}
        # FIX v14.7: primera lectura (o tras hueco de feed >5s): fijar base sin
        # puntuar. Sin esto, el arranque del bot emitía wall_momentum saturado
        # inventado ("todos los muros acaban de crecer") que sesgaba el bloque
        # anticipativo durante los primeros ticks y contaminaba el conteo de
        # precursores frescos del gate de apertura.
        gap = now - self._last_ts if self._last_ts else 0.0
        if not self._primed or gap > 5.0:
            self._prev_b, self._prev_a = wb, wa
            self._pend_gb = self._pend_sb = self._pend_ga = self._pend_sa = 0.0
            self._last_ts = now
            self._primed = True
            return Signal("wall_momentum", 0, 0, Direction.NEUTRAL, 0.5)
        gb, sb = self._diff(wb, self._prev_b)
        ga, sa = self._diff(wa, self._prev_a)
        self._prev_b, self._prev_a = wb, wa
        if now - self._last_ts < self.poll:
            # FIX A4b: acumular en vez de tirar la lectura
            self._pend_gb += gb; self._pend_sb += sb
            self._pend_ga += ga; self._pend_sa += sa
            return Signal("wall_momentum", 0, 0, Direction.NEUTRAL, 0.5)
        self._last_ts = now
        growth_b = self._pend_gb + gb; shrink_b = self._pend_sb + sb
        growth_a = self._pend_ga + ga; shrink_a = self._pend_sa + sa
        self._pend_gb = self._pend_sb = self._pend_ga = self._pend_sa = 0.0
        # muro que ADELGAZA en un lado = intención opuesta
        raw_b = growth_b - 0.6 * shrink_b
        raw_a = growth_a - 0.6 * shrink_a
        tot = abs(raw_b) + abs(raw_a)
        if tot <= 1e-9:
            return Signal("wall_momentum", 0, 0, Direction.NEUTRAL, 0.5)
        v = (raw_b - raw_a) / tot
        norm = max(-1.0, min(1.0, v * 1.6))
        conf = min(tot / (self.min_wall * 2), 1.0)
        return Signal("wall_momentum", v, norm, dir_from(norm, 0.15),
                      max(conf, 0.5))


# 3. OFI ponderado por profundidad — cancel-incluida v13.8
class WeightedOFI:
    """Order Flow Imbalance del LIBRO con cuenta de NIVELES QUE DESAPARECEN.

    v13.8 FIX C2: los niveles que estaban y ya no cuentan como cancel completo.

    FIX v14.1 A3 (cuatro agujeros restantes):
      a) FLUSH TOTALES: las salidas se comparan contra el mapa COMPLETO de
         precios previos (no solo los top-20 actuales), de modo que un flush
         vía snapshot/reconciliación es visible.
      b) DESPLAZAMIENTO: un nivel que sale del top-N sin cancel explícito
         (el precio se movió) se toma de `book.last_top_b/a` (registro del
         corte anterior hecho por el propio libro) -> cuenta como cancel.
      c) RE-SNAPSHOTS: si el snapshot es idéntico (flag `resnapshot`), NO se
         recalculan deltas: antes generaba norm=-1.0/+1.0 fantasma de "cancel
         masivo" inventado y persistía en el tick siguiente.
      d) ESCALA: el blend 50/50 con `_raw_prev` saturaba ±1 con UN solo cancel
         pequeño y lo mantenía sin eventos nuevos. Ahora la normalización usa
         una escala EWMA mínima crecida con suelo alto (10 BTC-equivalentes)
         y la componente crua decae geométricamente si no hay flujo nuevo.
    """

    def __init__(self, levels: int = 20, decay_half_life: float = 4.0):
        self.levels = levels
        self.half_life = decay_half_life
        self._prev_b: Dict[float, float] = {}
        self._prev_a: Dict[float, float] = {}
        self._ewma = 0.0
        self._ewma_n = 0.0
        self._last_ts = 0.0
        self._last_seq = -1          # secuencia del libro ya consumida
        self._pending_raw = 0.0      # cruas acumuladas desde última publicación

    def reset_baseline(self, bids, asks):
        """FIX v14.3 G2 (arranque con 'cancel masivo' inventado): al arrancar el
        bot (o tras una reconexión WS que trae un snapshot nuevo) `_prev_b/_prev_a`
        están vacíos o desincronizados y la comparación de deltas contra el
        registro top-N del libro (`book.last_top_b/a`, que sobrevive a la
        reconexión) fabrica cancels/adds ENORMES inexistentes. En los logs se ve
        como `order_flow_imb=-1.0` / `ofi_weighted` saturado justo después de
        conectar: el motor direccional arrancaba sesgado en contra del mercado
        real durante los primeros segundos. Este método fija la línea base sin
        emitir delta alguno."""
        self._prev_b = dict(bids)
        self._prev_a = dict(asks)
        self._ewma = 0.0
        self._ewma_n = 0.0
        self._pending_raw = 0.0
        self._last_ts = time.time()

    def calculate(self, book: SimpleOBM) -> Signal:
        now = time.time()
        bids, asks = _sides(book, self.levels)
        cb = dict(bids)
        ca = dict(asks)

        # FIX v14.1 A3c (completado): un RE-SNAPSHOT identico de Bybit no es un
        # evento de microestructura. Si la ultima mutacion del libro fue un
        # snapshot real (`book._last_snap_seq`) y esa es la secuencia que ya
        # consumimos, el "delta" contra el estado anterior es artefacto del
        # corte top-N (niveles fuera del corte) o directamente inexistente:
        # se devuelve neutro en vez de inventar norm=-1/+1 "cancel masivo".
        # FIX A3c-2: EXCEPCIÓN — si ese snapshot reetiquetó niveles viejos
        # (`_last_snap_event_seq`: flush/reconciliación con precios nuevos),
        # el contenido sí cambió y los deltas deben procesarse normalmente.
        if (self._last_seq == book._last_snap_seq and self._last_seq != -1
                and book._last_snap_event_seq != book._last_snap_seq):
            self._last_seq = book._seq
            return Signal("ofi_weighted", 0, 0, Direction.NEUTRAL, 0.2)
        self._last_seq = book._seq

        # FIX v14.3 G2: PRIMERA lectura del arranque (o tras una reconexión que
        # borró el libro): no existe línea base, así que los deltas contra
        # `book.last_top_b/a` (registro superviviente de la sesión anterior)
        # serían artefactos puros. Se fija la base y se emite neutro.
        if not self._prev_b and not self._prev_a:
            self.reset_baseline(cb, ca)
            return Signal("ofi_weighted", 0, 0, Direction.NEUTRAL, 0.2)

        near_ofi = deep_ofi = 0.0
        prev_order_b = {p: i for i, p in enumerate(
            sorted(self._prev_b.keys(), reverse=True))}
        prev_order_a = {p: i for i, p in enumerate(
            sorted(self._prev_a.keys()))}

        # bids: add = alcista(+), cancel/reduce = bajista(-)
        for i, (p, q) in enumerate(bids):
            d = q - self._prev_b.get(p, 0.0)
            if i < 5:
                near_ofi += d * 3.0
            else:
                deep_ofi += d
        # FIX A3a/b: niveles bid que DESAPARECIERON. Se busca en el mapa previo
        # COMPLETO y ademas en el top-N registrado por el libro (niveles que
        # salieron del corte por desplazamiento de precio tambien eran liquidez
        # visible y su perdida es real).
        vanished_b = set(self._prev_b) | set(book.last_top_b)
        for p in vanished_b:
            if p in cb:
                continue
            q = self._prev_b.get(p) or book.last_top_b.get(p) or 0.0
            if q <= 0:
                continue
            lvl = prev_order_b.get(p, 99)
            if lvl < 5:
                near_ofi -= q * 3.0
            else:
                deep_ofi -= q
        # asks: add = bajista(-), cancel = alcista(+)
        for i, (p, q) in enumerate(asks):
            d = q - self._prev_a.get(p, 0.0)
            if i < 5:
                near_ofi -= d * 3.0
            else:
                deep_ofi -= d
        vanished_a = set(self._prev_a) | set(book.last_top_a)
        for p in vanished_a:
            if p in ca:
                continue
            q = self._prev_a.get(p) or book.last_top_a.get(p) or 0.0
            if q <= 0:
                continue
            lvl = prev_order_a.get(p, 99)
            if lvl < 5:
                near_ofi += q * 3.0
            else:
                deep_ofi += q
        self._prev_b, self._prev_a = cb, ca

        dt = max(now - self._last_ts, 1e-6) if self._last_ts else 1.0
        # FIX v14.7 (log 03:29-03:34): tras un hueco de feed >5s (reconexión WS,
        # micro-corte) los deltas comparan contra una base obsoleta y fabrican
        # "cancel/add masivo" fantasma; además `raw` se acumulaba en
        # _pending_raw SIN decaer durante el hueco -> arrancaba saturado. Se
        # renueva la base, se purga el pending y se emite neutro este tick.
        if dt > 5.0:
            self.reset_baseline(cb, ca)
            self._last_ts = now
            return Signal("ofi_weighted", 0, 0, Direction.NEUTRAL, 0.2)
        self._last_ts = now
        alpha = 1 - 0.5 ** (dt / self.half_life)
        raw = near_ofi + 0.3 * deep_ofi
        self._ewma += alpha * (raw - self._ewma)
        self._ewma_n = max(_ewma_scale(self._ewma_n, raw, alpha), 10.0)
        # FIX A3d: la componente crua es un PENDING que decae si no llega flujo
        # nuevo (antes `0.5*(raw+_raw_prev)/2` mezclaba lecturas repetidas y
        # saturaba la señal con un solo cancel pequeno durante ticks siguientes).
        decay = 0.5 ** (dt / 1.5)          # vida de 1.5s del impulso inmediato
        blended = 0.6 * self._ewma + 0.4 * self._pending_raw
        # acumular para proxima lectura; decaido por tiempo
        self._pending_raw = self._pending_raw * decay + raw

        scale = max(self._ewma_n, 10.0)
        norm = max(-1.0, min(1.0, blended / scale))
        conf = min(scale / 40.0, 1.0)
        return Signal("ofi_weighted", raw, norm, dir_from(norm, 0.18), conf)


# 4. Micro-price divergence — escala adaptativa v13.8
class MicroPriceDeviation:
    """micro = (Qa*Bid + Qb*Ask)/(Qb+Qa) vs último trade.

    v13.8 FIX C1/F1: normalizador fijo /0.02% sustituido por desviación media
    adaptativa (half-life 60s). El motor_main ahora llama UNA sola vez por
    batch (F1 resuelto en engine_main).
    """

    def __init__(self, smooth_sec: float = 1.5, scale_half_life: float = 60.0):
        self._ema = 0.0
        self._scale = 0.004           # % típico; arranca conservador
        self._last_ts = 0.0
        self._prev_last = 0.0         # v13.8: devolucion de la EMA por el propio tick
        self._smooth = smooth_sec
        self._scale_alpha = 1 - 0.5 ** (1.0 / scale_half_life)

    def calculate(self, book: SimpleOBM, last_price: float) -> Signal:
        if last_price <= 0:
            return Signal("micro_price_dev", 0, 0, Direction.NEUTRAL, 0)
        bids, asks = _sides(book, 1)
        if not bids or not asks:
            return Signal("micro_price_dev", 0, 0, Direction.NEUTRAL, 0)
        bp, bq = bids[0]
        ap, aq = asks[0]
        if bq + aq <= 0:
            return Signal("micro_price_dev", 0, 0, Direction.NEUTRAL, 0)
        micro = (aq * bp + bq * ap) / (bq + aq)
        dev_pct = (micro - last_price) / last_price * 100
        # REVERTIDO (FIX v14.1 A2): la compensacion de v13.8 anadia el salto de
        # last_price COMPLETO a la EMA (`self._ema += d_last`) sin escalar por
        # alpha. Efecto medido: libro quieto + salto de +25 USD => norm=+1.0 LONG
        # cuando la desviacion micro REAL es negativa (el micro NO subio). La
        # senal pasaba a PERSEGUIR el ultimo tick: momentum reactivo disfrazado
        # de anticipacion (causa directa de los "LONGs mientras el precio bajaba"
        # del log 00:12). El decaimiento legitimo de la EMA es el comportamiento
        # correcto: un trade en el touch DISUELVE la divergencia, no la conserva.
        self._prev_last = last_price   # FIX v14.1 A2b: mantener coherente la ref
        now = time.time()
        dt = now - self._last_ts if self._last_ts else 0.5
        if dt > 2.0:
            # FIX v14.1 A2c: tras inactividad/reconexion WS no hay referencia
            # valida del tick anterior -> decaimos la EMA hacia el nuevo estado
            # en lugar de arrastrar sesgo viejo (artefacto de reconexion).
            self._ema *= 0.25
        self._last_ts = now
        alpha = 1 - 0.5 ** (max(dt, 1e-6) / self._smooth)
        self._ema += alpha * (dev_pct - self._ema)
        self._scale = max(_ewma_scale(self._scale, dev_pct, self._scale_alpha),
                          0.0015)     # suelo anti-congelación
        norm = max(-1.0, min(1.0, self._ema / (self._scale * 2)))
        conf = min(abs(self._ema) / self._scale, 1.0)
        return Signal("micro_price_dev", self._ema, norm, dir_from(norm, 0.2), conf)


# 5. CVD Divergence — z-score v13.8
class CVDDivergence:
    """Divergencia CVD vs precio (absorción anticipativa).

    v13.8 FIX C3: dc/40 BTC fijo -> z-score del delta de CVD sobre su propio
    histórico de deltas de ventana (45s), para que "mucho flujo" sea relativo
    al régimen actual.
    """

    def __init__(self, window_sec: float = 45.0):
        self.window = window_sec
        self.cvd_hist: deque = deque(maxlen=1200)
        self.price_hist: deque = deque(maxlen=1200)
        self._delta_hist: deque = deque(maxlen=120)   # deltas por lectura
        self._mean_d = 0.0
        self._var_d = 1.0
        # FIX v14.1 A6a: el z-score se calculaba contra los deltas de las
        # PROPIAS ventanas superpuestas (autocorreladas y con la ventana actual
        # incluida -> cn~0 casi siempre). Ahora el historico son DIFERENCIAS
        # de CVD entre puntos ~3s separados (muestras cuasi-independientes) y
        # la medida es el delta del ULTIMO paso de 3s.
        self._sample_every = 3.0
        self._last_sample_ts = 0.0
        # FIX v14.7 (log 03:29-03:34): el gate de apertura contaba esta señal
        # como "precursor fresco alineado" aunque su confidence era ~0 (no hay
        # divergencia). En engine_main se exige ahora conf>=0.40 Y |norm|>=0.25
        # para contarla; aquí solo se purga el historial tras huecos de feed
        # >60s (reconexión) para no mezclar pasos de dos sesiones en el z-score.
        self._last_calc_ts = 0.0

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        prev_cvd = self.cvd_hist[-1][1] if self.cvd_hist else 0.0
        delta = qty if not maker else -qty
        self.cvd_hist.append((ts, prev_cvd + delta))
        self.price_hist.append((ts, price))

    def calculate(self) -> Signal:
        now = time.time()
        # FIX v14.7: tras un corte largo del feed, _delta_hist/ventanas mezclan
        # datos pre y post reconexión (step_dc entre sesiones -> z fantasma).
        if self._last_calc_ts and now - self._last_calc_ts > 60.0:
            self.cvd_hist.clear()
            self.price_hist.clear()
            self._delta_hist.clear()
            self._mean_d, self._var_d = 0.0, 1.0
            self._last_sample_ts = 0.0
        self._last_calc_ts = now
        c = [(t, v) for t, v in self.cvd_hist if t >= now - self.window]
        p = [(t, v) for t, v in self.price_hist if t >= now - self.window]
        if len(c) < 10 or len(p) < 10:
            return Signal("cvd_divergence", 0, 0, Direction.NEUTRAL, 0)
        dc = c[-1][1] - c[0][1]
        dp = (p[-1][1] - p[0][1]) / p[0][1] * 100 if p[0][1] else 0.0
        # muestreo cada ~3s para poblar el historico de incrementos NO solapados
        step_dc = 0.0
        if now - self._last_sample_ts >= self._sample_every:
            if self._last_sample_ts > 0:
                prev_v = None
                for t, v in c:
                    if t <= self._last_sample_ts:
                        prev_v = v
                    else:
                        break
                if prev_v is not None:
                    step_dc = c[-1][1] - prev_v
                    self._delta_hist.append(step_dc)
                    n = len(self._delta_hist)
                    if n >= 8:
                        mean = sum(self._delta_hist) / n
                        var = sum((x - mean) ** 2 for x in self._delta_hist) / n
                        self._mean_d, self._var_d = mean, max(var, 1e-9)
            self._last_sample_ts = now
        # medir SIEMPRE el ultimo paso (no el acumulado de 45s, que era ~0
        # tras normalizar contra su propia media -> conf casi nunca >=0.40)
        meas = step_dc if step_dc != 0.0 else dc
        # FIX A6b: divisor con suelo relativo al volumen de la ventana: en
        # regimenes muertos var_d colapsaba y un flujo de 0.2 BTC parecia un
        # z-score enorme. Suelo = 2 BTC por paso.
        sigma = max(self._var_d ** 0.5, 2.0)
        cn = max(-1.0, min(1.0, (meas - self._mean_d) / (2.5 * sigma)))
        pn = max(-1.0, min(1.0, dp / 0.08))
        div = cn - pn                     # >0: flujo comprador SIN precio alcista
        norm = max(-1.0, min(1.0, div))
        strength = abs(div)
        if abs(pn) >= 0.6:
            strength *= 0.35              # ya hay tendencia: no es divergencia
        conf = min(strength / 0.8, 1.0)
        return Signal("cvd_divergence", div, norm, dir_from(norm, 0.25), conf)


# 6. Trade Arrival Run — min_run=4 + peso USD v13.8
class TradeArrivalRun:
    """Rachas de takers del mismo lado con precio quieto (iceberg being eaten).

    v13.8 FIX C4: min_run 8->4 (rachas de 8 son rarísimas en BTC líquido: la
    señal vivía en 0); `usd` pasa a usarse como peso de agresión; ventana 8s.
    """

    def __init__(self, window_sec: float = 30.0, min_run: int = 4):
        # v14.0: ventana 8s -> 30s (horizon real del flujo agresor; con 8s el
        # score solo confirmaba lo que YA paso, no anticipaba)
        self.window = window_sec
        self.min_run = min_run
        self.trades: deque = deque(maxlen=400)

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, price, qty, maker))

    def calculate(self) -> Signal:
        now = time.time()
        recent = [t for t in self.trades if t[0] >= now - self.window]
        if len(recent) < self.min_run:
            return Signal("trade_arrival", 0, 0, Direction.NEUTRAL, 0.5)
        side = 1 if not recent[-1][3] else -1
        run = 0
        usd = 0.0
        for _, pr, q, mk in reversed(recent):
            s = -1 if mk else 1
            if s != side:
                break
            run += 1
            usd += pr * q
        move = (recent[-1][1] - recent[0][1]) / recent[0][1] * 100 if recent[0][1] else 0
        quiet = max(0.0, 1.0 - abs(move) / 0.05)      # precio plano = 1
        size_w = min(usd / 50_000.0, 1.0)             # agresión material (USD)
        v = side * min(run / 12.0, 1.0) * (0.3 + 0.4 * quiet + 0.3 * size_w)
        norm = max(-1.0, min(1.0, v))
        conf = min(run / (self.min_run * 2), 1.0) * (0.5 + 0.5 * quiet)
        return Signal("trade_arrival", v, norm, dir_from(norm, 0.2),
                      max(conf, 0.5))


# ================================================================ AMPLITUD

# 7. Liquidity Thinning — base LARGA + hint corregido v13.8
class LiquidityThinning:
    """Ambos lados perdiendo tamaño simultáneamente vs una BASE DE RÉGIMEN.

    v13.8 FIX F3/C7: half-life 20s -> 300s (si no, la base persigue al estado
    actual y "fino permanente" deja de puntuar justo cuando más riesgo hay);
    direction_hint invertido corregido (bids más delgados => ceden hacia abajo
    => SHORT).
    """

    def __init__(self, levels: int = 20, half_life: float = 300.0):
        self.levels = levels
        self.half_life = half_life
        self._base_bid = 0.0
        self._base_ask = 0.0
        self._last_ts = 0.0

    def calculate(self, book: SimpleOBM) -> Signal:
        now = time.time()
        bids, asks = _sides(book, self.levels)
        tb = sum(q for _, q in bids)
        ta = sum(q for _, q in asks)
        if self._last_ts == 0:
            self._base_bid, self._base_ask = tb, ta
            self._last_ts = now
            return Signal("liquidity_thinning", 0, 0, Direction.NEUTRAL, 0.5)
        dt = now - self._last_ts
        # FIX v14.1 A7a: la base solo se actualiza al vencer la ventana de
        # observacion (half-life). Antes `alpha` era ~0.06 POR TICK (loop a
        # 0.5s): con un goteo sostenido de cancels la base PERSEGUIA el estado
        # dentro de ~8s y thin colapsaba a 0 exactamente cuando mas importaba
        # (fuga rapida de liquidez). Ahora el ratio se mide contra una base que
        # NO persigue fluctuaciones de <half_life/2.
        if dt >= self.half_life * 0.05:      # actualizar como maximo cada ~15s
            self._last_ts = now
            alpha = 1 - 0.5 ** (dt / self.half_life)
            self._base_bid += alpha * (tb - self._base_bid)
            self._base_ask += alpha * (ta - self._base_ask)
        if self._base_bid <= 0 or self._base_ask <= 0:
            return Signal("liquidity_thinning", 0, 0, Direction.NEUTRAL, 0.5)
        rb = tb / self._base_bid
        ra = ta / self._base_ask
        thin = max(0.0, 1.0 - min(rb, ra))
        both_thin = thin if (rb < 1.0 and ra < 1.0) else thin * 0.4
        norm = min(both_thin / 0.30, 1.0)             # 30% bajo la base = pleno
        hint = Direction.NEUTRAL
        if norm > 0.3:
            # el lado MÁS delgado cede primero: bids finos -> cae -> SHORT
            hint = Direction.SHORT if rb < ra else Direction.LONG
        return Signal("liquidity_thinning", thin, norm, hint, 0.8,
                      metadata={"hint": hint.name, "rb": round(rb, 3),
                                "ra": round(ra, 3)})


# 8. Spread Compression / Coiling — solo compresión real v13.8
class SpreadCompression:
    """Spread bajo su base + tamaño cerca del touch CRECIENDO = coiling.

    v13.8 FIX C8: antes puntuaba norm>0 aunque el spread estuviera ANCHO
    (ratio>1) porque emitía con conf fija 0.7; ahora norm exige compresión y
    añade el componente de apilado (sin apilado la mitad de fuerza). Base con
    half-life 300s para representar el régimen, no el instante.
    """

    def __init__(self, half_life: float = 300.0):
        self.half_life = half_life
        self._base_spread = 0.0
        self._prev_near = 0.0
        self._last_ts = 0.0

    def calculate(self, book: SimpleOBM, price: float) -> Signal:
        now = time.time()
        bid, ask = book.get_best_bid_ask()
        if bid <= 0 or ask <= 0 or price <= 0:
            return Signal("spread_compression", 0, 0, Direction.NEUTRAL, 0.5)
        sp = (ask - bid) / price * 100
        if self._last_ts == 0:
            self._base_spread, self._last_ts = sp, now
            return Signal("spread_compression", 0, 0, Direction.NEUTRAL, 0.5)
        dt = now - self._last_ts
        # FIX v14.1 A7b (espejo de thinning): la base no persigue el estado en
        # cada tick; se refresca como maximo cada half_life*0.05 (~15s).
        if dt >= self.half_life * 0.05:
            self._last_ts = now
            alpha = 1 - 0.5 ** (dt / self.half_life)
            self._base_spread += alpha * (sp - self._base_spread)
        if self._base_spread <= 0:
            return Signal("spread_compression", 0, 0, Direction.NEUTRAL, 0.5)
        ratio = sp / self._base_spread                 # <1 = comprimido
        comp = max(0.0, 1.0 - ratio)
        # apilado cerca del touch (primeros 5 niveles, ambos lados)
        bids, asks = _sides(book, 5)
        near = sum(q for _, q in bids) + sum(q for _, q in asks)
        stacking = 1.0 if self._prev_near <= 0 else min(near / self._prev_near, 2.0)
        self._prev_near = near if near > 0 else self._prev_near
        stack_bonus = max(0.0, min((stacking - 1.0), 0.5)) * 2.0   # 0..1
        norm = min(comp / 0.4, 1.0) * (0.5 + 0.5 * stack_bonus)
        return Signal("spread_compression", ratio, norm, Direction.NEUTRAL, 0.7,
                      metadata={"ratio": round(ratio, 3),
                                "stacking": round(stacking, 2)})


# 9. Volume Acceleration — base excluyente del burst v13.8
class VolumeAcceleration:
    """Aceleración del flujo: tasa últimos 5s vs tasa de la VENTANA PREVIA.

    v13.8 FIX F4: la base de 60s incluía el propio burst (dilución: techo
    ~1.15x). Ahora base = trades entre [now-60, now-5] (ventana previa pura),
    y norm despega desde accel>1.3 hasta x4.
    """

    def __init__(self, burst_sec: float = 5.0, base_sec: float = 60.0):
        # FIX v14.1 C2 (antes bug de v14.0): el ctor guardaba `self.burst = 30.0`
        # hardcodeado IGNORANDO burst_sec, pero el umbral de norm ((accel-1.3)/2.7)
        # sigue calibrado para burst de 5s. Con ventana de 30s casi cualquier
        # repunte sostenido daba accel>=4 -> norm=1.0 permanente (amplitud
        # inflada: "amp=0.66" fantasma en las aperturas del log). Ahora se usa
        # el parametro real y la escala se ajusta al tamano de la ventana.
        self.burst = float(burst_sec)
        self.base_w = base_sec
        self.trades: deque = deque(maxlen=6000)

    def add_trade(self, ts: float, price: float, qty: float, maker: bool):
        self.trades.append((ts, qty))

    def calculate(self) -> Signal:
        now = time.time()
        burst_v = 0.0
        base_v = 0.0
        b_lo = now - self.base_w
        b_hi = now - self.burst
        for t, q in self.trades:
            if t >= b_hi:
                burst_v += q
            elif t >= b_lo:
                base_v += q
        rate_now = burst_v / self.burst
        rate_base = base_v / max(self.base_w - self.burst, 1e-9)
        if rate_base <= 1e-9:
            # sin referencia previa: burst absoluto pequeño = neutro
            accel = 1.0 if burst_v < 2.0 else 3.0
        else:
            accel = rate_now / rate_base
        # FIX v14.1 C2: umbral adaptativo. Ventanas cortas (5s) son mas
        # ruidosas -> exigen mas aceleracion; ventanas largas ya promedian.
        # accel_efectiva normalizada: arranque en 1.3x para 5s, 1.15x para 30s.
        start_thr = 1.3 if self.burst <= 5.0 else 1.15
        span_thr = 2.7 if self.burst <= 5.0 else 1.85   # techo 4x / 3x
        norm = min(max((accel - start_thr) / span_thr, 0.0), 1.0)
        conf = 0.85 if burst_v > 0 else 0.5
        return Signal("volume_accel", accel, norm, Direction.NEUTRAL, conf)
