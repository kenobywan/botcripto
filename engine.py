"""
Engine v12.8 - SignalBus sin lock + agrupación por horizonte (anticipación/reacción)
"""
import asyncio
import time
from typing import Dict

from models import Direction, EngineOutput, Signal


class SignalBus:
    # FIX v13.3 CRÍTICO: señales que caducan solas. Antes una señal publicada se
    # quedaba "viva" para siempre aunque su calculadora dejara de emitir (p. ej.
    # book_slope devuelve NEUTRAL/conf=0 y NO se publica cuando el libro tiene
    # <5 niveles; o un fallo silencioso en un handler). El score direccional se
    # anclaba en ese último valor -> el bot mantenía la posición 1 hora aunque
    # toda la anticipación hubiera girado a reversal. TTL por tipo de señal.
    DEFAULT_TTL = 4.0
    TTL_MAP = {
        # reactivas de ventana larga / REST lento
        "cvd": 10.0, "vwap_distance": 10.0, "candle_delta": 10.0,
        "oi_delta": 180.0, "ls_ratio": 180.0,
        # anticipativas de libro/trade: vida corta (son de SEGUNDOS)
        "ofi_weighted": 6.0, "micro_price_dev": 6.0, "book_slope": 6.0,
        "wall_momentum": 6.0, "trade_arrival": 6.0, "cvd_divergence": 10.0,
        "liquidity_thinning": 6.0, "spread_compression": 10.0, "volume_accel": 6.0,
        # amplitud reactiva
        "realized_vol": 10.0, "volume_spike": 10.0, "volatility_ratio": 10.0,
        "atr_expansion": 10.0, "book_depth": 6.0, "book_elasticity": 6.0,
        "obi_anti_spoof": 6.0, "large_trades": 10.0, "absorption": 10.0,
        "taker_ratio": 10.0, "order_flow_imb": 10.0,
    }

    def __init__(self):
        self._latest: Dict[str, tuple] = {}   # name -> (Signal, deadline_mono)
        self._pub_mono: Dict[str, float] = {}  # name -> mono del último publish
        self._last_publish_time = 0.0
        # FIX v13.5: reloj monotónico CACHEADO. get_running_loop().time() era
        # correcto pero ahora centralizamos la fuente para TTL y feed_age.
        try:
            self._mono = asyncio.get_running_loop().time
        except RuntimeError:
            import time as _t
            self._mono = _t.monotonic

    def publish_nowait(self, signal: Signal):
        """FIX v12.6: sin lock ni awaits. Escribir en dict es atomico dentro del mismo loop."""
        now = self._mono()
        ttl = self.TTL_MAP.get(signal.name, self.DEFAULT_TTL)
        self._latest[signal.name] = (signal, now + ttl)
        self._pub_mono[signal.name] = now
        self._last_publish_time = now

    async def publish(self, signal: Signal):
        self.publish_nowait(signal)

    def get_all_latest(self) -> Dict[str, Signal]:
        """Devuelve solo señales VIVAS (descarta las caducadas al leer).

        FIX v13.5 CRÍTICO (zombie en el reversal): el TTL controla la MUERTE de
        la señal, pero NO su frescura. Con feed irregular (WS medio-caído o
        throttles de 1s), una señal publicada hace 8-10s seguía "viva" y contaba
        igual que una de hace 0.2s para `fresh_anticip_aligned` y para el conteo
        de señales opuestas del cierre por REVERSAL_ANT. Los logs lo mostraban:
        cierres con `precursor_fresco(0<1)` en HOLD justo antes/después de
        REVERSAL_ANT disparados por precursores viejos. Ahora además se exige
        edad <= fresh_age_sec para que la señal sea CONTADA como fresca; las
        más antiguas siguen vivas (ponderan con descuento) pero no activan
        aperturas ni cierres por anticipación.
        """
        now = self._mono()
        out: Dict[str, Signal] = {}
        expired = []
        for name, (sig, deadline) in self._latest.items():
            if deadline >= now:
                out[name] = sig
            else:
                expired.append(name)
        for name in expired:
            self._latest.pop(name, None)
            self._pub_mono.pop(name, None)
        return out

    def get_fresh(self, fresh_age_sec: float) -> Dict[str, Signal]:
        """Señales vivas CUYA EDAD REAL <= fresh_age_sec (para gates de apertura
        y cierre por reversión anticipada)."""
        now = self._mono()
        cutoff = now - fresh_age_sec
        return {n: s for n, (s, dl) in list(self._latest.items())
                if dl >= now and self._pub_mono.get(n, 0.0) >= cutoff}

    def feed_age(self) -> float:
        """Segundos desde la última publicación de CUALQUIER señal."""
        if self._last_publish_time == 0:
            return 999.0
        return max(0.0, self._mono() - self._last_publish_time)

    def is_alive(self) -> bool:
        if self._last_publish_time == 0:
            return False
        try:
            now = asyncio.get_running_loop().time()
        except RuntimeError:
            import time as _t
            now = _t.monotonic()
        return (now - self._last_publish_time) < 30.0




# ------------------------- agrupación por horizonte -------------------------
ANTICIP_DIR_WEIGHTS: Dict[str, float] = {
    "micro_price_dev": 0.22,
    "ofi_weighted": 0.20,
    "book_slope": 0.14,
    "wall_momentum": 0.12,
    "trade_arrival": 0.16,
    "cvd_divergence": 0.16,
}

REACT_DIR_WEIGHTS: Dict[str, float] = {
    "cvd": 0.17,
    "taker_ratio": 0.12,
    "large_trades": 0.12,
    "absorption": 0.12,
    "order_flow_imb": 0.10,
    "obi_anti_spoof": 0.09,
    "candle_delta": 0.08,
    "oi_delta": 0.08,
    "ls_ratio": 0.05,
    "vwap_distance": 0.07,
}

ANTICIP_AMP_WEIGHTS: Dict[str, float] = {
    "liquidity_thinning": 0.40,
    "spread_compression": 0.25,
    "volume_accel": 0.35,
}

REACT_AMP_WEIGHTS: Dict[str, float] = {
    "realized_vol": 0.22,
    "volume_spike": 0.20,
    "volatility_ratio": 0.18,
    "atr_expansion": 0.16,
    "book_depth": 0.12,
    "book_elasticity": 0.12,
}

# compatibilidad total con código viejo que importe estos dicts
DIR_WEIGHTS: Dict[str, float] = {**ANTICIP_DIR_WEIGHTS, **REACT_DIR_WEIGHTS}
AMP_WEIGHTS: Dict[str, float] = {**ANTICIP_AMP_WEIGHTS, **REACT_AMP_WEIGHTS}

# mezcla entre bloques
DIR_BLEND = 0.55          # peso del bloque anticipativo sobre el reactivo
AMP_LEAD = 0.60           # cuánto manda el bloque anticipativo en amplitud


def _block_score(signals: Dict[str, Signal], weights: Dict[str, float],
                 min_conf: float, pub_mono: Dict[str, float] = None,
                 mono=None):
    """Score [-1,1] de un bloque + señales activas usadas.

    FIX v13.5 (reversal zombie): descuento por edad. Una señal viva pero con
    >2s de antigüedad pondera menos; a los 8s casi no aporta. Así el blend
    anticipativo deja de "creer" en microestructura de hace 10s cuando el feed
    se interrumpe (caso real del log 23:42: price y scores idénticos durante
    12 ticks -> todo el score provenía de señales congeladas).

    FIX v14.1 D4 (dos relojes): antes la edad se medía con `time.time()`
    (wall clock) contra `Signal.timestamp`, mientras el TTL del bus usaba el
    reloj MONOTÓNICO del loop. Un salto de reloj/NTP corrompía a la vez el
    descuento de frescura y el gate de apertura. Ahora, si el caller pasa
    `pub_mono`/`mono` (fuentes del propio bus), la edad se calcula 100% en
    monotónico; wall clock queda solo como fallback para callers antiguos.
    """
    num = den = conf_sum = 0.0
    active = []
    for name, sig in signals.items():
        if name not in weights or sig.confidence < min_conf:
            continue
        # FIX v14.1 D4: edad real en el MISMO reloj que el TTL del bus.
        if pub_mono is not None and mono is not None and name in pub_mono:
            age = max(0.0, mono() - pub_mono[name])
        else:
            age = max(0.0, time.time() - sig.timestamp)
        freshness = 1.0 if age <= 2.0 else max(0.15, 1.0 - (age - 2.0) / 6.0)
        w = weights[name] * sig.confidence * freshness
        num += sig.normalized * w
        den += w
        conf_sum += sig.confidence * weights[name] * freshness
        active.append(sig)
    if den <= 0:
        return 0.0, 0.0, []
    score = num / den
    conf = conf_sum / sum(weights.values())
    return score, conf, active


def calculate_engine(bus, weights: dict, min_conf: float = 0.40) -> EngineOutput:
    """API compatible con v12.6: puntúa contra el dict `weights` que le pase el caller.

    El engine_main nuevo llama a calculate_grouped(); esta función queda para
    tests/callers antiguos (usa el dict plano DIR/AMP_WEIGHTS sin capas).
    """
    active = {k: v for k, v in bus.get_all_latest().items()
              if k in weights and v.confidence >= min_conf}
    if not active:
        return EngineOutput(0.0, Direction.NEUTRAL, 0.0, [])
    num = den = conf_sum = 0.0
    for name, sig in active.items():
        w = weights[name] * sig.confidence
        num += sig.normalized * w
        den += w
        conf_sum += sig.confidence * weights[name]
    score = num / den if den > 0 else 0.0
    conf = conf_sum / sum(weights.values())
    direction = (Direction.LONG if score > 0.08
                 else (Direction.SHORT if score < -0.08 else Direction.NEUTRAL))
    return EngineOutput(score, direction, conf, list(active.values()))


def calculate_directional(bus, min_conf: float = 0.40) -> EngineOutput:
    """Score direccional con capas anticipación/reacción + confluencia."""
    latest = bus.get_all_latest()
    # FIX v14.1 D4: pasar las fuentes monotónicas del bus para que la edad de
    # frescura use el MISMO reloj que el TTL (antes time.time vs loop.time).
    _pm = getattr(bus, "_pub_mono", None)
    _mono = getattr(bus, "_mono", None)
    a_score, a_conf, a_act = _block_score(latest, ANTICIP_DIR_WEIGHTS, min_conf,
                                          pub_mono=_pm, mono=_mono)
    r_score, r_conf, r_act = _block_score(latest, REACT_DIR_WEIGHTS, min_conf,
                                         pub_mono=_pm, mono=_mono)

    # confluencia anticipativa: >=2 precursores fuertes en la misma dirección
    # FIX v14.1 B2 (bonus mal firmado): antes el bonus usaba el signo de
    # `a_score` (mayoritario ponderado), pero se activaba cuando >=2 señales
    # FUERTES compartían signo — que puede ser OPUESTO al promedio del bloque
    # (p.ej. 2 precursores SHORT -0.9 y uno LONG +0.4: a_score>0 y el bonus
    # sumaba LONG pese a que los fuertes eran SHORT). Ahora el bonus firma con
    # el signo REAL compartido por las señales fuertes (el consenso que dice
    # representar), no con el promedio.
    strong_dir = [s for s in a_act
                  if abs(s.normalized) >= 0.35 and s.confidence >= 0.5
                  and s.direction != Direction.NEUTRAL]
    signs = set((1 if s.normalized > 0 else -1) for s in strong_dir)
    n_strong = len(strong_dir)
    anticip_bonus = 0.0
    if n_strong >= 2 and len(signs) == 1:
        _strong_sign = signs.pop()
        # coherencia mínima: si el blend del bloque va en contra del consenso
        # fuerte, el bonus solo amortigua hacia cero, nunca invierte el score.
        if _strong_sign * a_score >= 0:
            anticip_bonus = 0.10 * min(n_strong, 4) / 4 * _strong_sign
        else:
            anticip_bonus = -min(abs(a_score), 0.10 * min(n_strong, 4) / 4) * (
                1 if a_score > 0 else -1)

    blend = DIR_BLEND * a_score + (1 - DIR_BLEND) * r_score + anticip_bonus

    # contradicción fuerte anticipación vs reacción -> amortiguar
    if a_score * r_score < 0 and abs(a_score) > 0.3 and abs(r_score) > 0.3:
        blend *= 0.6

    conf = 0.6 * a_conf + 0.4 * r_conf
    allsig = a_act + r_act
    direction = (Direction.LONG if blend > 0.08
                 else (Direction.SHORT if blend < -0.08 else Direction.NEUTRAL))
    out = EngineOutput(blend, direction, conf, allsig)
    out.metadata = {  # type: ignore[attr-defined]
        "anticip_score": round(a_score, 3), "react_score": round(r_score, 3),
        "n_anticip_strong": n_strong, "bonus": round(anticip_bonus, 3)}
    return out


def calculate_amplitude(bus, min_conf: float = 0.40) -> EngineOutput:
    """Amplitud: los precursores mandan; basta uno fuerte si el resto no lo niega.

    Usa max(bloque_anticip, blend) para que un libro vaciándose + volumen
    acelerando no se diluya contra una ATR todavía baja (rezago típico).
    """
    latest = bus.get_all_latest()
    _pm = getattr(bus, "_pub_mono", None)
    _mono = getattr(bus, "_mono", None)
    a_score, a_conf, a_act = _block_score(latest, ANTICIP_AMP_WEIGHTS, min_conf,
                                          pub_mono=_pm, mono=_mono)
    r_score, r_conf, r_act = _block_score(latest, REACT_AMP_WEIGHTS, min_conf,
                                          pub_mono=_pm, mono=_mono)

    if a_act and r_act:
        blend = AMP_LEAD * a_score + (1 - AMP_LEAD) * r_score
        # FIX v14.1 C5: `score = max(blend, a_score*0.85)` tomaba el MAXIMO de
        # dos estimadores FUERTEMENTE correlacionados (ambos incluyen a_score):
        # E[max(X,Y)] > max(E[X],E[Y]) -> sesgo positivo sistematico de
        # amplitud (inflaba el gate y abria con "amp" fantasma). El atajo de
        # precursores se conserva pero como PISO CONDICIONADO: solo vale si el
        # bloque reactivo NO lo niega (r_score >= 0 o debil). Si la reaccion
        # dice que no hay movimiento, el maximo ya no puede imponerlo.
        score = blend
        if a_score > blend and r_score > -0.3:
            score = max(blend, a_score * 0.85)
    elif a_act:
        score, blend = a_score, a_score
    else:
        score, blend = r_score, r_score

    conf = 0.55 * a_conf + 0.45 * r_conf if (a_act and r_act) else max(a_conf, r_conf)
    # direction_hint agregado de las amplitúdicas (p.ej. lado delgado del libro)
    hint = Direction.NEUTRAL
    hs = [s.metadata.get("hint") for s in a_act if s.metadata.get("hint")]
    if hs:
        up = hs.count("LONG")
        dn = hs.count("SHORT")
        hint = Direction.LONG if up > dn else (Direction.SHORT if dn > up else Direction.NEUTRAL)
    out = EngineOutput(score, hint, conf, a_act + r_act)
    out.metadata = {"anticip_amp": round(a_score, 3),  # type: ignore[attr-defined]
                    "react_amp": round(r_score, 3), "hint": hint.name}
    return out
