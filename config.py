"""
Config v12.5
"""
import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
STATE_DIR = Path("state")
STATE_DIR.mkdir(exist_ok=True)


@dataclass
class Config:
    api_key: str = field(default_factory=lambda: os.getenv("BYBIT_KEY", ""))
    api_secret: str = field(default_factory=lambda: os.getenv("BYBIT_SECRET", ""))
    symbol: str = "BTCUSDT"
    demo_mode: bool = True

    @property
    def ws_public_url(self) -> str:
        return "wss://stream.bybit.com/v5/public/linear"

    @property
    def rest_url(self) -> str:
        return "https://api.bybit.com"

    # Trading
    base_order_size_btc: float = 0.005
    fee_per_side_usdt: float = 0.44

    # FIX v14.1 D3: UNICA fuente de comisiones. Antes estimated_fee usaba 0.0011
    # al abrir y el cierre contabilizaba 0.00055*2 -> el break-even del gate de
    # aperturas era ~2x mas caro que el PnL neto registrado (incoherente).
    taker_fee_rate: float = 0.00055      # tasa real por lado (taker)

    # FIX v14.1 C1: expected_move en USD REALES = volatilidad realizada x tamano.
    # Antes `amp*100` con amp medio 0.33 -> "exp=33$" ficcional: el gate
    # expected_move>=min_profitable_move estaba SIEMPRE vivo y la amplitud no
    # filtraba nada.
    amp_to_usd_multiplier: float = 1.0   # factor sobre vol-realizada-USD
    amp_expected_horizon_sec: float = 60.0
    # FIX v14.1.2 C1-ter (log 03:21-03:28): 0.015% x 77350 = 11.6 USD de suelo:
    # en mercado PLANO expected_move valia siempre 11.6$ > break-even (~0.9$) y
    # el gate volvía a estar SIEMPRE vivo (mitad residual del bug C1). El suelo
    # es ahora ruido mínimo de tick (0.0008%/min ~ 0.6$), por debajo del
    # break-even, que se aplica aparte como piso duro.
    amp_vol_floor_pct: float = 0.0008    # suelo de vol %/min (solo ruido de ticks)

    # FIX v14.1.1 C1-bis (log 03:21-03:28): con multiplier=1 el expected_move
    # era 1 sigma del horizonte -> p.ej. px20s=+19$ y exp=12$: el bot abria en
    # el PICO local (SHORT tras +19$ en 20s, LONG tras -25$). El movimiento
    # esperado debe ser un objetivo REALISTA hasta take_profit, no una banda de
    # ruido: ahora es un cuantil superior (percentil ~90 = 1.28 sigma) del
    # horizonte, calibrado al rango TP (30s). Con vol 60s~11$ -> exp(30s) =
    # 1.28*11*sqrt(0.5) = ~10$, coherente con objetivos TP realistas.
    amp_expected_quantile_z: float = 1.28
    amp_expected_target_horizon_sec: float = 30.0

    # FIX v14.1.3 D1 (log 03:21-03:28): min_profitable_move = fee*2 = ~0.9$ era
    # TAN barato que el gate de amplitud no filtraba NADA (cualquier vela lo
    # superaba). Se exige cubrir el coste ida+vuelta x un margen minimo de 3x:
    # abrir solo si el objetivo esperado >= ~1.4$ con tamano base.
    amp_profit_margin_mult: float = 3.0  # min_profitable_move = fee_ida_vuelta * esto

    # FIX v14.1.4 E1 (log trades 03:22-03:36, 5 cierres): la regla del log era
    # "el cierre debe cubrir sus fees" (move >= fee*3), PERO los 5 cierres
    # registrados fueron REVERSAL_ANT/HARD_STOP PERDEDORES (-$35, +$40 contra
    # SHORT, -$64...): exigir rentabilidad a un corte por deterioro de tesis es
    # imposible -> esa regla jamas se activaba y el unico cierre activo era la
    # loteria de REVERSAL_ANT. Ahora hay DOS clases de salida explicitas:
    #   - salidas por tesis/deterioro (REVERSAL_ANT/TRAIL/GIVEBACK/soft-SL):
    #     NO exigen lucro (cortar mal pero rapido), y
    #   - salidas por beneficio (TARGET/TRAILING/BE/REVERSAL clasico): SI.
    # Ademas, para que esas salidas por beneficio EXISTAN en la practica, el TP
    # debe ser realista: target_move_usd 250$ era inalcanzable (rango total de
    # las sesiones: +-40$) -> GIVEBACK/TRAILING/REVERSAL nunca podian disparar.
    target_move_usd: float = 12.0        # TP realista ~= expected_move p90/30s
    giveback_min_peak_usd: float = 7.0   # arma trailing solo si el pico >= esto
    giveback_give_ratio: float = 0.5     # ...y corta al devolver este ratio
    reversal_min_profit_mult: float = 3.0  # REVERSAL clasico: move >= fee*esto
    soft_stop_loss_usd: float = 25.0     # SL temprano ANTES de grace: corta
                                         # perdedores sin esperar 45s+loteria
                                         # (evita el HARD_STOP a -$64 del log)

    # Umbrales de entrada
    min_directional_score: float = 0.25
    min_amplitude_score: float = 0.30
    min_aligned_signals: int = 2
    min_signal_confidence: float = 0.40
    # FIX v14.7 (log 03:29-03:34): fuerza minima REAL de una senal para contar
    # en la confluencia (aligned / fresh_anticip). La confidence no basta:
    # wall_momentum/trade_arrival publican conf=max(conf,0.5) garantizado
    # aunque su normalized sea ~0.02 (ruido direccional). Sin este filtro el
    # log mostraba "SHORT ali=6 ant=3" con px20s subiendo +11.9$ -> aperturas
    # contra el movimiento real basadas en particulas casi neutras.
    dir_min_strength: float = 0.25

    # FIX v14.1 D1: RiskManager NUNCA se instanciaba (modulo muerto). Parametros:
    use_risk_manager: bool = True
    max_daily_loss_usdt: float = 80.0
    max_trades_per_day: int = 12
    max_consecutive_losses: int = 3
    use_dynamic_sizing: bool = False
    risk_per_trade_pct: float = 0.005
    min_position_size_btc: float = 0.001
    max_position_size_btc: float = 0.02
    use_volatility_filter: bool = False
    max_volatility_pct: float = 0.35
    use_trailing_stop: bool = False
    trailing_distance_pct: float = 0.06
    trailing_activation_usd: float = 80.0
    use_breakeven: bool = False
    breakeven_trigger_usd: float = 60.0

    # Gestion de posicion
    min_trade_duration_sec: float = 180.0
    max_trade_duration_sec: float = 900.0
    # FIX v14.2 (runner): permitir mantener un trade hasta 1 hora SIEMPRE que la
    # direccion se mantenga. El TIMEOUT duro sigue siendo el techo absoluto,
    # pero si al vencerlo la tesis sigue viva (dir_score a favor >= umbral,
    # precursores anticipativos frescos alineados, precio no degradado) la
    # posicion se EXTIENDE en bloques de extension_max_sec hasta
    # max_runner_duration_sec (3600s). Con dir en contra o ambiguedad, el
    # TIMEOUT cierra como antes.
    use_direction_runner: bool = True
    # FIX v14.5: "mas de 1 hora" exigia que el techo ABSOLUTO fuera >3600s.
    # Con hard_cap=3600 la extension se calculaba como
    # min(deadline+300, entry+3600): el ultimo bloque valia 0s y el runner
    # moria clavado en 1h exacta. Ahora techo de 2h: una tesis viva puede
    # sostenerse 1h05m...1h59m con bloques de 5min siempre utiles.
    max_runner_duration_sec: float = 7200.0   # techo absoluto del runner (2h)
    extension_block_sec: float = 300.0        # cada bloque de extension (5 min)
    runner_min_dir_score: float = 0.25        # |d_score| a favor minimo para seguir
    runner_min_anticip_fresh: int = 2         # precursores anticipativos frescos a favor
    runner_price_degrade_frac: float = 0.75   # veto si move < peak*esto (rally roto)
    # FIX v14.1.4 E1: se ELIMINA la redefinicion duplicada `target_move_usd=250`
    # (la dataclass guardaba el ULTIMO valor -> TP de $250 inalcanzable con un
    # rango de sesion de +-40$: TARGET/GIVEBACK/REVERSAL jamas podian disparar y
    # REVERSAL_ANT quedaba como unica salida, cortando en plena perdida).
    # El unico target vigente es el realista definido arriba (=12$).
    stop_loss_usd: float = 120.0
    cooldown_after_close_sec: float = 20.0

    # v13.3 Fresura de señales y salida por deterioro de tesis
    max_signal_age_sec: float = 4.0          # señal mas alla de esto NO cuenta como precursor
    # FIX v14.6 CRITICO (log 03:29-03:34): los precursores de libro se
    # publican con throttle de 1s dentro del loop (~2.5s/tick). Con corte de
    # frescura = max_signal_age_sec(4s), entre dos publicaciones su edad real
    # (2.5-3.5s) rozaba el corte y el conteo saltaba 3->1 en ~90% de los ticks
    # -> el bot NUNCA abria pese a dir=+-0.7 con 8 senales alineadas.
    # La ventana del gate de apertura/cierre/runner es ahora este valor
    # (cubre un ciclo completo de throttle sin dejar pasar microestructura
    # realmente vieja; sigue siendo <= TTL de las anticipativas = 6s).
    fresh_gate_window_sec: float = 6.0
    min_fresh_anticip_signals: int = 3       # v14.0: CONFLUENCIA de >=3 precursores
                                             # anticipativos frescos y alineados para ABRIR
                                             # (con 1 el bot era reactivo disfrazado)
    reversal_grace_sec: float = 45.0         # FIX v14.1 D2: 30->45s. El churn del
                                             # log (100% cierres REVERSAL_ANT, fees
                                             # 7x el bruto) venia de cortes con solo
                                             # 30s de vida vs aperturas que exigian
                                             # 3 precursores de vida <=4s.
    min_opposing_signals_to_flip: int = 3    # señales alineadas en contra para cierre inmediato

    # v13.6 Reglas derivadas del analisis de logs (23:50-04:10)
    strong_dir_override_score: float = 0.70  # |dir| >= esto ignora el gate de amplitud
    strong_dir_min_aligned: int = 5          # ...pero exige esta confluencia de senales
    ambiguity_min_both: float = 0.50         # ant y react opuestos ambos >= esto -> sin tesis
    ambiguity_leader_margin: float = 0.20    # v13.6.1: si |ant|-|react| > esto hay lider claro (no es ambiguo)
    min_reversal_anticip_fresh: int = 2      # precursores ANTICIPATIVOS puros en contra para cerrar
    reversal_min_dir_score: float = 0.45     # |d_score| en contra minimo para REVERSAL_ANT

    # v13.7 Reglas derivadas del analisis de logs (00:11-00:13)
    strong_dir_amp_floor: float = 0.15       # STRONG_DIR exige amp_effectiva >= esto
    # FIX v14.1 B4: exigir movimiento PASADO a favor era anti-anticipacion pura
    # (comprar techos / vender suelos; corr(dir_apertura, pnl) = -0.11 en el log).
    # Ahora el override solo exige que el precio NO se haya movido FUERTE EN
    # CONTRA en la ventana (0 = desactiva el requisito de "a favor").
    strong_dir_price_move_usd: float = 0.0   # movimiento minimo A FAVOR exigido
    strong_dir_max_adverse_usd: float = 12.0 # veto si el precio ya fue > esto en contra
    strong_dir_lookback_sec: float = 20.0    # ventana para medir ese movimiento
    max_hold_loss_usd: float = 60.0          # HARD_STOP: corta la posicion aunque el
                                             # gate de duracion minima no lo permita

    # Senales
    obi_levels: int = 20
    obi_lifetime_min_ms: int = 500
    large_trade_usd_threshold: float = 5_000.0
    # FIX v13.4: obsoleto (AbsorptionDetector usa ventana temporal de 60s)
    absorption_lookback_trades: int = 100
    oi_delta_window_sec: int = 300
    book_depth_levels: int = 50

    # Frecuencias
    # v13.1: antes 2.0s -> el snapshot de estado (scores/barras/señales) que se
    # envía al dashboard solo se recalculaba cada 2s y además el broadcast estaba
    # mal throttled (nunca salía del intervalo) -> ~4s entre actualizaciones.
    loop_interval_sec: float = 0.5
    rest_poll_sec: float = 30.0
    reconcile_interval_sec: float = 10.0
    state_save_interval_sec: float = 15.0

    def validate(self) -> list:
        errors = []
        if not self.api_key or not self.api_secret:
            errors.append("Faltan BYBIT_KEY o BYBIT_SECRET")
        return errors