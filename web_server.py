"""
Web Server v13.2 - Broadcast ligero de precio + estado completo (cacheado)

FIX CRÍTICO v13.2 (precio congelado): este módulo se importa con
`from web_server import ...` desde engine_main.py, que corre en el EVENT LOOP
PRINCIPAL del bot. Pero `start_web_server()` arrancaba uvicorn en un HILO con su
PROPIO event loop -> las tareas de broadcast creadas desde el loop del bot
(`loop.create_task(broadcast_price_tick())`) vivían en un loop DISTINTO al de los
clientes WebSocket. El resultado: el navegador se conectaba y recibía UN mensaje
inicial (por eso el precio nacía en 0.0/blanco), y NUNCA llegaban más ticks ni
más estados -> precio clavado y barras de dirección/amplitud moviéndose a saltos.
Ahora uvicorn corre en el MISMO loop que el bot (ver start_web_server / launcher).

FIX CRÍTICO v13.2 (números grises): `import json` faltaba a nivel de módulo y
`_load_last_good_snapshot()` lo usaba -> NameError silencioso capturado por su
propio except; además la píldora "lag" pasaba a ámbar/rojo cuando el feed se
quedaba mudo y el CSS marcaba bloques obsoletos. Ver dashboard.html v13.2.
"""
import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Set, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

app = FastAPI()
BASE_DIR = Path(__file__).parent
HTML_FILE = BASE_DIR / "templates" / "dashboard.html"


class AccountInfo:
    def __init__(self):
        self.balance_usdt = 0.0
        self.position_side = "None"
        self.position_size = 0.0
        self.entry_price = 0.0
        self.unrealized_pnl = 0.0
        self.unrealized_pnl_pct = 0.0
        self.session_trades = 0
        self.session_wins = 0
        self.session_losses = 0
        self.session_wr = 0.0
        self.session_pnl = 0.0
        self.total_fees = 0.0
        self.consecutive_losses = 0


class BotState:
    def __init__(self):
        # FIX v14.3: el bot ARRANCA SIEMPRE en modo MANUAL. En MANUAL el motor
        # de decisiones sigue calculando y publicando señales/scores (el
        # dashboard vive), pero _trade_logic jamas abre ni cierra posiciones
        # automaticamente: solo ejecuta las ordenes que el operador lance desde
        # el panel (LONG/SHORT/CERRAR). Para operar en automatico hay que
        # pulsar explicitamente "AUTO" en el dashboard.
        self.mode = "MANUAL"
        self.position = "None"
        self.last_price = 0.0
        self.price_change = 0.0
        self.price_update_time = 0.0
        self.directional_score = 0.0
        self.amplitude_score = 0.0
        self.directional_signals = {}
        self.amplitude_signals = {}
        self.decision = "HOLD"
        self.expected_move = 0.0
        self.account = AccountInfo()
        self.connected_clients: Set[WebSocket] = set()
        self.recent_decisions: List[dict] = []
        self.recent_trades: List[dict] = []
        self.bot_state = "IDLE"
        self.last_signal = ""
        self.risk_state: Dict = {}
        # FIX v13.3: telemetría de frescura del feed (para el dashboard)
        self.signal_age_sec = 0.0
        self.loop_count = 0
        self.ws_connected = False
        self._last_price_broadcast = 0.0
        # FIX v13.2: 0.1s (10Hz) era tan agresivo que, con el bug del loop doble,
        # se creaban miles de tareas huérfanas. 0.5s es suficiente para ver el
        # precio moverse fluido y mantiene la carga baja.
        self._price_broadcast_interval = 0.5
        # --- cache de estado para broadcasts ligeros ---
        self._state_cache: Dict = {}
        self._state_dirty = True
        self._trades_version = 0
        self.position_opened_ts: float = 0.0
        # FIX v13.1 (precio congelado): copia del último snapshot que SÍ tenía
        # precio válido. Si el feed WS muere, _build_status() sirve esto en vez
        # de un last_price=0 -> el navegador nunca ve "congelarse" el número.
        self._last_good_snapshot: Dict = {}
        # FIX v13.2: referencia a la tarea del servidor uvicorn (mismo loop) y a
        # las tareas de broadcast pendientes, para poder cancelarlas al apagar.
        self._server_task = None
        self._price_task = None
        self._state_task = None
        # FIX v13.5: flag explícito de feed mudo (>10s sin mensajes WS). El
        # dashboard lo pinta en la píldora de estado; antes "ws_connected" podía
        # ser True con el feed muerto y no había ninguna señal visible.
        self.feed_muted = False


bot_state = BotState()


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if not HTML_FILE.exists():
        return HTMLResponse(
            content="<h1 style='color:red;'>Error: No se encuentra templates/dashboard.html</h1>",
            status_code=404
        )
    return HTMLResponse(content=HTML_FILE.read_text(encoding="utf-8"))


def _load_last_good_snapshot() -> Dict:
    """Lee state/dashboard_snapshot.json (escrito por el bot con fsync)."""
    try:
        p = Path(__file__).parent / "state" / "dashboard_snapshot.json"
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("last_price", 0) > 0:
                return data
    except Exception:
        pass
    return {}


def _build_status() -> Dict:
    return {
        "type": "state",
        "mode": bot_state.mode,
        "position": bot_state.position,
        "last_price": bot_state.last_price,
        "price_change": bot_state.price_change,
        "price_update_time": bot_state.price_update_time,
        "directional_score": bot_state.directional_score,
        "amplitude_score": bot_state.amplitude_score,
        "decision": bot_state.decision,
        "expected_move": bot_state.expected_move,
        "directional_signals": bot_state.directional_signals,
        "amplitude_signals": bot_state.amplitude_signals,
        "recent_decisions": bot_state.recent_decisions[-20:],
        "recent_trades": bot_state.recent_trades[-20:],
        "trades_version": bot_state._trades_version,
        "position_opened_ts": bot_state.position_opened_ts or 0.0,
        "bot_state": bot_state.bot_state,
        "last_signal": bot_state.last_signal,
        "risk_state": bot_state.risk_state,
        "signal_age_sec": round(bot_state.signal_age_sec, 2),
        "loop_count": bot_state.loop_count,
        "ws_connected": bot_state.ws_connected,
        "feed_muted": bool(getattr(bot_state, "feed_muted", False)),
        "server_ts": time.time(),
        "account": {
            "balance_usdt": bot_state.account.balance_usdt,
            "position_side": bot_state.account.position_side,
            "position_size": bot_state.account.position_size,
            "entry_price": bot_state.account.entry_price,
            "unrealized_pnl": bot_state.account.unrealized_pnl,
            "unrealized_pnl_pct": bot_state.account.unrealized_pnl_pct,
            "win_rate": bot_state.account.session_wr,
            "total_fees": bot_state.account.total_fees,
            "total_trades": bot_state.account.session_trades,
            "session_pnl": bot_state.account.session_pnl,
            "session_wins": bot_state.account.session_wins,
            "session_losses": bot_state.account.session_losses,
            "consecutive_losses": bot_state.account.consecutive_losses,
        }
    }


def _apply_price_fallback(status: Dict) -> Dict:
    """FIX v13.1: si el estado vivo no tiene precio (feed caído/arranque lento),
    se sirve el último snapshot bueno conocido — así el precio del dashboard nunca
    aparece clavado en 0.0 ni 'congelado' sin datos."""
    if status.get("last_price", 0) and status["last_price"] > 0:
        snap = dict(status)
        snap.pop("type", None)
        bot_state._last_good_snapshot = snap
        return status
    good = bot_state._last_good_snapshot or _load_last_good_snapshot()
    if not good:
        return status
    bot_state._last_good_snapshot = good
    merged = dict(good)
    merged.update({k: v for k, v in status.items()
                   if k not in ("last_price", "price_change", "price_update_time")})
    merged["last_price"] = good.get("last_price", 0.0)
    merged["price_update_time"] = good.get("price_update_time", 0.0)
    merged["stale_snapshot"] = True
    return merged


def get_status() -> Dict:
    """Estado completo CACHEADO: se reconstruye solo si hay cambios (dirty flag).

    Así el broadcast serializa UNA vez por actualización, no una por cliente.
    """
    if bot_state._state_dirty or not bot_state._state_cache:
        bot_state._state_cache = _apply_price_fallback(_build_status())
        bot_state._state_dirty = False
    return bot_state._state_cache


def get_price_tick():
    """Mensaje LIGERO solo con precio (broadcast rápido)"""
    return {
        "type": "price",
        "last_price": bot_state.last_price,
        "price_change": bot_state.price_change,
        "price_update_time": bot_state.price_update_time,
        "unrealized_pnl": bot_state.account.unrealized_pnl,
    }


@app.get("/api/status")
async def api_status():
    return get_status()


class ModeRequest(BaseModel):
    mode: str


class TradeRequest(BaseModel):
    action: str


@app.post("/api/mode")
async def set_mode(req: ModeRequest):
    if req.mode in ["AUTO", "MANUAL"]:
        bot_state.mode = req.mode
        await broadcast_state()
        return {"status": "ok", "mode": bot_state.mode}
    return {"status": "error", "message": "Modo inválido"}


@app.post("/api/trade")
async def manual_trade(req: TradeRequest):
    if bot_state.mode != "MANUAL":
        return {"status": "error", "message": "Cambia a modo MANUAL primero"}
    if req.action in ["LONG", "SHORT", "CLOSE"]:
        # FIX v14.4 (mando de golpe): el frontend YA no muestra confirmación
        # emergente; aquí se fuerza la acción de INMEDIATO: si hay una orden
        # en vuelo (ENTERING/CLOSING) se espera hasta 3s a que termine para
        # no pisarla, y se invalida el cache de estado para que el panel
        # refleje el mando en <100ms sin esperar al siguiente tick del bot.
        # FIX v14.5: antes se esperaba a que bot_state.bot_state saliera de
        # ENTERING/CLOSING, pero esos estados viven en BotRunner.self.bot_state y
        # solo se propagaban al final del tick -> el bucle era decorativo y un
        # mando podia pisar una orden en vuelo. Ahora se consulta al runner real.
        runner = getattr(bot_state, "_runner", None)
        for _ in range(30):
            busy = bool(getattr(runner, "_processing", False)) \
                or getattr(runner, "bot_state", "") in ("ENTERING", "CLOSING")
            if not busy:
                break
            await asyncio.sleep(0.1)
        bot_state.position = req.action
        # FIX v14.4b: si el mando es LONG/SHORT pero YA existe posición real
        # abierta (p. ej. huérfana tras reinicio), sincronizar con el lado
        # real: sin esto, el branch MANUAL nunca cerraría (wanted != CLOSE y
        # pos != None) y el operador vería su botón ignorado.
        if req.action in ("LONG", "SHORT") and bot_state.account.position_side not in ("None", "", None):
            real = "LONG" if bot_state.account.position_side == "Buy" else "SHORT"
            bot_state.position = real
            await broadcast_state()
            return {"status": "ok", "position": real,
                    "note": "ya habia posicion abierta; usa CERRAR"}
        bot_state._state_dirty = True
        await broadcast_state()
        return {"status": "ok", "position": bot_state.position}
    return {"status": "error", "message": "Acción inválida"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    bot_state.connected_clients.add(websocket)
    try:
        # FIX v13.1 (precio congelado en 0.0 al arrancar): si el navegador se
        # conecta ANTES de que el bot reciba su primer tick, no le servimos un
        # estado con last_price=0 — esperamos hasta 6s a que llegue un precio
        # real (el feed WS suele tardar ~1-2s). Así el precio grande nace ya
        # pintado en blanco/verde y nunca "se congela" arrancando en gris.
        deadline = time.time() + 6.0
        while bot_state.last_price <= 0 and time.time() < deadline:
            await asyncio.sleep(0.25)
        await websocket.send_json(get_status())
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        bot_state.connected_clients.discard(websocket)
    except Exception:
        bot_state.connected_clients.discard(websocket)


async def broadcast_state():
    await _send_to_all(get_status())


async def broadcast_price_tick():
    """Broadcast LIGERO de precio.

    FIX v13.1: el throttle se aplica ANTES de lanzar la tarea (en
    _broadcast_price_soon), así que aquí ya no debe comprobarlo: el timestamp se
    marca al programar, no al ejecutar. Antes esta doble comprobación hacía que
    casi ningún tick llegara a emitirse -> precio "congelado" en el navegador.
    """
    await _send_to_all(get_price_tick())


async def _send_to_all(message: dict):
    """FIX: envíos en paralelo con timeout por cliente.

    Antes era un bucle `for client: await send_json(...)` secuencial: UN cliente
    web lento/congelado (pestaña en background, móvil bloqueado) bloqueaba el
    broadcast de TODOS y, como el loop del bot esperaba el broadcast, el bot
    entero se congelaba "un rato". Ahora cada envío tiene timeout propio y los
    lentos se descartan sin frenar al bot.
    """
    clients = list(bot_state.connected_clients)
    if not clients:
        return
    disconnected = set()

    async def _send(client):
        try:
            await asyncio.wait_for(client.send_json(message), timeout=1.0)
        except Exception:
            disconnected.add(client)

    await asyncio.gather(*(_send(c) for c in clients), return_exceptions=True)
    if disconnected:
        bot_state.connected_clients -= disconnected


def _prune_tasks(store: str):
    """Mantén solo tareas vivas para no acumular referencias."""
    t = getattr(bot_state, store, None)
    if t is not None and t.done():
        setattr(bot_state, store, None)


async def update_bot_state(last_price, directional_score, amplitude_score,
                           directional_signals, amplitude_signals, decision,
                           expected_move, position, account_info,
                           recent_decisions=None, recent_trades=None,
                           bot_state_text="IDLE", last_signal="",
                           risk_state=None, price_change=0.0,
                           price_update_time=0.0):
    bot_state.last_price = last_price
    bot_state.price_change = price_change
    bot_state.price_update_time = price_update_time
    bot_state.directional_score = directional_score
    bot_state.amplitude_score = amplitude_score
    bot_state.directional_signals = directional_signals
    bot_state.amplitude_signals = amplitude_signals
    bot_state.decision = decision
    bot_state.expected_move = expected_move
    bot_state.position = position
    bot_state.bot_state = bot_state_text
    bot_state.last_signal = last_signal
    if risk_state is not None:
        bot_state.risk_state = risk_state

    bot_state.account.balance_usdt = account_info.balance_usdt
    bot_state.account.position_side = account_info.position_side
    bot_state.account.position_size = account_info.position_size
    bot_state.account.entry_price = account_info.entry_price
    bot_state.account.unrealized_pnl = account_info.unrealized_pnl
    bot_state.account.unrealized_pnl_pct = account_info.unrealized_pnl_pct
    bot_state.account.session_wr = account_info.session_wr
    bot_state.account.total_fees = account_info.total_fees
    bot_state.account.session_trades = account_info.session_trades
    bot_state.account.session_wins = account_info.session_wins
    bot_state.account.session_losses = account_info.session_losses
    bot_state.account.consecutive_losses = getattr(account_info, 'consecutive_losses', 0)
    bot_state.account.session_pnl = account_info.session_pnl

    if recent_decisions is not None:
        bot_state.recent_decisions = recent_decisions
    if recent_trades is not None:
        if recent_trades != bot_state.recent_trades:
            bot_state._trades_version += 1
        bot_state.recent_trades = recent_trades

    bot_state._state_dirty = True
    await broadcast_state()


async def update_price_only(price: float, price_change: float, upnl: float = 0.0):
    """🔧 NUEVO: Actualiza solo precio (llamado desde WebSocket del bot)"""
    bot_state.last_price = price
    bot_state.price_change = price_change
    bot_state.price_update_time = time.time()
    bot_state.account.unrealized_pnl = upnl
    await broadcast_price_tick()


async def _serve_uvicorn():
    """FIX v13.2 (RAÍZ DEL PRECIO CONGELADO): uvicorn se monta en el MISMO event
    loop del bot, no en un hilo con un loop aparte.

    Antes: `threading.Thread(target=uvicorn.run)` creaba un segundo event loop.
    Los clientes /ws vivían en ESE loop, pero las tareas de broadcast que lanza
    el bot (`loop.create_task(broadcast_price_tick())`) se creaban en el loop del
    BOT. asyncio NO puede enviar datos por un socket cuyo stream pertenece a otro
    loop: el navegador recibía solo el mensaje inicial y luego silencio eterno
    -> precio clavado, barras a saltos, "capa oscura" sobre los cuadros vacíos.
    Con un único loop todo fluye sin costuras.
    """
    config = uvicorn.Config(app, host="0.0.0.0", port=8000,
                            log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    print("🌐 Dashboard web: http://localhost:8000")
    await server.serve()


def start_web_server():
    """Arranca el dashboard en el loop actual.

    - Si se llama desde un coroutine (launcher/engine_main): devuelve la tarea ya
      creada para que el caller la añada a sus tasks.
    - Si no hay loop corriendo (arranque suelto de este módulo): usa threading con
      su propio loop, como antes (modo degradado, sin broadcasts cruzados).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        import threading

        def run():
            uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        print("🌐 Dashboard web (hilo separado): http://localhost:8000")
        return thread

    task = loop.create_task(_serve_uvicorn())
    bot_state._server_task = task
    return task


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)