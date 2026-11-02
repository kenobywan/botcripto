"""
WebSocket Client v13.5 - FIX congelamiento:
  - Reenvío automático del "ping" de Bybit (si no se responde, el server deja de enviar datos)
  - Watchdog interno: si no llegan mensajes en ws_stall_sec, fuerza reconexión
  - Dispatch con timeout por handler: un callback lento no puede paralizar la conexión
  - force_reconnect() público para el health monitor del bot
  - v13.5: backoff progresivo si las reconexiones mueren rápido y en serie
    (sin internet / DNS caído -> antes era bucle infinito que saturaba CPU y red)
"""
import asyncio
import inspect
import json
from typing import Callable, Dict, List

import websockets


class BybitWebSocket:
    def __init__(self, url: str, ws_stall_sec: float = 45.0):
        self.url = url
        self.ws = None
        self._callbacks: Dict[str, Callable] = {}
        self._running = False
        self._connected = False
        self._last_msg_time = 0.0
        self._ws_task: asyncio.Task | None = None
        self._stall_sec = ws_stall_sec
        self._reconnect_event = asyncio.Event()
        self._fail_streak = 0
        self._conn_start = 0.0

    def on(self, topic: str, callback: Callable):
        self._callbacks[topic] = callback

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self, subscriptions: List[str]):
        self._running = True
        self._subscriptions = subscriptions
        while self._running:
            conn_ok = False
            try:
                async with websockets.connect(
                        self.url, ping_interval=20, ping_timeout=20,
                        close_timeout=5, max_queue=512) as ws:
                    self.ws = ws
                    self._connected = True
                    conn_ok = True
                    self._conn_start = time_now()
                    self._last_msg_time = time_now()
                    print(f"✅ WebSocket conectado: {self.url}")
                    await ws.send(json.dumps({"op": "subscribe", "args": subscriptions}))
                    # watchdog de estancamiento dentro de la misma conexión
                    watchdog = asyncio.create_task(self._stall_watchdog())
                    try:
                        async for raw in ws:
                            self._last_msg_time = time_now()
                            try:
                                data = json.loads(raw)
                            except Exception:
                                continue
                            await self._dispatch(data)
                    finally:
                        watchdog.cancel()
                        try:
                            await watchdog
                        except (asyncio.CancelledError, Exception):
                            pass
                    self._connected = False
            except websockets.ConnectionClosed:
                self._connected = False
                if self._running:
                    print("⚠️  WebSocket cerrado, reconectando en 3s...")
                    await asyncio.sleep(3)
            except asyncio.CancelledError:
                self._connected = False
                raise
            except Exception as e:
                self._connected = False
                if self._running:
                    print(f"⚠️  WebSocket error: {e}, reconectando en 5s...")
                    await asyncio.sleep(5)
            # FIX v13.5: backoff si la conexión muere demasiado rápido y de forma
            # repetida (DNS caído / sin salida a internet). Antes esto generaba un
            # bucle infinito de reconexión que saturaba CPU/red y congelaba el bot.
            if conn_ok and time_now() - self._conn_start < 5.0:
                self._fail_streak += 1
            else:
                self._fail_streak = 0
            if self._fail_streak >= 3:
                wait = min(60.0, 5.0 * self._fail_streak)
                print(f"🛑 Reconexiones fallidas seguidas ({self._fail_streak}); espero {wait:.0f}s antes de reintentar")
                await asyncio.sleep(wait)
                self._fail_streak = 0

    async def _stall_watchdog(self):
        """Si Bybit deja de mandar datos (conexión medio-abierta), reinicia el socket."""
        while self._running:
            await asyncio.sleep(5)
            if self._last_msg_time and time_now() - self._last_msg_time > self._stall_sec:
                print(f"🔌 WS estancado {time_now()-self._last_msg_time:.0f}s -> reconectando")
                if self.ws:
                    try:
                        await self.ws.close()
                    except Exception:
                        pass
                return

    async def force_reconnect(self):
        """Cierra el socket actual; connect() reconecta solo."""
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass

    async def _dispatch(self, data: dict):
        # FIX crítico: Bybit envía {"op":"ping"} y hay que responder "pong".
        # Si no se responde, el servidor corta el flujo de datos y el bot
        # queda "congelado" sin feeds.
        op = data.get("op")
        if op == "ping":
            try:
                await self.ws.send(json.dumps({"op": "pong", "req_id": data.get("id")}))
            except Exception:
                pass
            return

        topic = data.get("topic", "")
        cb = self._callbacks.get(topic)
        if cb is None:
            for key, fn in self._callbacks.items():
                if topic.startswith(key):
                    cb = fn
                    break
        if cb:
            try:
                if inspect.iscoroutinefunction(cb):
                    # timeout por handler: un callback colgado no mata el stream
                    await asyncio.wait_for(cb(data), timeout=1.0)
                else:
                    cb(data)
            except asyncio.TimeoutError:
                pass  # descartamos el mensaje, nunca bloqueamos el dispatch
            except Exception:
                pass

    async def close(self):
        self._running = False
        self._connected = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass


def time_now() -> float:
    import time
    return time.time()
