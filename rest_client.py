"""
REST Client v11.2 - Con WR direccional
"""
import asyncio

try:
    from pybit.unified_trading import HTTP
    PYBIT_AVAILABLE = True
except ImportError:
    PYBIT_AVAILABLE = False

from config import Config
from models import AccountInfo


class BybitRestClient:
    def __init__(self, config: Config):
        self.config = config
        self.account_info = AccountInfo()
        self.client = None
        self._rest_lock = asyncio.Lock()

        if not PYBIT_AVAILABLE:
            print("❌ Sin pybit: pip install pybit")
            return

        try:
            self.client = HTTP(
                testnet=False,
                demo=config.demo_mode,
                api_key=config.api_key.strip(),
                api_secret=config.api_secret.strip(),
                timeout=6.0, max_retries=1
            )
            print(f"✅ pybit {'DEMO' if config.demo_mode else 'LIVE'} TRADING")
        except Exception as e:
            print(f"❌ Error pybit: {e}")

    async def _call(self, fn_name: str, **kwargs):
        if not self.client:
            return {}
        # FIX: try/finally -> si el request peta o tarda, el lock SIEMPRE se libera.
        # (Antes un fallo raro podía dejar el lock tomado y bloquear todas las
        # operaciones REST posteriores = bot "congelado" sin poder operar.)
        try:
            await asyncio.wait_for(self._rest_lock.acquire(), timeout=12.0)
        except asyncio.TimeoutError:
            print(f"⚠️  REST lock ocupado, descarto {fn_name}")
            return {}
        try:
            fn = getattr(self.client, fn_name)
            return await asyncio.wait_for(
                asyncio.to_thread(fn, **kwargs),
                timeout=8.0
            )
        except asyncio.TimeoutError:
            print(f"⏱️  Timeout en {fn_name}")
            return {}
        except Exception as e:
            print(f"⚠️  {fn_name}: {e}")
            return {}
        finally:
            self._rest_lock.release()

    async def fetch_all(self):
        print("🔍 Consultando Bybit...")
        bal_task = self._call("get_wallet_balance", accountType="UNIFIED", coin="USDT")
        pos_task = self._call("get_positions", category="linear", symbol=self.config.symbol)
        bal_r, pos_r = await asyncio.gather(bal_task, pos_task, return_exceptions=True)

        if isinstance(bal_r, dict) and bal_r.get("retCode") == 0:
            lst = bal_r.get("result", {}).get("list", [])
            if lst:
                coin = next((c for c in lst[0].get("coin", []) if c.get("coin") == "USDT"), None)
                if coin:
                    self.account_info.balance_usdt = float(coin.get("walletBalance", 0))
                    print(f"💰 Balance: ${self.account_info.balance_usdt:,.2f}")

        if isinstance(pos_r, dict) and pos_r.get("retCode") == 0:
            lst = pos_r.get("result", {}).get("list", [])
            if lst:
                p = lst[0]
                size = float(p.get("size", 0) or 0)
                if size > 0:
                    self.account_info.position_side = p.get("side", "None")
                    self.account_info.position_size = size
                    self.account_info.entry_price = float(p.get("avgPrice", 0) or 0)
                    # FIX v14.4: updatedTime (ms) del exchange -> época seg.
                    try:
                        self.account_info.position_updated_ts = \
                            float(p.get("updatedTime", 0) or 0) / 1000.0
                    except (TypeError, ValueError):
                        self.account_info.position_updated_ts = 0.0
                    self.account_info.unrealized_pnl = float(p.get("unrealisedPnl", 0) or 0)
                    self.account_info.unrealized_pnl_pct = float(p.get("unrealisedPnlPct", 0) or 0) * 100
                    print(f"📈 Posición: {self.account_info.position_side} {size} BTC")
                else:
                    self.account_info.position_side = "None"
                    self.account_info.position_size = 0.0
        print("✅ Bybit OK")

    async def refresh_position(self) -> float:
        r = await self._call("get_positions", category="linear", symbol=self.config.symbol)
        if isinstance(r, dict) and r.get("retCode") == 0:
            lst = r.get("result", {}).get("list", [])
            if lst:
                size = float(lst[0].get("size", 0) or 0)
                self.account_info.position_side = lst[0].get("side", "None") if size > 0 else "None"
                self.account_info.position_size = size
                return float(lst[0].get("avgPrice", 0) or 0)
        return 0.0

    def record_trade_result(self, net_pnl: float, is_directional_win: bool = False):
        """🔧 WR direccional: WIN si acertó la dirección"""
        self.account_info.session_trades += 1
        self.account_info.session_pnl += net_pnl
        # FIX v14.1.4 F4 (log trades 03:22-03:36): el contador de fees del
        # account usaba una tasa HARDCODEADA (0.00055*2) y ademas leian
        # entry_price/position_size YA RESETADOS por _close() -> notional con
        # precio default 80000 y size viejo. Ahora: misma fuente unica
        # (config.taker_fee_rate) y el caller pasa el nocional real del ciclo.
        notional = getattr(self, "_last_closed_notional", 0.0)
        if notional <= 0:
            px = self.account_info.entry_price or 80000.0
            sz = self.account_info.position_size or self.config.base_order_size_btc
            notional = px * sz
        real_fee = notional * self.config.taker_fee_rate * 2
        self.account_info.total_fees += real_fee

        # 🔧 WIN basado en dirección acertada, no en neto
        if is_directional_win:
            self.account_info.session_wins += 1
            self.account_info.consecutive_losses = 0
        else:
            self.account_info.session_losses += 1
            self.account_info.consecutive_losses += 1

        if self.account_info.session_trades > 0:
            self.account_info.session_wr = (
                self.account_info.session_wins / self.account_info.session_trades * 100
            )
        win_tag = "✅" if is_directional_win else "❌"
        print(f"📊 WR Dir: {self.account_info.session_wins}/{self.account_info.session_trades} "
              f"({self.account_info.session_wr:.1f}%) {win_tag} | PnL: {self.account_info.session_pnl:+.4f}")

    async def place_order(self, side: str, qty: float, reduce_only: bool = False) -> bool:
        print(f"📤 Orden: {side} {qty} BTC (reduce={reduce_only})")
        r = await self._call(
            "place_order", category="linear", symbol=self.config.symbol,
            side=side, orderType="Market", qty=str(qty),
            reduceOnly=reduce_only
        )
        if isinstance(r, dict) and r.get("retCode") == 0:
            print(f"✅ Orden OK")
            return True
        msg = r.get('retMsg', 'sin respuesta') if isinstance(r, dict) else 'error'
        print(f"❌ Orden fallida: {msg}")
        return False

    async def open_long(self, qty: float) -> bool:
        return await self.place_order("Buy", qty, False)

    async def open_short(self, qty: float) -> bool:
        return await self.place_order("Sell", qty, False)

    async def close_position(self) -> bool:
        side = "Sell" if self.account_info.position_side == "Buy" else "Buy"
        return await self.place_order(side, self.account_info.position_size, True)