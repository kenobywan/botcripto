"""
RiskManager v11.0 - Gestión de riesgo completa
"""
import json
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from config import STATE_DIR, Config


class RiskManager:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.state_file = STATE_DIR / "risk_state.json"

        self.daily_pnl: float = 0.0
        self.daily_trades: int = 0
        self.last_reset_day: Optional[str] = None

        self.entry_time: Optional[float] = None
        self.entry_price: Optional[float] = None
        self.entry_side: Optional[str] = None
        self.entry_size_btc: float = 0.0

        self.trailing_sl: Optional[float] = None
        self.breakeven_sl: Optional[float] = None
        self.trailing_active: bool = False
        self.breakeven_active: bool = False

        self.volatility_window = deque(maxlen=600)

        self._load_state()

    def _load_state(self):
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                    self.daily_pnl = data.get("daily_pnl", 0.0)
                    self.daily_trades = data.get("daily_trades", 0)
                    self.last_reset_day = data.get("last_reset_day")
        except Exception as e:
            print(f"⚠️  Risk state load: {e}")

    def save_state(self):
        try:
            data = {
                "daily_pnl": self.daily_pnl,
                "daily_trades": self.daily_trades,
                "last_reset_day": self.last_reset_day,
            }
            with open(self.state_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            print(f"⚠️  Risk state save: {e}")

    def _maybe_reset_daily(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.last_reset_day != today:
            print(f"🔄 Reset diario: PnL anterior {self.daily_pnl:+.2f}, "
                  f"trades {self.daily_trades}")
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.last_reset_day = today

    def can_open_trade(self, balance_usdt: float, consecutive_losses: int
                       ) -> Tuple[bool, str, float]:
        self._maybe_reset_daily()

        if self.daily_pnl <= -self.cfg.max_daily_loss_usdt:
            return False, f"DAILY_LOSS_LIMIT ({self.daily_pnl:+.2f})", 0.0

        if self.daily_trades >= self.cfg.max_trades_per_day:
            return False, f"MAX_TRADES_DAY ({self.daily_trades})", 0.0

        if consecutive_losses >= self.cfg.max_consecutive_losses:
            return False, f"CONSECUTIVE_LOSSES ({consecutive_losses})", 0.0

        size = self._calculate_position_size(balance_usdt)
        if size < self.cfg.min_position_size_btc:
            return False, f"SIZE_TOO_SMALL ({size:.4f})", 0.0

        return True, "", size

    def _calculate_position_size(self, balance_usdt: float) -> float:
        if not self.cfg.use_dynamic_sizing:
            return self.cfg.base_order_size_btc

        risk_usdt = balance_usdt * self.cfg.risk_per_trade_pct
        size_btc = risk_usdt / max(self.cfg.stop_loss_usd, 1.0)
        size_btc = max(self.cfg.min_position_size_btc,
                       min(self.cfg.max_position_size_btc, size_btc))
        return round(size_btc, 4)

    def add_price(self, ts: float, price: float):
        self.volatility_window.append((ts, price))

    def current_volatility(self) -> float:
        if len(self.volatility_window) < 30:
            return 0.0
        prices = [p for _, p in list(self.volatility_window)[-30:]]
        returns = [(prices[i] - prices[i-1]) / prices[i-1] * 100
                   for i in range(1, len(prices))]
        if not returns:
            return 0.0
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return var ** 0.5

    def is_market_safe(self) -> Tuple[bool, float]:
        if not self.cfg.use_volatility_filter:
            return True, 0.0
        vol = self.current_volatility()
        return vol < self.cfg.max_volatility_pct, vol

    def update_trailing(self, current_price: float) -> Optional[float]:
        if not self.cfg.use_trailing_stop or not self.trailing_active:
            return None

        if self.entry_side == "Buy":
            new_sl = current_price * (1 - self.cfg.trailing_distance_pct / 100)
            if self.trailing_sl is None or new_sl > self.trailing_sl:
                self.trailing_sl = new_sl
        else:
            new_sl = current_price * (1 + self.cfg.trailing_distance_pct / 100)
            if self.trailing_sl is None or new_sl < self.trailing_sl:
                self.trailing_sl = new_sl
        return self.trailing_sl

    def maybe_activate_trailing(self, unrealized_usd: float) -> bool:
        if not self.cfg.use_trailing_stop:
            return False
        if not self.trailing_active and unrealized_usd >= self.cfg.trailing_activation_usd:
            self.trailing_active = True
            print(f"📈 Trailing activado @ PnL {unrealized_usd:+.2f}")
            return True
        return False

    def maybe_activate_breakeven(self, unrealized_usd: float) -> bool:
        if not self.cfg.use_breakeven:
            return False
        if not self.breakeven_active and unrealized_usd >= self.cfg.breakeven_trigger_usd:
            self.breakeven_active = True
            self.breakeven_sl = self.entry_price
            print(f"🔒 Breakeven activado @ {self.breakeven_sl:,.2f}")
            return True
        return False

    def effective_stop_loss(self, current_price: float) -> Optional[float]:
        candidates = []

        if self.entry_price and self.entry_side:
            sign = 1 if self.entry_side == "Buy" else -1
            fixed_sl = self.entry_price - sign * self.cfg.stop_loss_usd
            candidates.append(fixed_sl)

        if self.trailing_sl is not None:
            candidates.append(self.trailing_sl)

        if self.breakeven_sl is not None:
            candidates.append(self.breakeven_sl)

        if not candidates:
            return None

        if self.entry_side == "Buy":
            return max(candidates)
        else:
            return min(candidates)

    def check_stop_hit(self, current_price: float) -> Tuple[bool, str]:
        sl = self.effective_stop_loss(current_price)
        if sl is None:
            return False, ""

        if self.entry_side == "Buy" and current_price <= sl:
            reason = "TRAILING" if self.trailing_active else ("BREAKEVEN" if self.breakeven_active else "STOP")
            return True, reason
        if self.entry_side == "Sell" and current_price >= sl:
            reason = "TRAILING" if self.trailing_active else ("BREAKEVEN" if self.breakeven_active else "STOP")
            return True, reason
        return False, ""

    def register_open(self, side: str, price: float, size_btc: float):
        self.entry_time = time.time()
        self.entry_price = price
        self.entry_side = side
        self.entry_size_btc = size_btc
        self.trailing_sl = None
        self.breakeven_sl = None
        self.trailing_active = False
        self.breakeven_active = False

    def register_close(self, net_pnl: float):
        self.daily_pnl += net_pnl
        self.daily_trades += 1
        self.entry_time = None
        self.entry_price = None
        self.entry_side = None
        self.entry_size_btc = 0.0
        self.trailing_sl = None
        self.breakeven_sl = None
        self.trailing_active = False
        self.breakeven_active = False
        self.save_state()

    def clear(self):
        self.entry_time = None
        self.entry_price = None
        self.entry_side = None
        self.entry_size_btc = 0.0
        self.trailing_sl = None
        self.breakeven_sl = None
        self.trailing_active = False
        self.breakeven_active = False