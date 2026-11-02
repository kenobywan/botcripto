"""
Models v11.0 - Modelos de datos compartidos
"""
import time
from dataclasses import dataclass, field
from enum import Enum


class Direction(Enum):
    LONG = 1
    SHORT = -1
    NEUTRAL = 0


@dataclass
class Signal:
    name: str
    value: float
    normalized: float
    direction: Direction
    confidence: float
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)


@dataclass
class EngineOutput:
    score: float
    direction: Direction
    confidence: float
    signals: list
    metadata: dict = field(default_factory=dict)


@dataclass
class TradeDecision:
    action: str
    direction_score: float
    amplitude_score: float
    expected_move_pct: float
    reason: str
    aligned_count: int = 0


@dataclass
class AccountInfo:
    balance_usdt: float = 0.0
    position_side: str = "None"
    position_size: float = 0.0
    entry_price: float = 0.0
    # FIX v14.4: timestamp (epoch seg) de la última actualización de la posición
    # según el exchange (updatedTime). Lo usa _reconcile para restaurar la
    # duración real de una posición huérfana tras reinicio del bot.
    position_updated_ts: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    session_trades: int = 0
    session_wins: int = 0
    session_losses: int = 0
    session_wr: float = 0.0
    session_pnl: float = 0.0
    total_fees: float = 0.0
    consecutive_losses: int = 0

    def to_dict(self) -> dict:
        return {
            "balance_usdt": self.balance_usdt,
            "position_side": self.position_side,
            "position_size": self.position_size,
            "entry_price": self.entry_price,
            "unrealized_pnl": self.unrealized_pnl,
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
            "win_rate": self.session_wr,
            "total_fees": self.total_fees,
            "total_trades": self.session_trades,
            "session_pnl": self.session_pnl,
            "session_wins": self.session_wins,
            "session_losses": self.session_losses,
            "consecutive_losses": self.consecutive_losses,
        }