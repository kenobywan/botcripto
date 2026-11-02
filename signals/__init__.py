"""
Signals package v12.8 - 16 señales clásicas + 9 anticipativas (3 grupos)
"""
from signals.directional import (
    calculate_obi,
    LargeTradeDetector, AbsorptionDetector,
    OIDeltaTracker,
    LongShortRatioTracker, FundingRateTracker, TakerBuySellRatio,
    CVDCalculator, VWAPDistance, CandleDelta, OrderFlowImbalance,
)
from signals.amplitude import (
    calculate_book_depth,
    OrderBookElasticity, ATRExpansion,
    RealizedVolatility, VolumeSpikeDetector, VolatilityRatio,
)
from signals.anticipation import (
    BookSlope, WallMomentum, WeightedOFI, MicroPriceDeviation,
    CVDDivergence, TradeArrivalRun,
    LiquidityThinning, SpreadCompression, VolumeAcceleration,
)

__all__ = [
    "calculate_obi",
    "LargeTradeDetector", "AbsorptionDetector",
    "OIDeltaTracker",
    "LongShortRatioTracker", "FundingRateTracker", "TakerBuySellRatio",
    "CVDCalculator", "VWAPDistance", "CandleDelta", "OrderFlowImbalance",
    "calculate_book_depth",
    "OrderBookElasticity", "ATRExpansion",
    "RealizedVolatility", "VolumeSpikeDetector", "VolatilityRatio",
    # v12.8 anticipación
    "BookSlope", "WallMomentum", "WeightedOFI", "MicroPriceDeviation",
    "CVDDivergence", "TradeArrivalRun",
    "LiquidityThinning", "SpreadCompression", "VolumeAcceleration",
]
