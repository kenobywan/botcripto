"""
Launcher v11.2 - Entry point del bot
"""
import argparse
import asyncio
import os
import signal
import sys

from dotenv import load_dotenv

from config import Config
from engine_main import EngineMain

load_dotenv()


def parse_args():
    p = argparse.ArgumentParser(description="BTC/USDT Bot v11.2")
    p.add_argument("--key", default=os.getenv("BYBIT_KEY"), help="Bybit API Key")
    p.add_argument("--secret", default=os.getenv("BYBIT_SECRET"), help="Bybit API Secret")
    p.add_argument("--symbol", default="BTCUSDT", help="Trading pair")
    p.add_argument("--live", action="store_true", help="Disable demo mode (LIVE trading)")
    return p.parse_args()


async def main():
    args = parse_args()
    cfg = Config(
        api_key=args.key or "",
        api_secret=args.secret or "",
        symbol=args.symbol,
        demo_mode=not args.live
    )

    errors = cfg.validate()
    if errors:
        print("❌ Errores de configuración:")
        for e in errors:
            print(f"   - {e}")
        sys.exit(1)

    engine = EngineMain(cfg)
    stop = asyncio.Event()

    def shutdown():
        print("\n⏹️  Apagando...")
        stop.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown)
    except NotImplementedError:
        pass

    try:
        await engine.run(stop)
    except KeyboardInterrupt:
        shutdown()
    except Exception as e:
        print(f"❌ Fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())