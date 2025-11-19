import os
from pathlib import Path

from dotenv import load_dotenv
from kalshi_python import Configuration, KalshiClient

# Load .env from repo root
load_dotenv()


def get_kalshi_client() -> KalshiClient:
    """
    Initialize an authenticated Kalshi client using env + local PEM file.
    """
    api_key_id = os.environ.get("KALSHI_API_KEY_ID")
    if not api_key_id:
        raise RuntimeError("KALSHI_API_KEY_ID not set in .env")

    key_path = Path("kalshi_private_key.pem")
    if not key_path.exists():
        raise RuntimeError(
            f"Private key file not found at {key_path.resolve()}")

    private_key_pem = key_path.read_text()

    config = Configuration(
        host="https://api.elections.kalshi.com/trade-api/v2",  # prod host
    )
    config.api_key_id = api_key_id
    config.private_key_pem = private_key_pem

    return KalshiClient(config)
