from .client import get_kalshi_client


def main():
    client = get_kalshi_client()

    # Easiest sanity check: get balance
    balance = client.get_balance()
    print(f"Balance (cents): {balance.balance}")
    print(f"Balance (dollars): {balance.balance / 100:.2f}")

    # Optional: also sanity-check markets call
    markets = client.get_markets(limit=3)
    print("\nSample markets:")
    for m in markets.markets:
        print(f"- {m.ticker}: {m.title}")


if __name__ == "__main__":
    main()
