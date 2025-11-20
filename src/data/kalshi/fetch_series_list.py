from .client import get_kalshi_client


def main():
    client = get_kalshi_client()

    # This maps to GET /series
    # status is optional; you can use "active" but start with no filter
    series_response = client.get_series()  # or client.get_series(status="active")

    series = series_response.series
    print(f"Found {len(series)} total series\n")

    for s in series:
        ticker = s.ticker
        title = s.title
        print(f"{ticker}: {title}")

        if "NBA" in ticker.upper() or "NBA" in title.upper():
            print("  >>> POSSIBLE NBA SERIES <<<")


if __name__ == "__main__":
    main()
