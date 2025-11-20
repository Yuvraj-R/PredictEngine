# src/data/kalshi/utils.py

from datetime import datetime


def parse_nba_event_ticker(event_ticker: str):
    """
    Parse event_ticker like:
      KXNBAGAME-25NOV22NYKORL

    into:
      event_date = '2025-11-22'
      away_team = 'NYK'
      home_team = 'ORL'

    Format is: YYMMMDD + AWAY + HOME
      YY   = season year suffix (25 -> 2025)
      MMM  = month (NOV)
      DD   = day of month (22)
    """
    try:
        suffix = event_ticker.split("-", 1)[1]  # "25NOV22NYKORL"
        date_str = suffix[:7]                  # "25NOV22"
        teams_str = suffix[7:]                 # "NYKORL"

        year_suffix = int(date_str[:2])        # 25
        month_abbr = date_str[2:5]             # "NOV"
        day = int(date_str[5:])                # 22

        year = 2000 + year_suffix             # 25 -> 2025

        date_token = f"{day:02d}{month_abbr}{year}"  # "22NOV2025"
        date_dt = datetime.strptime(date_token.upper(), "%d%b%Y")
        event_date = date_dt.date().isoformat()      # "2025-11-22"

        away_team = teams_str[:3]
        home_team = teams_str[3:]

        return event_date, away_team, home_team
    except Exception:
        return None, None, None
