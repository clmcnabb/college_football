import os
from datetime import datetime, time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent

load_dotenv()
API_KEY = os.getenv("CFDB_API_KEY")


def _convert_to_time(time_str) -> datetime.time:
    minutes, seconds = map(int, time_str.split(":"))
    return time(minute=minutes, second=seconds)


def _pull_data(url, params, headers) -> requests.Response:
    try:
        r = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=10,
        )
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("Oops: Something Else", err)

    if r:
        return r
    else:
        return None


def _dump_to_csv(df, save_path):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)


def pull_game_data(year, season_type):
    url = "https://api.collegefootballdata.com/games"
    params = {"year": year, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/games_{year}.csv"
    _dump_to_csv(df, save_path)


def pull_team_data(year, season_type, week):
    url = "https://api.collegefootballdata.com/games/teams"
    params = {"year": year, "week": week, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    processed_games = []

    for game in data:
        game_data = {"id": game["id"]}
        for team in game["teams"]:
            team_prefix = f"{team['homeAway']}_"
            game_data[f"{team_prefix}school"] = team["school"]
            game_data[f"{team_prefix}schoolId"] = team["schoolId"]
            for stat in team["stats"]:
                if stat["category"] == "completionAttempts":
                    completions, attempts = stat["stat"].split("-")
                    game_data[f"{team_prefix}completions"] = int(completions)
                    game_data[f"{team_prefix}attempts"] = int(attempts)
                elif stat["category"] == "totalPenaltiesYards":
                    penalties, yards = stat["stat"].split("-")
                    game_data[f"{team_prefix}penalty_yards"] = int(yards)
                    game_data[f"{team_prefix}penalties"] = int(penalties)
                elif stat["category"] == "possessionTime":
                    game_data[f"{team_prefix}{stat['category']}"] = _convert_to_time(
                        stat["stat"]
                    )
                else:
                    game_data[f"{team_prefix}{stat['category']}"] = stat["stat"]
        processed_games.append(game_data)

    df = pd.DataFrame(processed_games)

    save_path = f"{PROJECT_ROOT}/data/games_week{week}_{year}.csv"
    _dump_to_csv(df, save_path)


def pull_calendar_data(year):
    url = "https://api.collegefootballdata.com/calendar"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/calendar_{year}.csv"
    _dump_to_csv(df, save_path)


def pull_drive_data(year, season_type, week):
    url = "https://api.collegefootballdata.com/drives"
    params = {"year": year, "seasonType": season_type, "week": week}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/drives_week{week}_{year}.csv"
    _dump_to_csv(df, save_path)


def pull_team_season_stats(year):
    url = "https://api.collegefootballdata.com/stats/season"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/team_stats_{year}.csv"
    _dump_to_csv(df, save_path)


def pull_team_recruiting_rankings(year):
    url = "https://api.collegefootballdata.com/recruiting/teams"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/recruiting_{year}.csv"
    _dump_to_csv(df, save_path)


def pull_team_ranking_data(year, season_type):
    url = "https://api.collegefootballdata.com/rankings"
    params = {"year": year, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(
        data, record_path=["polls", "ranks"], meta=["season", "seasonType", "week"]
    )

    save_path = f"{PROJECT_ROOT}/data/rankings_{year}.csv"
    _dump_to_csv(df, save_path)


if __name__ == "__main__":
    # for year in range(2010, 2023):
    #     season_type = "regular"
    #     for week in range(1, 16):
    #         pull_team_data(year, season_type, week)
    #         pull_drive_data(year, season_type, week)

    #     pull_game_data(year, season_type)
    #     pull_calendar_data(year)
    #     pull_team_season_stats(year)
    year = 2023
    season_type = "regular"
    # for week in range(1, 16):
    #     pull_team_data(year, season_type, week)
    #     pull_drive_data(year, season_type, week)

    pull_game_data(year, season_type)
    pull_calendar_data(year)
    pull_team_season_stats(year)
    pull_team_recruiting_rankings(year)
    pull_team_ranking_data(year, season_type)
