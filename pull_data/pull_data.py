import os
from datetime import datetime, time
from pathlib import Path
from google.cloud import storage

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent

load_dotenv()
API_KEY = os.getenv("CFDB_API_KEY")
KEY_FILE_PATH = os.getenv("PATH_TO_SERVICE_ACCOUNT_KEY")
LANDING_BUCKET = os.getenv("LANDING_BUCKET")



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

    try:
        r
    except NameError:
        print("No response received")
        return None
    else:
        return r


def _dump_to_csv(df, save_path):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)


def _dump_to_google_cloud(df, object_path):
    # Initialize a client with authentication
    storage_client = storage.Client.from_service_account_json(KEY_FILE_PATH)

    # Define your bucket name
    bucket_name = LANDING_BUCKET

    # Create a bucket object
    bucket = storage_client.bucket(bucket_name)

    # Convert DataFrame to CSV string
    csv_string = df.to_csv(index=False)

    # Create a new blob and upload the file's content
    blob = bucket.blob(object_path)
    blob.upload_from_string(csv_string, content_type="text/csv")

    print(f"File uploaded to {blob.public_url}")


def pull_yearly_game_data(year, season_type):
    url = "https://api.collegefootballdata.com/games"
    params = {"year": year, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve data for week {week}, {year}")
        return
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/games_{year}.csv"
    _dump_to_csv(df, save_path)
    _dump_to_google_cloud(df, f"games/games_{year}.csv")


def pull_team_data(year, season_type, week):
    url = "https://api.collegefootballdata.com/games/teams"
    params = {"year": year, "week": week, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve data for week {week}, {year}")
        return
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
                    values = stat["stat"].split("-")
                    if len(values) == 2:
                        penalties, yards = values
                        game_data[f"{team_prefix}penalty_yards"] = int(yards)
                        game_data[f"{team_prefix}penalties"] = int(penalties)
                    else:
                        print(
                            f"Unexpected value format for totalPenaltiesYards: {stat['stat']}"
                        )
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
    _dump_to_google_cloud(df, f"games/games_week{week}_{year}.csv")


def pull_calendar_data(year):
    url = "https://api.collegefootballdata.com/calendar"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve data for week {year}")
        return
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/calendar_{year}.csv"
    _dump_to_csv(df, save_path)
    _dump_to_google_cloud(df, f"calendar/calendar_{year}.csv")


def pull_drive_data(year, season_type, week):
    url = "https://api.collegefootballdata.com/drives"
    params = {"year": year, "seasonType": season_type, "week": week}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve data for week {week}, {year}")
        return
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/drives_week{week}_{year}.csv"
    _dump_to_csv(df, save_path)
    _dump_to_google_cloud(df, f"drives/drives_week{week}_{year}.csv")


def pull_play_data(year, season_type, week):
    url = "https://api.collegefootballdata.com/plays"
    params = {"year": year, "seasonType": season_type, "week": week}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve data for week {week}, {year}")
        return
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/plays_week{week}_{year}.csv"
    _dump_to_csv(df, save_path)
    _dump_to_google_cloud(df, f"plays/plays_week{week}_{year}.csv")


def pull_team_season_stats(year):
    url = "https://api.collegefootballdata.com/stats/season"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/team_stats_{year}.csv"
    _dump_to_csv(df, save_path)
    _dump_to_google_cloud(df, f"team_stats/team_stats_{year}.csv")


def pull_team_recruiting_rankings(year):
    url = "https://api.collegefootballdata.com/recruiting/teams"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}
    response = _pull_data(url, params, headers)
    data = response.json()
    df = pd.json_normalize(data)

    save_path = f"{PROJECT_ROOT}/data/recruiting_{year}.csv"
    _dump_to_csv(df, save_path)
    _dump_to_google_cloud(df, f"recruiting/recruiting_{year}.csv")


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
    _dump_to_google_cloud(df, f"rankings/rankings_{year}.csv")


if __name__ == "__main__":
    for year in range(2015, 2024):
        season_type = "regular"
        for week in range(1, 16):
            pull_team_data(year, season_type, week)
            pull_drive_data(year, season_type, week)
            pull_play_data(year, season_type, week)

        pull_yearly_game_data(year, season_type)
        pull_calendar_data(year)
        pull_team_season_stats(year)
        pull_team_recruiting_rankings(year)
        pull_team_ranking_data(year, season_type)
    # year = 2023
    # season_type = "regular"
    # for week in range(1, 16):
    #     pull_team_data(year, season_type, week)
    #     pull_drive_data(year, season_type, week)
    # # pull_play_data(year, season_type, 1)
    # pull_yearly_game_data(year, season_type)
    # pull_calendar_data(year)
    # pull_team_season_stats(year)
    # pull_team_recruiting_rankings(year)
    # pull_team_ranking_data(year, season_type)
