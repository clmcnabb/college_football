import os
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import storage

PROJECT_ROOT = Path(__file__).parent.parent

load_dotenv()
API_KEY = os.getenv("CFDB_API_KEY")
KEY_FILE_PATH = os.getenv("PATH_TO_SERVICE_ACCOUNT_KEY")
LANDING_BUCKET = os.getenv("LANDING_BUCKET")


def _convert_to_time(time_str: str) -> datetime.time:
    """
    Converts a string in the format 'mm:ss' to a datetime.time object.

    Args:
        time_str (str): The time string to be converted, in the format 'mm:ss'.

    Returns:
        datetime.time: The converted time.
    """
    minutes, seconds = map(int, time_str.split(":"))
    return time(minute=minutes, second=seconds)


def _pull_data(
    url: str,  # The URL to retrieve data from.
    params: Dict[str, Any],  # The URL parameters to send with the request.
    headers: Dict[str, str],  # The headers to send with the request.
) -> Optional[requests.Response]:
    """
    Attempts to retrieve data from the given URL with the given parameters and headers.

    Args:
        url (str): The URL to retrieve data from.
        params (dict): The URL parameters to send with the request.
        headers (dict): The headers to send with the request.

    Returns:
        requests.Response: The response object, if the request was successful. None otherwise.
    """
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        return r
    except requests.exceptions.RequestException as err:
        print(f"Error retrieving data from {url}: {err}")

    return None


def _dump_to_csv(
    df: pd.DataFrame,  # The DataFrame to be dumped.
    save_path: str,  # The path to the file to be written.
) -> None:
    """
    Dumps a pandas DataFrame as a CSV file to the given path.

    Args:
        df (pandas.DataFrame): The DataFrame to be dumped.
        save_path (str): The path to the file to be written.

    Returns:
        None

    Raises:
        Exception: If there is an error saving the file.
    """
    try:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_csv(save_path, index=False)
        print(f"Successfully saved CSV file: {save_path}")
    except Exception as e:
        print(f"Error saving CSV file: {save_path}")
        print(f"Error details: {str(e)}")


def _dump_to_google_cloud(
    df: pd.DataFrame,  # The DataFrame to be uploaded.
    object_path: str,  # The path to the object in the Google Cloud Storage bucket.
) -> None:
    """
    Uploads a pandas DataFrame as a CSV file to Google Cloud Storage.

    Args:
        df (pandas.DataFrame): The DataFrame to be uploaded.
        object_path (str): The path to the object in the Google Cloud Storage bucket.

    Returns:
        None
    """
    try:
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

        print(f"File successfully uploaded to {blob.public_url}")
    except Exception as e:
        print(f"Error uploading file to Google Cloud: {str(e)}")
        print(f"Failed to upload file: {object_path}")


def pull_yearly_game_data(
    year: int,  # The year of the season.
    season_type: str,  # The type of season (e.g. "regular", "postseason").
) -> None:
    """
    Retrieves yearly game data from the College Football Data API for a given year and season type.

    Args:
        year (int): The year of the season.
        season_type (str): The type of season (e.g. "regular", "postseason").

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/games"
    params = {"year": year, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)
    if response is None:
        print(
            f"Failed to retrieve yearly game data for year {year}, season type {season_type}"
        )
        return

    data = response.json()
    if not data:
        print(
            f"No yearly game data available for year {year}, season type {season_type}"
        )
        return

    try:
        df = pd.json_normalize(data)

        save_path = f"{PROJECT_ROOT}/data/games_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"games/games_{year}.csv")
        print(
            f"Successfully processed yearly game data for year {year}, season type {season_type}"
        )
    except Exception as e:
        print(
            f"Error processing yearly game data for year {year}, season type {season_type}: {str(e)}"
        )


def pull_team_game_stats(
    year: int,  # The year of the season.
    season_type: str,  # The type of season (e.g. "regular", "postseason").
    week: int,  # The week of the season.
) -> None:
    """
    Retrieves team game stats from the College Football Data API for a given year,
    season type, and week.

    Args:
        year (int): The year of the season.
        season_type (str): The type of season (e.g. "regular", "postseason").
        week (int): The week of the season.

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/games/teams"
    params = {"year": year, "week": week, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve team game stats for week {week}, year {year}")
        return

    data = response.json()
    if not data:
        print(f"No team game stats available for week {week}, year {year}")
        return

    try:
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
                        game_data[f"{team_prefix}{stat['category']}"] = (
                            _convert_to_time(stat["stat"])
                        )
                    else:
                        game_data[f"{team_prefix}{stat['category']}"] = stat["stat"]
            processed_games.append(game_data)

        df = pd.DataFrame(processed_games)

        save_path = f"{PROJECT_ROOT}/data/games_week{week}_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"games/games_week{week}_{year}.csv")
        print(f"Successfully processed team game stats for week {week}, year {year}")
    except Exception as e:
        print(
            f"Error processing team game stats for week {week}, year {year}: {str(e)}"
        )


def pull_calendar_data(year: int) -> None:
    """
    Retrieves calendar data from the College Football Data API for a given year and season type.

    Args:
        year (int): The year of the season.

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/calendar"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)  # type: requests.Response
    if response is None:
        print(f"Failed to retrieve calendar data for year {year}")
        return

    data = response.json()  # type: List[Dict[str, Any]]
    if not data:
        print(f"No calendar data available for year {year}")
        return

    try:
        df = pd.json_normalize(data)  # type: pd.DataFrame

        save_path = f"{PROJECT_ROOT}/data/calendar_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"calendar/calendar_{year}.csv")
        print(f"Successfully processed calendar data for year {year}")
    except Exception as e:
        print(f"Error processing calendar data for year {year}: {str(e)}")


def pull_weekly_drive_data(
    year: int,  # The year of the season.
    season_type: str,  # The type of season (e.g. "regular", "postseason").
    week: int,  # The week of the season.
) -> None:
    """
    Retrieves drive data from the College Football Data API for a given year, season type, and week.

    Args:
        year (int): The year of the season.
        season_type (str): The type of season (e.g. "regular", "postseason").
        week (int): The week of the season.

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/drives"
    params = {"year": year, "seasonType": season_type, "week": week}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve drive data for week {week}, year {year}")
        return

    data = response.json()
    if not data:
        print(f"No drive data available for week {week}, year {year}")
        return

    try:
        df = pd.json_normalize(data)

        save_path = f"{PROJECT_ROOT}/data/drives_week{week}_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"drives/drives_week{week}_{year}.csv")
        print(f"Successfully processed drive data for week {week}, year {year}")
    except Exception as e:
        print(f"Error processing drive data for week {week}, year {year}: {str(e)}")


def pull_weekly_play_data(
    year: int,  # The year of the season.
    season_type: str,  # The type of season (e.g. "regular", "postseason").
    week: int,  # The week of the season.
) -> None:
    """
    Retrieves play data from the College Football Data API for a given year, season type, and week.

    Args:
        year (int): The year of the season.
        season_type (str): The type of season (e.g. "regular", "postseason").
        week (int): The week of the season.

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/plays"
    params = {"year": year, "seasonType": season_type, "week": week}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve play data for week {week}, year {year}")
        return

    data = response.json()
    if not data:
        print(f"No play data available for week {week}, year {year}")
        return

    try:
        df = pd.json_normalize(data)

        save_path = f"{PROJECT_ROOT}/data/plays_week{week}_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"plays/plays_week{week}_{year}.csv")
        print(f"Successfully processed play data for week {week}, year {year}")
    except Exception as e:
        print(f"Error processing play data for week {week}, year {year}: {str(e)}")


def pull_team_season_stats(year: int) -> None:
    """
    Retrieves team season stats from the College Football Data API for a given year.

    Args:
        year (int): The year of the season.

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/stats/season"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve team season stats for year {year}")
        return

    data = response.json()
    if not data:
        print(f"No team season stats available for year {year}")
        return

    try:
        df = pd.json_normalize(data)

        save_path = f"{PROJECT_ROOT}/data/team_stats_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"team_stats/team_stats_{year}.csv")
        print(f"Successfully processed team season stats for year {year}")
    except Exception as e:
        print(f"Error processing team season stats for year {year}: {str(e)}")


def pull_team_recruiting_score(year: int) -> None:
    """
    Retrieves recruiting data from the College Football Data API for a given year.

    Args:
        year (int): The year of the season.

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/recruiting/teams"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)  # type: requests.Response
    if response is None:
        print(f"Failed to retrieve recruiting data for year {year}")
        return

    data = response.json()  # type: List[Dict[str, Any]]
    if not data:
        print(f"No recruiting data available for year {year}")
        return

    try:
        df = pd.json_normalize(data)  # type: pd.DataFrame

        save_path = f"{PROJECT_ROOT}/data/recruiting_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"recruiting/recruiting_{year}.csv")
        print(f"Successfully processed recruiting data for year {year}")
    except Exception as e:
        print(f"Error processing recruiting data for year {year}: {str(e)}")


def pull_team_ranking_data(year: int, season_type: str) -> None:
    """
    Retrieves team ranking data from the College Football Data API for a given year and season type.

    Args:
        year (int): The year of the season.
        season_type (str): The type of season (e.g. "regular", "postseason").

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/rankings"
    params = {"year": year, "seasonType": season_type}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)  # type: requests.Response
    if response is None:
        print(
            f"Failed to retrieve ranking data for year {year}, season type {season_type}"
        )
        return

    data = response.json()  # type: List[Dict[str, Any]]
    if not data:
        print(f"No ranking data available for year {year}, season type {season_type}")
        return

    try:
        df = pd.json_normalize(
            data, record_path=["polls", "ranks"], meta=["season", "seasonType", "week"]
        )  # type: pd.DataFrame

        save_path = f"{PROJECT_ROOT}/data/rankings_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"rankings/rankings_{year}.csv")
        print(
            f"Successfully processed ranking data for year {year}, season type {season_type}"
        )
    except Exception as e:
        print(
            f"Error processing ranking data for year {year}, season type {season_type}: {str(e)}"
        )


def pull_team_talent_rankings(year: int) -> None:
    """
    Retrieves team talent rankings from the College Football Data API for a given year.

    Args:
        year (int): The year of the season.

    Returns:
        None

    Raises:
        Exception: If there is an error processing the data.
    """
    url = "https://api.collegefootballdata.com/talent"
    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}

    response = _pull_data(url, params, headers)
    if response is None:
        print(f"Failed to retrieve talent data for year {year}")
        return

    data = response.json()
    if not data:
        print(f"No talent data available for year {year}")
        return

    try:
        df = pd.json_normalize(data)

        save_path = f"{PROJECT_ROOT}/data/talent_{year}.csv"
        _dump_to_csv(df, save_path)
        _dump_to_google_cloud(df, f"talent/talent_{year}.csv")
        print(f"Successfully processed talent data for year {year}")
    except Exception as e:
        print(f"Error processing talent data for year {year}: {str(e)}")


if __name__ == "__main__":
    for year in range(2015, 2025):
        season_type = "regular"
        for week in range(1, 16):
            pull_team_game_stats(year, season_type, week)
            pull_weekly_drive_data(year, season_type, week)
            pull_weekly_play_data(year, season_type, week)

        pull_yearly_game_data(year, season_type)
        pull_calendar_data(year)
        pull_team_season_stats(year)
        pull_team_recruiting_score(year)
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
