import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from pull_data.pull_data import (
    _convert_to_time,
    _pull_data,
    _dump_to_csv,
    _dump_to_google_cloud,
    pull_yearly_game_data,
    pull_team_game_stats,
    pull_calendar_data,
    pull_weekly_drive_data,
    pull_weekly_play_data,
    pull_team_season_stats,
    pull_team_recruiting_score,
    pull_team_ranking_data,
    pull_team_talent_rankings,
    pull_yearly_data,
    pull_weekly_data,
)

# Test _convert_to_time function
def test_convert_to_time():
    assert _convert_to_time("30:45") == pd.Timestamp("00:30:45").time()
    assert _convert_to_time("01:15") == pd.Timestamp("00:01:15").time()

# Test _pull_data function
@patch('pull_data.requests.get')
def test_pull_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = _pull_data("http://test.com", {}, {})
    assert result == mock_response

@patch('pull_data.requests.get')
def test_pull_data_failure(mock_get):
    mock_get.side_effect = Exception("Test error")
    
    result = _pull_data("http://test.com", {}, {})
    assert result is None

# Test _dump_to_csv function
@patch('pull_data.pd.DataFrame.to_csv')
def test_dump_to_csv(mock_to_csv):
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    _dump_to_csv(df, "test.csv")
    mock_to_csv.assert_called_once_with("test.csv", index=False)

# Test _dump_to_google_cloud function
@patch('pull_data.storage.Client.from_service_account_json')
def test_dump_to_google_cloud(mock_client):
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    mock_bucket = MagicMock()
    mock_client.return_value.bucket.return_value = mock_bucket
    
    _dump_to_google_cloud(df, "test/path.csv")
    mock_bucket.blob.assert_called_once_with("test/path.csv")
    mock_bucket.blob.return_value.upload_from_string.assert_called_once()

# Test pull_yearly_game_data function
@patch('pull_data._pull_data')
@patch('pull_data._dump_to_csv')
@patch('pull_data._dump_to_google_cloud')
def test_pull_yearly_game_data(mock_cloud, mock_csv, mock_pull):
    mock_response = MagicMock()
    mock_response.json.return_value = [{'id': 1, 'home_team': 'Team A', 'away_team': 'Team B'}]
    mock_pull.return_value = mock_response
    
    pull_yearly_game_data(2022, "regular")
    mock_pull.assert_called_once()
    mock_csv.assert_called_once()
    mock_cloud.assert_called_once()

# Similar tests can be written for other pull_* functions

# Test pull_yearly_data function
@patch('pull_data.pull_yearly_game_data')
@patch('pull_data.pull_calendar_data')
@patch('pull_data.pull_team_season_stats')
@patch('pull_data.pull_team_recruiting_score')
@patch('pull_data.pull_team_ranking_data')
@patch('pull_data.pull_team_talent_rankings')
def test_pull_yearly_data(mock_talent, mock_ranking, mock_recruiting, mock_stats, mock_calendar, mock_game):
    pull_yearly_data(2022, "regular")
    mock_game.assert_called_once_with(2022, "regular")
    mock_calendar.assert_called_once_with(2022)
    mock_stats.assert_called_once_with(2022)
    mock_recruiting.assert_called_once_with(2022)
    mock_ranking.assert_called_once_with(2022, "regular")
    mock_talent.assert_called_once_with(2022)

# Test pull_weekly_data function
@patch('pull_data.pull_team_game_stats')
@patch('pull_data.pull_weekly_drive_data')
@patch('pull_data.pull_weekly_play_data')
def test_pull_weekly_data(mock_play, mock_drive, mock_game):
    pull_weekly_data(2022, "regular", 1)
    mock_game.assert_called_once_with(2022, "regular", 1)
    mock_drive.assert_called_once_with(2022, "regular", 1)
    mock_play.assert_called_once_with(2022, "regular", 1)