import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from equal_weighted_index_composition import (
    get_market_cap_data,
    get_daily_top_100,
    calculate_weights,
    track_composition_changes,
    calculate_index_performance,
)

# Sample test data
data = pd.DataFrame({
    "Date": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"]),
    "Ticker": ["AAPL", "MSFT", "GOOGL", "AMZN"],
    "MarketCap": [2500, 2200, 1800, 1500],
    "Price": [150, 300, 2800, 3300]
})


# Mock NO_OF_COMPANIES for the entire test module
@pytest.fixture(scope="module", autouse=True)
def mock_no_of_companies():
    with patch("equal_weighted_index_composition.NO_OF_COMPANIES", 4):
        yield


@pytest.fixture
def mock_duckdb():
    """Mock DuckDB connection and query execution."""
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchdf.return_value = data
        yield mock_connect


# Test get_market_cap_data
@pytest.mark.usefixtures("mock_duckdb")
def test_get_market_cap_data(mock_duckdb):
    df = get_market_cap_data()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == ["Date", "Ticker", "MarketCap", "Price"]


# Test get_daily_top_100
@pytest.mark.parametrize("top_n, expected", [(2, 2), (1, 1)])
def test_get_daily_top_100(top_n, expected):
    df = get_daily_top_100(data.head(top_n))
    assert len(df) == expected
    assert "MarketCap" in df.columns


# Test calculate_weights
def test_calculate_weights():
    df = calculate_weights(data)
    assert "Weight" in df.columns
    assert df["Weight"].sum() == 1


# Test track_composition_changes
def test_track_composition_changes():
    df = track_composition_changes(data)
    assert isinstance(df, pd.DataFrame)
    assert set(["Date", "Additions", "Removals"]).issubset(df.columns)
    assert 'MSFT' in df['Removed_Tickers'].iloc[0]


# Test calculate_index_performance
def test_calculate_index_performance():
    df = calculate_index_performance(data)
    assert "Daily_Return" in df.columns
    assert "Cumulative_Value" in df.columns
    assert df['Cumulative_Value'].iloc[1] == 0.7021276595744681
