import pytest
import pandas as pd
import duckdb
from datetime import datetime
from unittest.mock import patch
from data_fetcher import calculate_adjusted_shares, fetch_ticker_data, create_database_schema, insert_company_data, \
    insert_market_data

@pytest.fixture
def sample_splits():
    return pd.Series({
        pd.Timestamp("2022-01-10"): 2,
        pd.Timestamp("2023-01-05"): 3
    })


@pytest.fixture
def sample_hist_index():
    return pd.date_range(start="2022-01-01", periods=10, freq='D')


# Test for calculate_adjusted_shares
@pytest.mark.parametrize("shares_outstanding, expected", [
    (100, 100/3),  # When the final split factor is 2, the adjusted shares should be half.
    (300, 100)  # When the final split factor is 3, the adjusted shares should be one-third.
])
def test_calculate_adjusted_shares(sample_splits, sample_hist_index, shares_outstanding, expected):
    adjusted_shares = pd.Series(calculate_adjusted_shares(shares_outstanding, sample_splits, sample_hist_index),
                                index=sample_hist_index)
    assert adjusted_shares.iloc[-1] == expected


# Test for fetch_ticker_data
@patch("yfinance.Ticker")
def test_fetch_ticker_data(mock_ticker):
    mock_instance = mock_ticker.return_value
    mock_instance.history.return_value = pd.DataFrame({
        "Close": [100, 200, 300]
    }, index=pd.date_range(start="2023-01-01", periods=3))

    mock_instance.info = {"sharesOutstanding": 1000, "longName": "Mock Company"}
    mock_instance.splits = pd.Series({pd.Timestamp("2023-01-02"): 2})

    ticker, company_name, hist, market_caps = fetch_ticker_data("MOCK", datetime(2023, 1, 1), datetime(2023, 1, 3))

    assert ticker == "MOCK"
    assert company_name == "Mock Company"
    assert not hist.empty
    assert not market_caps.empty
    assert market_caps.iloc[0] == 100 * (1000 / 2)  # Adjusted shares after split


# Test for create_database_schema
def test_create_database_schema():
    conn = duckdb.connect(database=':memory:')
    create_database_schema(conn)
    tables = conn.execute("SHOW TABLES").fetchall()
    assert len(tables) > 0  # Ensure tables exist after schema creation
    conn.close()


# Test for insert_company_data
def test_insert_company_data():
    conn = duckdb.connect(database=':memory:')
    create_database_schema(conn)
    insert_company_data(conn, "MOCK", "Mock Company")
    result = conn.execute("SELECT * FROM companies").fetchall()
    assert len(result) == 1
    conn.close()


# Test for insert_market_data
def test_insert_market_data():
    conn = duckdb.connect(database=':memory:')
    create_database_schema(conn)
    df = pd.DataFrame({
        'date': [datetime(2023, 1, 1)],
        'ticker': ["MOCK"],
        'close_price': [100],
        'market_cap': [100000]
    })
    insert_market_data(conn, df)
    result = conn.execute("SELECT * FROM market_data").fetchall()
    assert len(result) == 1
    conn.close()
