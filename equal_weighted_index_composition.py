import duckdb
import pandas as pd
import logging
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from constants import NO_OF_COMPANIES, DB_PATH, OUTPUT_PATH
from typing import List, Set

# Configure logging to display messages in the terminal only
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(message)s'))  # Only show raw messages
logger.addHandler(handler)


# =============================================
# Database Operations
# =============================================
def get_market_cap_data() -> pd.DataFrame:
    """Fetch market cap and price data from DuckDB"""
    conn = duckdb.connect(DB_PATH)
    query = """
        SELECT date AS Date, 
               ticker AS Ticker,
               market_cap AS MarketCap,
               close_price AS Price
        FROM market_data
    """
    df = conn.execute(query).fetchdf()
    conn.close()

    # Clean data
    df['MarketCap'] = pd.to_numeric(df['MarketCap'].replace('[\$,]', '', regex=True), errors='coerce')
    df['Price'] = pd.to_numeric(df['Price'].replace('[\$,]', '', regex=True), errors='coerce')
    df['Date'] = pd.to_datetime(df['Date'])

    return df.dropna()


# =============================================
# Index Construction Logic
# =============================================
def get_daily_top_100(df: pd.DataFrame) -> pd.DataFrame:
    """Identify top 100 stocks by market cap each day"""
    return (
        df.groupby("Date", group_keys=False)
            .apply(lambda x: x.nlargest(NO_OF_COMPANIES, "MarketCap"))
            .reset_index(drop=True)
    )


def calculate_weights(df: pd.DataFrame) -> pd.DataFrame:
    """Assign equal weights to constituents"""
    df['Weight'] = 1 / NO_OF_COMPANIES
    return df


def track_composition_changes(df: pd.DataFrame) -> pd.DataFrame:
    """Identify days with changes in index composition"""
    df = df.sort_values('Date')
    changes: List[dict] = []
    prev_constituents: Set[str] = set()

    for date, group in df.groupby('Date'):
        current_constituents = set(group['Ticker'])

        if prev_constituents:
            added = current_constituents - prev_constituents
            removed = prev_constituents - current_constituents
            if added or removed:
                changes.append({
                    'Date': date,
                    'Additions': len(added),
                    'Removals': len(removed),
                    'Added_Tickers': ', '.join(added),
                    'Removed_Tickers': ', '.join(removed)
                })

        prev_constituents = current_constituents

    return pd.DataFrame(changes, columns=['Date', 'Additions', 'Removals', 'Added_Tickers', 'Removed_Tickers'])


def calculate_index_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate index returns and cumulative performance"""
    df['Stock_Return'] = df.groupby('Ticker')['Price'].pct_change()
    index_df = df.groupby('Date').apply(
        lambda x: (x['Stock_Return'] * x['Weight']).sum()
    ).reset_index(name='Daily_Return')

    index_df['Cumulative_Value'] = (1 + index_df['Daily_Return']).cumprod()

    return index_df


# =============================================
# PDF Export Functions
# =============================================
def create_pdf(data: pd.DataFrame, title: str, filename: str) -> None:
    """Create a PDF from a DataFrame"""
    pdf_path = f"{OUTPUT_PATH}{filename}.pdf"
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()

    elements = [Paragraph(title, styles['Title']), Spacer(1, 12)]

    table_data: List[List] = [data.columns.tolist()] + data.values.tolist()

    table = Table(table_data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))

    elements.append(table)
    doc.build(elements)
    logger.info(f"PDF created: {pdf_path}")


# =============================================
# Main Execution
# =============================================
def main() -> None:
    raw_data = get_market_cap_data()
    top_100 = get_daily_top_100(raw_data)
    constituents = calculate_weights(top_100)

    constituents[['Date', 'Ticker', 'MarketCap', 'Weight']].to_csv(
        f"{OUTPUT_PATH}daily_composition.csv", index=False
    )

    changes = track_composition_changes(constituents)
    changes.to_csv(
        f"{OUTPUT_PATH}composition_changes.csv", index=False
    )

    performance = calculate_index_performance(constituents)
    performance.to_csv(
        f"{OUTPUT_PATH}index_performance.csv", index=False
    )

    create_pdf(changes, "Composition Changes", "composition_changes")
    create_pdf(performance, "Index Performance", "index_performance")

    logger.info(f"""
    Files generated:
    1. {OUTPUT_PATH}daily_composition.csv
    2. {OUTPUT_PATH}composition_changes.csv
    3. {OUTPUT_PATH}index_performance.csv
    4. {OUTPUT_PATH}composition_changes.pdf
    5. {OUTPUT_PATH}index_performance.pdf
    """)


if __name__ == "__main__":
    main()
