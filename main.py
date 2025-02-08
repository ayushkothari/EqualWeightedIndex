import argparse
import logging
from datetime import datetime
from data_fetcher import main as data_fetcher_main
from equal_weighted_index_composition import main as equal_weighted_index_composition_main

# Configure logging to display messages in the terminal only
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(message)s'))  # Only show raw messages
logger.addHandler(handler)


def is_valid_date(date_string):
    """Check if the date string is in the format 'YYYY-MM-DD'."""
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def main(start_date=None, end_date=None):
    # Validate start and end dates
    if start_date and not is_valid_date(start_date):
        logger.error(f"Invalid start date format: {start_date}. Expected format: YYYY-MM-DD.")
        return

    if end_date and not is_valid_date(end_date):
        logger.error(f"Invalid end date format: {end_date}. Expected format: YYYY-MM-DD.")
        return

    # If both start_date and end_date are provided, ensure start_date is before end_date
    if start_date and end_date:
        if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
            logger.info(f"Start date {start_date} is later than end date {end_date}. Please provide a valid date range.")
            return
        logger.info(f"Running with passed start_date={start_date} and end_date={end_date}")
        data_fetcher_main(start_date=datetime.strptime(start_date, "%Y-%m-%d") , end_date=datetime.strptime(end_date, "%Y-%m-%d"))

    # If only start_date is provided
    elif start_date:
        logger.info(f"Running with passed start_date={start_date} and no end_date")
        data_fetcher_main(start_date=datetime.strptime(start_date, "%Y-%m-%d") )

    # If only end_date is provided
    elif end_date:
        logger.info(f"Running with passed end_date={end_date} and no start_date")
        data_fetcher_main(end_date=datetime.strptime(end_date, "%Y-%m-%d"))

    else:
        logger.info("Running without passed start_date and end_date")
        data_fetcher_main()

    # Running equal_weighted_index_composition_main and interactive_dashboard_main regardless of dates
    equal_weighted_index_composition_main()
    from interactive_dashboard import main as interactive_dashboard_main
    interactive_dashboard_main()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run the scripts in sequence.")
    parser.add_argument('--start-date', type=str, help="Start date for data processing (optional).")
    parser.add_argument('--end-date', type=str, help="End date for data processing (optional).")

    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args.start_date, args.end_date)
