#!/home/mansuman/venv/bin/python
import sys
from pathlib import Path
import argparse
import pandas as pd
from nsepy import get_history
import logging
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

def download_data(symbol, date):
    logging.info("Downloading 1 year data for {} from {}".format(symbol, date))
    start_date = dt.strptime(date, '%d-%m-%Y').date()
    end_date = start_date + timedelta(weeks=30)

    logging.info("Downloading 7 weeks data for {} from {} to {}".format(symbol, start_date, end_date))

    try:
        data_df = get_history(symbol=symbol, start=start_date, end=end_date)
    except Exception as e:
        logging.error("Failed to download data for symbol {}: {}".format(symbol, str(e)))
        file_loc_err = '../data/.' + symbol + '.err'
        Path(file_loc_err).touch()
        sys.exit(0)

    # data_df.reset_index(drop=True)
    file_name = '../data/' +  symbol + '.csv'
    logging.info('Writing data to file {}'.format(file_name))
    data_df.to_csv(file_name, header=True)


def backtest(symbol, date, price):
    file_name = '../data/' +  symbol + '.csv'
    logging.info("Starting backtesting for %s" % file_name)

    df = pd.read_csv(file_name)


    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index)


    """
    Target = 15% of buy price
    Stoploss = 10% of buy price
    """
    target_15 = price * 1.15
    target_20 = price * 1.20
    stoploss = price * .9

    # Variable to be computed
    max_correction = 99999 
    result_15 = None # "Target Hit" or "SL Hit"
    result_20 = None # "Target Hit" or "SL Hit"
    time_taken = -1
    time_taken_15 = 0 
    time_taken_20 = 0 

    target_hit_15 = False
    target_hit_20 = False
    sl_hit = False

    high_in_3_months = 0
    high_in_6_months = 0
    # Lets iterate through the rows of the dataframe
    for index, row in df.iterrows():
        low = row['Low']
        high = row['High']
        close = row['Close']

        time_taken += 1


        if target_hit_15 is False and close < stoploss:
            sl_hit = True
            result_15 = "SL hit"
            
        if sl_hit is False and target_hit_15 is False:
            if high > target_15:
                result_15 = "Target Hit"
                target_hit_15 = True
                time_taken_15 = time_taken

        if sl_hit is False and target_hit_20 is False:
            if high > target_20:
                result_20 = "Target Hit"
                target_hit_20 = True
                time_taken_20 = time_taken

        if low < max_correction:
            max_correction = low

        if high > high_in_6_months:
            high_in_6_months = high

        if time_taken < 90 and high > high_in_3_months:
            high_in_3_months = high

    max_correction_p = ((price - max_correction)/price) * 100
    logging.info("""
    target_hit_15:       {}
    target_hit_20:       {}
    sl_hit:              {}
    max_correction_p:    {} 
    high_in_3_months:    {}
    high_in_6_months:    {}
    time_taken_15:       {}
    time_taken_20:       {}
    """.format(target_hit_15, target_hit_20, sl_hit, max_correction_p, high_in_3_months, high_in_6_months, time_taken_15, time_taken_20))

    # Update backtest results csv file.

def main():
    # Setup logging
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    # log_format = '[%(asctime)s %(levelname)s %(module)s %(funcName)s] %(message)s'
    log_format = '[ %(asctime)s %(levelname)s ] %(message)s'
    logging.basicConfig(level=logging.DEBUG,
            format=log_format,
            handlers=[logging.FileHandler("backtest_log.txt", mode="a"), stream_handler])

    parser = argparse.ArgumentParser(
            description="""
            ==============================
            Backtest
            ==============================
            """
            )

    parser.add_argument('--symbol', metavar='symbol', help='symbol to backtest', type=str, required=True)
    parser.add_argument('--date', metavar='date', help='date of entry', type=str, required=True)
    parser.add_argument('--price', metavar='price', help='buy price', type=float, required=True)

    args = parser.parse_args()

    # print(vars(args))
    print(args.symbol, args.date, args.price)
    logging.info("Starting backtesting for symbol {} date {} buy price {}".format(args.symbol, args.date, args.price))

    """
    Check if we have already backtested this symbol. If .symbol.done or.symbol.err file exists, we don't backtest this symbol.
    """
    file_loc_check = '../data/.' + args.symbol + '.done'
    file_loc_err = '../data/.' + args.symbol + '.err'

    my_file = Path(file_loc_check)
    if my_file.is_file():
        logging.info("{} file found. Skipping backtesting. If you want to backtest it again, delete this file.".format(file_loc_check)) 
        sys.exit(0)

    my_file = Path(file_loc_err)
    if my_file.is_file():
        logging.info("{} file found. Skipping backtesting. If you want to backtest it again, delete this file. Check the symbol properly, last time while backtesting this symbol, and error was encountered.".format(file_loc_err)) 
        sys.exit(0)

    """
    1. Check if ../data/symbol.csv file exists
    2. If it does not exist, download the data from nse
    3. If it exists, open the file and start processing it.
    """
    data_file_loc = '../data/' + args.symbol + '.csv'
    my_file = Path(data_file_loc)
    if not my_file.is_file():
        download_data(args.symbol, args.date)
    else:
        logging.info("{}  found. Skipping download.".format(data_file_loc))

    backtest(args.symbol, args.date, args.price)
    
if __name__ == "__main__":
    main()
