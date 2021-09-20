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
import sqlite3


"""
NSE Cache implementation BEGIN
"""
class NSEDB:
    instance = None
    db_name = "nsecache.db"
    home_dir = str(Path.home())
    # working_dir = home_dir + "/.nsecache/"
    working_dir = "./.nsecache/"
    db_location = working_dir + db_name

    def __new__(cls, *args, **kwargs):
        logging.debug("__new__ Enter")
        if cls.instance is None:
            logging.info("Creating new NSEDB instance")
            cls.instance = super().__new__(NSEDB)

            """
            This is our first time. Lets setup NSE cache.
            Initialize the NSE cache.
            """
            if Path(cls.working_dir).is_dir() is False:
                logging.info("Creating nsecache directory...")
                # Create cache directory
                """
                mode = 0o666
                try:
                    os.mkdir(cls.working_dir, mode)
                except Exception as e:
                    logging.err("Could not create cache dir: {}".str(e))
                    sys.exit(0)
                """
                try:
                    p = Path(cls.working_dir)
                    p.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logging.err("Could not create cache dir: {}".str(e))
                    sys.exit(0)

                logging.info("Creating cache database...")
                try:
                    conn = sqlite3.connect(cls.db_location)
                except sqlite3.Error as e:
                    logging.error("Sqlite connect error: {}".format(str(e)))
                    logging.info("Deleting cache dir")
                    os.rmdir(cls.working_dir)
                    sys.exit(0)

                # Create an empty table
                cmd_1 = """ 
                CREATE TABLE IF NOT EXISTS "NSECACHE" (
                "Date" DATE,
                  "Symbol" TEXT,
                  "Series" TEXT,
                  "Prev Close" REAL,
                  "Open" REAL,
                  "High" REAL,
                  "Low" REAL,
                  "Last" REAL,
                  "Close" REAL,
                  "VWAP" REAL,
                  "Volume" INTEGER,
                  "Turnover" REAL,
                  "Trades" INTEGER,
                  "Deliverable Volume" INTEGER,
                  "%Deliverble" REAL
                );
                """
                cmd_2 = """
                CREATE INDEX "ix_NSECACHE_Date"ON "NSECACHE" ("Date");
                """

                logging.info("Creating empty table NSECACHE...")
                c = conn.cursor()
                c.execute(cmd_1)
                c.execute(cmd_2)
                conn.commit()

        return cls.instance

    def __init__(self):
        logging.debug("__init__ Enter")
        self.conn = self.connect()
        self.cursor = self.conn.cursor()

    def connect(self):
        logging.debug("connect Enter")
        try:
            return sqlite3.connect(self.db_location)
        except sqlite3.Error as e:
            logging.error("Sqlite connect error: ({}) {}".format(self.db_location, str(e)))

    def __del__(self):
        print("__del__ Enter")
        try:
            self.cursor.close()
            self.conn.close()
        except:
            pass

def get_nse_history_from_cache(symbol, start, end):
    start_date = dt.strptime(start, '%d-%m-%Y').date()
    end_date = dt.strptime(end, '%d-%m-%Y').date()


    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    query = "SELECT * FROM NSECACHE WHERE Symbol = '%s' AND Date BETWEEN '%s' AND '%s'" %(symbol, start_date_str, end_date_str)

    db_instance = NSEDB()
    df = pd.read_sql_query(query, db_instance.conn)

    return df

def get_nse_history(symbol, start, end):
    """
    First check if we have the data in our cache. If we have it, return it.
    Else, fetch the data from NSE India site, update the cache and return it.
    Hack: We always query one week more data than the end date.
    Note: We expect date to be a string in dd-mm-yyyy format.
    """
    start_date = dt.strptime(start, '%d-%m-%Y').date()
    end_date = dt.strptime(end, '%d-%m-%Y').date()

    # Sanitize dates
    if end_date < start_date:
        # Return empty dataframe
        return pd.DataFrame()

    # Fetch one week before and after the requested start and end date
    start_date_fetch = start_date - timedelta(weeks=1)
    end_date_fetch = end_date + timedelta(weeks=1)
    df = get_nse_history_from_cache(symbol, start_date_fetch.strftime('%d-%m-%Y'), end_date_fetch.strftime('%d-%m-%Y'))

    print(df)

    # Check if dataframe is empty
    if df.empty:
        #Fetch data from 2017 till today.
        from_date = dt.strptime('01-01-2017', '%d-%m-%Y').date()
        to_date = date.today()

        try:
            data_df = get_history(symbol=symbol, start=from_date, end=to_date)
        except Exception as e:
            logging.error("Failed to download data for symbol {}: {}".format(symbol, str(e)))
            sys.exit(0)

        try:
            db_instance = NSEDB()
            data_df.to_sql('NSECACHE', db_instance.conn, if_exists='append', index = True)
        except Exception as e:
            logging.error("{}".format(str(e)))

        # We have updated the cache. Now loop over again to get the data from cache.
        df = get_nse_history_from_cache(symbol, start, end_date_fetch.strftime('%d-%m-%Y'))

    if not df.empty:
        df.set_index('Date', inplace=True)
        df.index = pd.to_datetime(df.index)
        # Prune the dataframe to fit between start date and end date.
        # Does not yet work: mask = (df.index >= start_date) & (df.index <= end_date)
        # df = df.loc[mask]
        # df = df.loc[start_date.strftime('%Y-%m-%d'): (end_date + timedelta(days=1)).strftime('%Y-%m-%d')]

        df = df.loc[start_date.strftime('%Y-%m-%d'): end_date.strftime('%Y-%m-%d')]

    return df

def get_nse_history_1(symbol, start, end):
    """
    First check if we have the data in our cache. If we have it, return it.
    Else, fetch the data from NSE India site, update the cache and return it.
    Hack: We always query one week more data than the end date.
    Note: We expect date to be a string in dd-mm-yyyy format.
    """
    fetch_from_nse = False
    from_date = None

    start_date = dt.strptime(start, '%d-%m-%Y').date()
    end_date = dt.strptime(end, '%d-%m-%Y').date()

    # Sanitize dates
    if end_date < start_date:
        # Return empty dataframe
        return pd.DataFrame()

    # Fetch one week before and after the requested start and end date
    start_date_fetch = start_date - timedelta(weeks=1)
    end_date_fetch = end_date + timedelta(weeks=1)
    df = get_nse_history_from_cache(symbol, start_date_fetch.strftime('%d-%m-%Y'), end_date_fetch.strftime('%d-%m-%Y'))

    # Check if dataframe is empty
    if df.empty:
        fetch_from_nse = True
        from_date = start_date
    else:
        # Check if dataframe is incomplete
        # 1) We should get at least one entry which has date more than end_date
        # Otherwise, check the last date date and fetch from there.
        last_date_str = df.iloc[-1].Date
        last_date = dt.strptime(last_date_str, '%Y-%m-%d').date()

        if last_date < end_date:
            fetch_from_nse = True
            from_date = last_date + timedelta(days=1)
            logging.info("We have data till %s. Fetch from NSE." % last_date_str)

        # 2) The first frame date should be equal to the start date
        first_date_str = df.iloc[0].Date
        first_date = dt.strptime(first_date_str, '%Y-%m-%d').date()
        if first_date > start_date:
            fetch_from_nse = True
            from_date = start_date_fetch
            logging.info("We have data from %s. Fetch from NSE." % first_date_str)

    if fetch_from_nse is True:
        # Fetch data from NSE. We fetch a minimum of 7 months of data in one query.
        # to_date = from_date + timedelta(weeks=30)
        to_date = end_date_fetch

        if end_date > to_date:
            to_date = end_date

        if to_date > date.today():
            to_date = date.today()

        if from_date == date.today():
            # If today is a weekend, return.
            weekno = dt.today().weekday()
            # Monday = 0, Friday = 4
            if weekno > 4:
                if not df.empty:
                    df.set_index('Date', inplace=True)
                    df.index = pd.to_datetime(df.index)
                    # Prune the dataframe to fit between start date and end date.
                    # Does not yet work: mask = (df.index >= start_date) & (df.index <= end_date)
                    # df = df.loc[mask]
                    # df = df.loc[start_date.strftime('%Y-%m-%d'): (end_date + timedelta(days=1)).strftime('%Y-%m-%d')]

                    df = df.loc[start_date.strftime('%Y-%m-%d'): end_date.strftime('%Y-%m-%d')]
                    return df
                

        # Fetch data from before 2 weeks of start date.
        from_date = from_date - timedelta(weeks=2)

        logging.info("Fetch data from NSE for %s from %s to %s" % (symbol, from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")))

        try:
            data_df = get_history(symbol=symbol, start=from_date, end=to_date)
        except Exception as e:
            logging.error("Failed to download data for symbol {}: {}".format(symbol, str(e)))
            sys.exit(0)

        try:
            db_instance = NSEDB()
            data_df.to_sql('NSECACHE', db_instance.conn, if_exists='append', index = True)
        except Exception as e:
            logging.error("{}".format(str(e)))

        # We have updated the cache. Now loop over again to get the data from cache.
        df = get_nse_history_from_cache(symbol, start, end_date_fetch.strftime('%d-%m-%Y'))

    if not df.empty:
        df.set_index('Date', inplace=True)
        df.index = pd.to_datetime(df.index)
        # Prune the dataframe to fit between start date and end date.
        # Does not yet work: mask = (df.index >= start_date) & (df.index <= end_date)
        # df = df.loc[mask]
        # df = df.loc[start_date.strftime('%Y-%m-%d'): (end_date + timedelta(days=1)).strftime('%Y-%m-%d')]

        df = df.loc[start_date.strftime('%Y-%m-%d'): end_date.strftime('%Y-%m-%d')]


    return df


"""
NSE Cache implementation END
"""

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


def backtest_sell(symbol, date, price):
    # get 7 months of data for backtesting..
    start_date = dt.strptime(date, '%d-%m-%Y').date()
    end_date = start_date + timedelta(weeks=30)

    df = get_nse_history(symbol, date, end_date.strftime('%d-%m-%Y'))

    # df.set_index('Date', inplace=True)
    # df.index = pd.to_datetime(df.index)

    if price == 0:
        try:
            price = df.iloc[0].Close
        except Exception as e:
            # Something wrong with this symbol.
            logging.error("Stopping backtesting for symbol: {}: {}".format(symbol, str(e)))
            file_loc_err = '../data/.' + symbol + '.err'
            Path(file_loc_err).touch()
            sys.exit(0)


    logging.info("Starting backtesting for Symbol -> %s Price -> %f Date -> %s" % (symbol, price, date))

    """
    Target = 15% of buy price
    Stoploss = 10% of buy price
    """
    target_15 = price * .97
    target_20 = price * .95
    stoploss = price * 1.05

    # Variable to be computed
    max_correction = price
    result_15 = None # "Target Hit" or "SL Hit"
    result_20 = None # "Target Hit" or "SL Hit"
    time_taken = -1
    time_taken_15 = 0 
    time_taken_20 = 0 

    target_hit_15 = False
    target_hit_20 = False
    sl_hit = False

    low_in_3_months = price
    low_in_6_months = price
    # Lets iterate through the rows of the dataframe
    for index, row in df.iterrows():
        low = row['Low']
        high = row['High']
        close = row['Close']

        time_taken += 1
        if time_taken == 0:
            continue

        if target_hit_15 is False and close > stoploss:
            sl_hit = True
            result_15 = "SL hit"
            result_20 = "SL hit"
            
        if sl_hit is False and target_hit_15 is False:
            if low < target_15:
                result_15 = "Target Hit"
                target_hit_15 = True
                time_taken_15 = time_taken

        if sl_hit is False and target_hit_20 is False:
            if low < target_20:
                result_20 = "Target Hit"
                target_hit_20 = True
                time_taken_20 = time_taken

        if high > max_correction:
            max_correction = high

        if low < low_in_6_months:
            low_in_6_months = low

        if time_taken < 90 and low < low_in_3_months:
            low_in_3_months = low

    if max_correction != 99999:
        max_correction_p = ((price - max_correction)/price) * 100
    else:
        max_correction_p = 0

    logging.info("""
    target_hit_15:       {}
    target_hit_20:       {}
    sl_hit:              {}
    max_correction_p:    {} 
    low_in_3_months:    {}
    low_in_6_months:    {}
    time_taken_15:       {}
    time_taken_20:       {}
    """.format(target_hit_15, target_hit_20, sl_hit, max_correction_p, low_in_3_months, low_in_6_months, time_taken_15, time_taken_20))

    # Update backtest results csv file.
    """
    Date,Symbol,Marketcapname,Sector,Buy Price,Stoploss(10%),Target(15%),Max correction from Buy Price,Result(15% Target),Time taken to hit 15%,Target(20%),Time taken to hit 20%,Result(20% Target),High in 3 Months,High in 6 Months
    """
    with open("../data/backtest_results.csv", "a") as fh:
        entry_str = "{},{},---, ---,{},{},{},{},{},{},{},{},{},{},{}\n".format(date, symbol, price, stoploss, target_15, max_correction_p, result_15, time_taken_15, target_20, time_taken_20, result_20, low_in_3_months, low_in_6_months)
        fh.write(entry_str)

def backtest_buy(symbol, date, price):
    # get 7 months of data for backtesting..
    start_date = dt.strptime(date, '%d-%m-%Y').date()
    end_date = start_date + timedelta(weeks=30)

    df = get_nse_history(symbol, date, end_date.strftime('%d-%m-%Y'))

    # df.set_index('Date', inplace=True)
    # df.index = pd.to_datetime(df.index)

    if price == 0:
        try:
            price = df.iloc[0].Close
        except Exception as e:
            # Something wrong with this symbol.
            logging.error("Stopping backtesting for symbol: {}: {}".format(symbol, str(e)))
            file_loc_err = '../data/.' + symbol + '.err'
            Path(file_loc_err).touch()
            sys.exit(0)


    logging.info("Starting backtesting for Symbol -> %s Price -> %f Date -> %s" % (symbol, price, date))

    """
    Target = 15% of buy price
    Stoploss = 10% of buy price
    """
    target_15 = price * 1.03
    target_20 = price * 1.05
    stoploss = price * .95

    # Variable to be computed
    max_correction = price
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
        if time_taken == 0:
            continue

        if target_hit_15 is False and close < stoploss:
            sl_hit = True
            result_15 = "SL hit"
            result_20 = "SL hit"
            
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

        if low < max_correction and target_hit_15 is False:
            max_correction = low

        if high > high_in_6_months:
            high_in_6_months = high

        if time_taken < 90 and high > high_in_3_months:
            high_in_3_months = high

    if max_correction != 99999:
        max_correction_p = ((price - max_correction)/price) * 100
    else:
        max_correction_p = 0

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
    """
    Date,Symbol,Marketcapname,Sector,Buy Price,Stoploss(10%),Target(15%),Max correction from Buy Price,Result(15% Target),Time taken to hit 15%,Target(20%),Time taken to hit 20%,Result(20% Target),High in 3 Months,High in 6 Months
    """
    with open("../data/backtest_results.csv", "a") as fh:
        entry_str = "{},{},---, ---,{},{},{},{},{},{},{},{},{},{},{}\n".format(date, symbol, price, stoploss, target_15, max_correction_p, result_15, time_taken_15, target_20, time_taken_20, result_20, high_in_3_months, high_in_6_months)
        fh.write(entry_str)

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

    parser.add_argument('--symbol', metavar='symbol', help='symbol to backtest', type=str)
    parser.add_argument('--date', metavar='date', help='date of entry if dd-mm-yyyy format', type=str)
    parser.add_argument('--price', metavar='price', help='buy price (0 for close price of the day)', type=float)
    parser.add_argument('--type', metavar='type', help='long or short', type=str)
    parser.add_argument('--update_db', help='Update the nsecache DB (time taking!!)', action='store_true', default=False)

    args = parser.parse_args()

    if args.update_db:
        print("Update the NSE cache DB..")
        sys.exit(0)
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

    if args.type == 'buy':
        backtest_buy(args.symbol, args.date, args.price)
    elif args.type == 'sell':
        backtest_sell(args.symbol, args.date, args.price)
    else:
        print("--type should be buy or sell.")


if __name__ == "__main__":
    main()
