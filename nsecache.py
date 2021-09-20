#!/home/mansuman/venv/bin/python
import sys
import os

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
            logging.info("Incomplete data in cache. Need to fetch from NSE")

        # 2) The first frame date should be equal to the start date
        first_date_str = df.iloc[0].Date
        first_date = dt.strptime(first_date_str, '%Y-%m-%d').date()
        if first_date > start_date:
            fetch_from_nse = True
            from_date = start_date

    if fetch_from_nse is True:
        # Fetch data from NSE. We fetch a minimum of 7 months of data in one query.
        to_date = from_date + timedelta(weeks=30)

        if end_date > to_date:
            to_date = end_date

        if to_date > date.today():
            to_date = date.today()

        if from_date == date.today():
            # If today is a weekend, return.
            weekno = dt.today().weekday()
            # Monday = 0, Friday = 4
            if weekno > 4:
                return df


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

def main():
    # Setup logging
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    # log_format = '[%(asctime)s %(levelname)s %(module)s %(funcName)s] %(message)s'
    log_format = '[ %(asctime)s %(levelname)s ] %(message)s'
    logging.basicConfig(level=logging.DEBUG,
            format=log_format,
            handlers=[logging.FileHandler("nsecache_log.txt", mode="a"), stream_handler])


    # df = get_nse_history('SBIN', '1-1-2021', '10-2-2021')
    df = get_nse_history('TV18BRDCST', '25-2-2021', '5-3-2021')
    print(df.head(10))

    """
    db_instance = NSEDB()

    symbol = "SBIN"
    start_date = dt.strptime("1-1-2021", '%d-%m-%Y').date()
    end_date = dt.strptime("1-2-2021", '%d-%m-%Y').date()

    try:
        data_df = get_history(symbol=symbol, start=start_date, end=end_date)
    except Exception as e:
        logging.error("Failed to download data for symbol {}: {}".format(symbol, str(e)))
        file_loc_err = '../data/.' + symbol + '.err'
        Path(file_loc_err).touch()
        sys.exit(0)

    print(data_df)

    try:
        data_df.to_sql('NSECACHE', db_instance.conn, if_exists='replace', index = True)
    except Exception as e:
        logging.error("{}".format(str(e)))

    df = pd.read_sql_query("SELECT * FROM NSECACHE WHERE Symbol = 'SBIN' AND Date BETWEEN '2021-01-01' AND '2021-01-10'", db_instance.conn)
    # df = pd.read_sql_query("SELECT * FROM NSECACHE WHERE Symbol = 'SBIN'", db_instance.conn)
    print(df)
    """

main()
