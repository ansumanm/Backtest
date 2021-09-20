#!/home/mansuman/venv/bin/python
import pandas as pd
import sys
import subprocess

backtest_file = "../data/backtest_data.csv"
print("Opening backtesting data...")
df = pd.read_csv(backtest_file)

print(df.columns)


for index, row in df.iterrows():
    proc = subprocess.Popen(
            ['./backtest.py', '--date', row['date'], '--symbol', row['symbol'], '--price', '0', '--type', 'buy'],
            stdout=subprocess.PIPE)

    out, err = proc.communicate()
    print (out.decode('utf-8'))

    # print("================================")
    # print('./backtest.py', '--date', row['date'], '--symbol', row['symbol'], '--price', '0')
