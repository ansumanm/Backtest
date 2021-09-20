#!/home/mansuman/venv/bin/python
import datetime
import calendar
import sys

def findDay(date):
    born = datetime.datetime.strptime(date, '%d-%m-%Y').weekday()
    return (calendar.day_name[born])

# date = '1-3-2021'
date = sys.argv[1]
print(findDay(date))

