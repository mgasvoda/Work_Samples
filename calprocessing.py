"""Manage processing of ical files.

Extracts appointment dates and emails for import into CRM
"""
import argparse
import logging
import concurrent.futures
import pandas as pd
import icalendar
import os
import re

from pathlib import Path

ENCODE_OUT = 'utf-8'

log = logging.getLogger(Path(__file__).stem)


def load_calendars(q):
    """Get calendars from working directory + /CalData."""
    calendar_holder = []
    test = 'CalData/' + q
    with open(test) as f:
        temp = f.read()

    cal = icalendar.Calendar.from_ical(temp)
    calendar_holder.append(cal)
    data = process_calendar(calendar_holder)
    return data


def process_calendar(calendar_holder):
    """Extract events into Pandas DataFrame."""
    events = []
    for cal in calendar_holder:
        for x in cal.walk(name='vevent'):
            events.append(x)
    data = pd.DataFrame(data=events)
    return data


def build_out(master):
    """Handle type conversions and prepare output DataFrame."""
    for x in master['DTSTART']:
        x = x.dt
        x = x.strftime("%Y-%m-%d %H:%M:%S")
    for y in master['ATTENDEE']:
        y = re.search("\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b", y)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('folder')
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', action='store_const',
                           const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', dest='verbose',
                           action='store_const', const=logging.WARNING)
    return parser.parse_args()


def main():
    """Manage flow of program."""
    args = parse_args()
    logging.basicConfig(level=args.verbose)
    master = pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_complete = {executor.submit(load_calendars, q): q for q in
                              os.listdir(args.folder)}
        for future in concurrent.futures.as_completed(future_to_complete):
            cal = future_to_complete[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (cal, exc))
            else:
                print('%r page is %d rows' % (cal, len(data)))
                master = master.append(data)

    master = build_out(master)
    master.to_csv('events.csv')
    print("Done")
    print(len(master))


if __name__ == "__main__":
    main()
