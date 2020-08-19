import datetime
import re
from collections import defaultdict

import camelot
import numpy as np


def extract_date(df):
    statement_date_string = str(df.iloc[0])
    matches = re.search("(\\d{2}/\\d{2}/\\d{4})", statement_date_string)

    if matches is None:
        raise Exception("Could not parse date from statement")

    return datetime.datetime.strptime(matches[1], "%d/%m/%Y")


def filter_multiple_tables(tables, check_date_fn):
    page_tables = defaultdict(list)
    page_table_sizes = defaultdict(list)
    for t in tables:
        # the second column should always the date. If this is an invalid table, there will be
        # zero rows with a valid date.
        is_valid_date = t.df.iloc[:, 1].apply(check_date_fn)
        if np.any(is_valid_date):
            page_tables[t.page].append((t, t.shape))

    def pick_largest_table(table_list):
        num_rows = [t[1][0] for t in table_list]
        return table_list[np.argmax(num_rows)][0]

    return [pick_largest_table(t) for t in page_tables.values()]


class CamelotParser(object):
    def __init__(self, stmt_config):
        self.config = stmt_config

        # TODO: Make configurable
        self.min_table_accuracy = 90

    def date(self, filename, pages):
        opts = self.config.get_opts(self, pages)
        ddf = camelot.read_pdf(filename,
                               pages=opts["date_page"],
                               flavor='stream',
                               table_areas=opts["date_area"])
        return extract_date(ddf[0].df)

    def transaction_tables(self, filename, pages):
        opts = self.config.get_opts(self, pages)
        # Read pdf into list of DataFrame
        dfs = camelot.read_pdf(filename,
                               pages=opts["page_list"],
                               flavor='stream',
                               table_regions=opts["regions"],
                               columns=opts["cols"])

        tables = [t for t in dfs if t.parsing_report["accuracy"] > self.min_table_accuracy]

        # On rare occasions, camelot will throw the same table up multiple times with slightly
        # different headers. We'll keep only the largest table on any given page.
        return filter_multiple_tables(tables, self.config.check_date_validity)
