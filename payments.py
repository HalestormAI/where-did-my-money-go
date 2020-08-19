import datetime

import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None


def name_columns(p):
    cols = ["junk", "date", "description", "amount"]
    if len(p.columns) == 5:
        cols.append("credit")
    p.columns = cols


def clear_nans(df, col):
    df[col].replace(np.nan, '', regex=True, inplace=True)


def process(payments, statement_date, config):
    # The columns don't get proper names coming out of tabula, this will name them
    name_columns(payments)

    payments.drop(columns=["junk"], inplace=True)

    # Set all NaNs to empty strings in the descriptions
    clear_nans(payments, "description")

    # Because the bounding isn't always 100% correct (particularly as we don't
    # know how long the table will be in advance), we'll assume any row that
    # doesn't start with a date is invalid data and can be removed.

    is_valid_date = payments.date.apply(config.check_date_validity)
    payments = payments.loc[is_valid_date, :]

    # Values in the thousands have a comma in them we need to replace
    amt = payments.amount.astype(str).str.replace(',', '')
    amt = pd.to_numeric(amt, errors="coerce")

    # Convert the "credit or debit" column to boolean - true if credit
    if "credit" in payments.columns:
        credit_rows = payments.credit.str.contains("CR")
        cred = payments.credit[credit_rows].str.replace(" CR", "")
        cred = cred.str.replace(',', '')
        cred = pd.to_numeric(cred, errors="coerce").multiply(-1)
        amt[credit_rows] = cred
        payments.drop(columns=["credit"], inplace=True)
    payments["amount"] = amt

    # Now sort dates out
    def handle_date(date_str):
        year = statement_date.year
        if statement_date.month == 1 and "Dec" in date_str:
            year -= 1

        return datetime.datetime.strptime(f"{date_str} {year}", "%d %b %Y")
    payments["date"] = payments.date.apply(handle_date)

    return payments
