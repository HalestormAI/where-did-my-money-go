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


def process(payments, statement_date):
    # The columns don't get proper names coming out of tabula, this will name them
    # and account for the mysterious 5th column that appears in some statements

    name_columns(payments)

    payments.drop(columns=["junk"], inplace=True)

    # Set all NaNs to empty strings in the descriptions
    clear_nans(payments, "description")

    # Because we don't know how large this table will be, tabula goes
    # beyond the end of the table and brings in irrelevant text.
    # This always starts with "If you do not" in the date column, so
    # we'll delete that and all subsequent rows.
    # end_idx = payments.index[payments['date'] == "If you do not"].tolist()
    # if end_idx:
    #     payments = payments[payments.index < end_idx]

    def check_date_validity(date_str):
        try:
            datetime.datetime.strptime(str(date_str), "%d %b")
            return True
        except ValueError as err:
            if "does not match format" in str(err):
                return False
            raise err

    is_valid_date = payments.date.apply(check_date_validity)

    payments = payments.loc[is_valid_date, :]

    # Values in the thousands have a comma in them we need to replace
    amt = payments.amount.astype(str).str.replace(',', '')
    amt = pd.to_numeric(amt, errors="coerce")

    # Convert the "credit or debit" column to boolean - true if credit
    if "credit" in payments.columns:
        payments.credit = payments.credit == "CR"
        amt[payments.credit] = amt[payments.credit].multiply(-1)
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
