import os
import glob
import re
import datetime
import multiprocessing

import tabula
import tqdm
import pandas as pd

import payments
import utils

date_template_path = "resources/statement-date.json"
template_path_1page = "resources/getdocument.tabula-template.json"
template_path_2page = "resources/getdocument.tabula-template-2page.json"


def extract_date(df):
    statement_date_string = df.columns[0]
    matches = re.search("(\\d{2}/\\d{2}/\\d{4})", statement_date_string)

    if matches is None:
        raise Exception("Could not parse date from statement")

    return datetime.datetime.strptime(matches[1], "%d/%m/%Y")


def find_statements(path):
    return sorted(glob.glob(os.path.join(path, "*.pdf")))


def parse_statement(filename):
    # Extract the statement date from the first page
    ddf = tabula.read_pdf_with_template(filename, date_template_path)
    statement_date = extract_date(ddf[0])

    # get num pages - this will inform the template that we use
    pages = utils.num_pages(filename)

    # Read pdf into list of DataFrame
    template_path = template_path_1page if pages < 5 else template_path_2page
    dfs = tabula.read_pdf_with_template(filename,
                                        template_path,
                                        columns=[50, 105, 465, 560],
                                        pandas_options={
                                            "header": ["junk", "date", "description", "amount", "credit"]
                                        })

    transactions = payments.process(dfs[0], statement_date)

    # Even if we had enough pages to warrant the larger template,
    # it doesn't guarantee more transactions. This checks the end of
    # the transaction table for more results:
    if transactions.iloc[-1, 2] == "Continued":
        transactions2 = payments.process(dfs[1], statement_date)
        transactions.drop(transactions.tail(1).index, inplace=True)
        transactions = transactions.append(transactions2)

    return statement_date, transactions


def single_process(paths):
    all_transactions = []
    for f in tqdm.tqdm(paths):
        date, transactions = parse_statement(f)
        amount_spent = transactions.amount[transactions.amount > 0].sum()
        amount_paid = -1 * transactions.amount[transactions.amount < 0].sum()
        logger.info(f"{date.strftime('%d/%m/%Y')}: £{amount_spent:.2f} (-£{amount_paid})")
        all_transactions.append(transactions)
    return all_transactions


def multi_process(paths, num_processes=8):
    with multiprocessing.Pool(num_processes) as p:
        results = p.map(parse_statement, paths)
        return [r[1] for r in results]


def main(args):
    paths = find_statements(args.input_dir)
    if args.num_processes > 1:
        transaction_list = multi_process(paths, args.num_processes)
    else:
        transaction_list = single_process(paths)

    all_transactions = pd.concat(transaction_list, axis=0)
    all_transactions.sort_values("date", inplace=True)
    all_transactions.to_csv(args.output_path, index=False, date_format="%Y-%m-%d", doublequote=True)


if __name__ == "__main__":
    logger = utils.setup_logging(__name__)
    args = utils.parse_args()
    main(args)
