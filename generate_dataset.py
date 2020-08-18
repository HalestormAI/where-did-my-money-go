import glob
import multiprocessing
import os

import pandas as pd
import tqdm

import payments
import utils
from configs import cc_statement_1
from parsers import camelot_parser


def find_statements(path):
    return sorted(glob.glob(os.path.join(path, "*.pdf")))


def parse_statement(parser, statement_config, filename):
    # get num pages - this will inform the config params that we use
    pages = utils.num_pages(filename)

    # Extract the statement date from the first page
    statement_date = parser.date(filename, pages)

    tables = parser.transaction_tables(filename, pages)

    transactions = None
    for i in tables:
        t = payments.process(i.df, statement_date, statement_config)
        if transactions is None:
            transactions = t
        else:
            transactions = transactions.append(t)

    return statement_date, transactions


def single_process(paths):
    # TODO: These shouldn't be hardcoded
    config = cc_statement_1
    parser = camelot_parser.CamelotParser(config)

    all_transactions = []
    for f in tqdm.tqdm(paths):
        date, transactions = parse_statement(parser, config, f)
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
