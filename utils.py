import argparse
import logging
import re

import tqdm


# https://stackoverflow.com/a/38739634/168735
# Doesn't seem to work particularly well...
class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def setup_logging(name, level=logging.INFO):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    log.addHandler(TqdmLoggingHandler())
    return log


# https://stackoverflow.com/a/16649820/168735
def num_pages(path):
    count_pages_pattern = re.compile(rb"/Type\s*/Page([^s]|$)", re.MULTILINE | re.DOTALL)
    with open(path, "rb") as f:
        results = count_pages_pattern.findall(f.read())
        return len(list(filter(lambda y: y == b'\r', results)))


def parse_args():
    parser = argparse.ArgumentParser(description="Generate a dataset CSV from bank statements")
    parser.add_argument("input_dir",
                        help="Path to the directory containing the datasets (note: search is not recursive")
    parser.add_argument("output_path",
                        help="Path into which to store the CSV (if necessary, will create parent dirs.")
    parser.add_argument("--num-processes", type=int, default=1,
                        help="Use multiple processors to speed up statement parsing"),
    return parser.parse_args()
