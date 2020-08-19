import datetime

from parsers.camelot_parser import CamelotParser

MAX_TABLES = 10

# Table locations, per page.
page_masks = {
    3: '50,597,576,60',
    5: '50,704,576,60',
    6: '50,704,576,60',
}

# Cols must be at least as long as the number of tables we will find. This varies depending on the
# statement but could be as high as 6. Sending too many doesn't hurt.
cols = ['50, 105, 465, 570'] * MAX_TABLES


def camelot_opts(pages):
    return {
        "regions": [page_masks[3]] if pages < 5 else [page_masks[p] for p in [3, 5, 6]],
        "cols": cols,
        "page_list": "3" if pages < 5 else ",".join([str(k) for k in page_masks.keys()]),
        "date_area": ['350,710,575,665'],
        "date_page": "1"
    }


# TODO: This needs extracting when we have multiple config types
def get_opts(parser, num_pages):
    if isinstance(parser, CamelotParser):
        return camelot_opts(num_pages)

    raise NotImplementedError("Only the camelot parser is currently supported")


def check_date_validity(date_str):
    try:
        datetime.datetime.strptime(str(date_str), "%d %b")
        return True
    except ValueError as err:
        if "does not match format" in str(err):
            return False
        raise err
