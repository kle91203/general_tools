import json
import logging
import traceback
from datetime import datetime
import csv


def curr_time_nanos():
    return super_magical_timestamp_maker()


def super_magical_timestamp_maker():
    return int(datetime.now().timestamp() * 1000000)


def scrub(param):
    if param is None:
        return param
    if not isinstance(param, str):
        return param
    return param.strip()


def load_csv_as_json(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        rv = {}
        for row in reader:
            try:
                rv[row['identifier']] = row
            except Exception:
                logging.error(traceback.format_exc())
    return rv


def load_csv_as_json_rownum_as_key(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        rv = {}
        cnt = 1
        for row in reader:
            try:
                rv[cnt] = row
            except Exception:
                logging.error(traceback.format_exc())
            cnt += 1
    return rv


# noinspection PyUnusedLocal
def black_hole(arg):
    pass


def raise_an_exception______raise_an_exception______raise_an_exception______raise_an_exception______raise_an_exception______raise_an_exception______raise_an_exception______raise_an_exception():
    raise RuntimeError('You asked me to raise an exception')


def read_file(file_path, binary=False):
    _type='rb' if binary else 'r'
    with open(file_path, _type) as input_file:
        content = input_file.read()
    return content


def write_json_to_file(file_name, content):
    with open(file_name, 'w') as outfile:
        json.dump(content, outfile)


def populated(some_string):
    if some_string is None:
        return False
    if not isinstance(some_string, str):
        raise ValueError('argument must be a string')
    rv = some_string.strip() != ''
    return rv


def nbe(some_string):
    if some_string is None:
        return True
    if not isinstance(some_string, str):
        raise ValueError('argument must be a string')
    rv = some_string.strip() == ''
    return rv


def nbe_to_empty_space(some_string):
    if some_string is None:
        return ''
    if not isinstance(some_string, str):
        return ''
    rv = some_string.strip()
    return rv


def all_nbe(strings):
    rv = True
    for string in strings:
        rv = rv and nbe(string)
    return rv


def nbe_to_none_or_strip(some_string):
    if some_string is None:
        return None
    if not isinstance(some_string, str):
        return some_string
    if some_string.strip() == '':
        return None
    else:
        return some_string.strip()


def missing_five_identifiers(person):
    rv = nbe(person["contact"]["work_email"]) and nbe(person["contact"]["personal_email"]) and nbe(
        person["contact"]["work_phone"]) and nbe(person["contact"]["personal_phone"]) and nbe(
        person["contact"]["twitter"])
    return rv


def str_index_safe(string, substring):
    if substring in string:
        indx = string.index(substring)
        return indx
    else:
        return None


def zero_to_none(some_number):
    if some_number is None:
        return None
    if not isinstance(some_number, int):
        return some_number
    if some_number == 0:
        return None
    else:
        return some_number

