import pandas as pd
import logging
import re


def convert_to_object(col_series):
    logging.info(f'Converting "{col_series.name}" to object')
    converted_col = col_series.astype('object')
    return converted_col


def convert_to_dates(col_series):
    logging.info(f'Converting "{col_series.name}" to datetime')
    converted_col = pd.to_datetime(col_series, format='mixed', dayfirst=True, errors='coerce')
    return converted_col


def convert_to_numeric(col_series):
    logging.info(f'Converting "{col_series.name}" to numerics')
    converted_col = clean_numerics(col_series) # also converts
    return converted_col


def convert_dtypes(df, col_dtype_map):
    logging.info('---Converting Dtypes---')
    for col, col_type in col_dtype_map.items():
        if col_type.lower() == 'numerics':
            df[col] = convert_to_numeric(df[col])
        elif col_type.lower() == 'dates':
            df[col] = convert_to_dates(df[col])
        else:
            df[col] = convert_to_object(df[col])
    return df


def clean_numerics(col_series):
    if isinstance(col_series, list):
        col_series = pd.Series(col_series)
    clean_s = col_series.astype(str).str.strip().str.replace(r'^\((.*?)\)$', r'-\1', regex=True)
    col_series = clean_s.str.replace(r'[^0-9.\-]', '', regex=True)
    col_series = pd.to_numeric(col_series, errors='coerce')
    return col_series


def assign_dtype(total, empty, numerics, dates, threshold):
    non_empty = total - empty
    if non_empty == 0:
        return 'object'
    if empty/total >= threshold:
        return 'object'
    if numerics/non_empty >= threshold:
        return 'numerics'
    if dates/non_empty >= threshold:
        return 'dates'
    return 'object'


def accounting_numeric(value):
    s = str(value).strip()
    is_negative = s.count('(') > 0 and s.count(')') > 0
    cleaned = re.sub(r'[(),\s]', '', s)
    cleaned = re.sub(r'[^0-9.\-a-zA-Z]','',cleaned)
    try:
        num = float(cleaned)
        return -num if is_negative else num
    except (ValueError, TypeError):
        return None


def if_acc_numeric(value):
    value = accounting_numeric(value)
    if value is not None:
        return 1
    return 0


def if_numeric(value):
    try:
        float(value)
        return 1
    except (ValueError, TypeError):
        return 0


def if_date(value):
    date_regex = r'(?i)(\d{1,4}[-./]\d{1,2}[-./]\d{1,4}|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    try:
        pd.to_datetime(value, format='mixed', dayfirst=True, errors='raise')
        return 1
    except Exception:
        if re.search(date_regex, str(value)):
            return 1
        return 0


def if_empty(value):
    if str(value).lower().strip() in ['nan', 'na', 'n/a', 'null', '', 'none', '<na>']:
        return 1
    return 0


def identify_dtypes(col_series, threshold):
    total = col_series.shape[0]
    empty = 0
    numerics = 0
    accounting_numerics = 0
    dates = 0
    for value in col_series:
        is_empty = if_empty(value)
        if is_empty:
            empty += 1
            continue
        is_date = if_date(value)
        if is_date:
            dates += 1
            continue
        is_numeric = if_numeric(value)
        if is_numeric:
            numerics += 1
            continue
        is_acc_numeric = if_acc_numeric(value)
        if is_acc_numeric:
            accounting_numerics += 1
            continue
    numerics += accounting_numerics
    dtype = assign_dtype(total, empty, numerics, dates, threshold)
    return dtype


def assign_dtypes(df_sample, threshold):
    col_dtype_map = {}
    for col in df_sample.columns:
        d_type = identify_dtypes(df_sample[col], threshold)
        col_dtype_map[col] = d_type
    return col_dtype_map


def take_sample(df, sample_size):
    total_rows = df.shape[0]
    target_sample = int(total_rows * sample_size)
    min_sample = 10 if 10 <= df.shape[0] else df.shape[0]
    sample = max(min(target_sample, total_rows), min_sample)
    try:
        df_sample = df.sample(n=sample).copy()
    except ValueError:
        return None
    else:
        return df_sample


def map_dtypes(df, tuning):
    sample_size = tuning[0]
    threshold = tuning[1]
    logging.info('Taking data sample for dtype mapping.')
    df_sample = take_sample(df, sample_size)
    if df_sample is None:
        logging.error('Sampling failed.')
        raise ValueError
    col_dtype_map = assign_dtypes(df_sample, threshold)
    return col_dtype_map


def normalize_headers(df):
    logging.info('---Normalizing Headers---')
    try:
        normalized_cols = df.columns.str.strip().str.lower().str.replace(' ', '_')
    except AttributeError as e:
        logging.critical(f'Error occured:{e}')
        raise AttributeError
    new_cols = []
    col_count ={}
    for col in normalized_cols:
        if col not in col_count:
            new_cols.append(col)
            col_count[col] = 1
        else:
            col_count[col] += 1
            new_cols.append(f'{col}_{col_count[col]}')
    df.columns = new_cols
    return df


def cleaner(data, tuning):
    df = data.copy()
    logging.info('----Cleaning Pipeline----')
    try:
        df = normalize_headers(df)
    except AttributeError:
        logging.critical(f'Expected DataFrame received "{type(df)}"')
        raise AttributeError
    else:
        column_dtype_map = map_dtypes(df, tuning)
        df = convert_dtypes(df, column_dtype_map)
    return df
