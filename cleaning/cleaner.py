import pandas as pd
import logging
import re



def drop_duplicates(df, validator=None):
    dup_count = df.duplicated().sum()
    df.drop_duplicates(inplace=True)


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
        if col_type.lower() == 'numeric':
            df[col] = convert_to_numeric(df[col])
        elif col_type.lower() == 'date':
            df[col] = convert_to_dates(df[col])
        else:
            df[col] = convert_to_object(df[col])
    return df


def is_date(sample):
    garbage_count = 0
    empty_count = 0
    re_sample = []
    date_regex = r'(?i)(\d{1,4}[-./]\d{1,2}[-./]\d{1,4}|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    for value in sample:
        v_str = str(value).lower().strip()
        if v_str in ['nan', 'na', 'n/a', 'null', '', 'none', '<na>']:
            empty_count += 1
            continue
        try:
            float_val = float(value)
            if float_val > 3000:
                 pass
            else:
                garbage_count += 1
                continue
        except (ValueError, TypeError):
            pass
        if not re.search(date_regex, v_str):
            garbage_count += 1
            continue
        try:
            pd.to_datetime(value, format='mixed', errors='raise', dayfirst=True)
            re_sample.append(value)
        except (ValueError, TypeError, OverflowError):
            if re.search(date_regex, v_str):
                re_sample.append(value)
            else:
                garbage_count += 1
    clean_sample = pd.Series(re_sample)
    if clean_sample.empty:
        return 0
    converted = pd.to_datetime(clean_sample, format='mixed', errors='coerce', dayfirst=True)
    passed_count = converted.notna().sum()
    if (empty_count + garbage_count) / len(sample) >= 0.5:
        return 0
    else:
        total_values = len(sample) - empty_count
        return passed_count / total_values if total_values > 0 else 0


def clean_numerics(col_series, validator=None):
    if isinstance(col_series, list):
        col_series = pd.Series(col_series)
    clean_s = col_series.astype(str).str.strip().str.replace(r'^\((.*?)\)$', r'-\1', regex=True)
    col_series = clean_s.str.replace(r'[^0-9.\-]', '', regex=True)
    col_series = pd.to_numeric(col_series, errors='coerce')
    return col_series


def is_accounting_numeric(value):
    s = str(value).strip()
    is_negative = s.count('(') > 0 and s.count(')') > 0
    cleaned = re.sub(r'[(),\s]', '', s)
    cleaned = re.sub(r'[^0-9.\-a-zA-Z]','',cleaned)
    try:
        num = float(cleaned)
        return -num if is_negative else num
    except (ValueError, TypeError):
        return None

def is_numeric(sample):
    garbage_count = 0
    empty_count = 0
    re_sample = []
    date_regex = r'(?i)(\d{2}[-./]\d{2}[-./]\d{4}|\d{4}[-./]\d{2}[-./]\d{2}|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    for value in sample:
        if str(value).lower().strip() in ['nan', 'na', 'n/a', 'null', '', 'none', '<na>']:
            empty_count += 1
            continue
        try:
            if isinstance(float(value), float):
                re_sample.append(value)
                continue
        except (ValueError, TypeError):
            pass
        if not re.search(r'\d', str(value)):
            garbage_count += 1
            continue
        try:
            pd.to_datetime(value, format='mixed', errors='raise', dayfirst=True)
            garbage_count += 1
        except ValueError:
                pass
        if re.search(date_regex, str(value)):
            garbage_count += 1
            continue
        if is_accounting_numeric(value):
            re_sample.append(is_accounting_numeric(value))
            continue
        re_sample.append(value)
    clean_sample = pd.Series(re_sample)
    if clean_sample.empty:
        return 0
    converted = pd.to_numeric(clean_sample, errors='coerce')
    passed_count = converted.notna().sum()
    if (empty_count + garbage_count) / len(sample) >= 0.5:
        return 0
    else:
        total_values = len(sample) - empty_count
        return passed_count/total_values if total_values > 0 else 0


def detect_dtypes(col_series, threshold):
    if threshold >= 0.95:
        gray_zone = threshold - 0.05
    elif threshold >= 0.80:
        gray_zone = threshold - 0.10
    else:
        gray_zone = threshold * 0.7
    result = is_numeric(col_series)
    if result >= threshold:
        return "numeric"
    if result >= gray_zone:
        old_result = result
        clean_sample = clean_numerics(col_series)
        new_result = is_numeric(clean_sample)
        if new_result > old_result:
            if new_result >= threshold or threshold - new_result < 0.02:
                return 'numeric'
    result = is_date(col_series)
    if result >= threshold:
        return 'date'
    return 'object'


def assign_dtypes(df_sample, threshold):
    col_dtype_map = {}
    for col in df_sample.columns:
        d_type = detect_dtypes(df_sample[col], threshold)
        col_dtype_map[col] = d_type
    return col_dtype_map


def take_sample(df, sample_size):
    total_rows = df.shape[0]
    target_sample = int(total_rows * sample_size)
    min_sample = 10 if 10 <= df.shape[0] else df.shape[0]
    sample = max(min(target_sample, total_rows), min_sample)
    df_sample = df.sample(n=sample).copy()
    if df_sample is None:
        return None
    else:
        return df_sample


def map_dtypes(df, tuning):
    sample_size = tuning[0]
    threshold = tuning[1]
    df_sample = take_sample(df, sample_size)
    col_dtype_map = assign_dtypes(df_sample, threshold)
    return col_dtype_map


def normalize_headers(df, validator=None):
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


def cleaner(data, tuning, validator=None):
    df = data.copy()
    logging.info('----Cleaning Pipeline----')
    try:
        df = normalize_headers(df, validator)
    except AttributeError:
        logging.critical(f'Expected DataFrame received "{type(df)}"')
    else:
        column_dtype_map = map_dtypes(df, tuning)
        df = convert_dtypes(df, column_dtype_map)
        drop_duplicates(df, validator)
    return df, validator

def clean_data(data):
    sample_size = 0.1
    threshold = 0.9
    tuning = (sample_size, threshold)
    data, report = cleaner(data, tuning, None)
    return data