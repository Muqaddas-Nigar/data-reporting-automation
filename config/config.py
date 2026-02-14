import yaml
from pathlib import Path
import logging


def normalize_nested_keys(value):
    if isinstance(value, dict):
        container = {key.lower(): value[key] for key in value.keys()}
        return container
    return value


def normalize_keys(config):
    container = {
        key.lower(): normalize_nested_keys(config[key]) for key in config.keys()
        }
    config = container
    return config


def verify_data(config):
    keys = [
        'database',
        'columns',
        'raw_files',
        'processed_files',
        'sample_size',
        'threshold'
        ]
    missing = []
    for key in keys:
        if key not in config.keys():
            missing.append(key)
    if missing:
        logging.error(
            'Missing keys in config.yaml' + ', '.join(missing)
            )
        raise ValueError
    if not isinstance(config['columns'], list):
        logging.error(
            'Columns must be provided in a list'
            )
        raise TypeError


def load_file(path):
    try:
        with open(path) as yml:
            config = yaml.safe_load(yml)
            logging.info('Configurations loaded.')
            return config
    except FileNotFoundError as e:
        logging.exception(f'config.yaml file missing:{e}')
        raise
    except yaml.YAMLError as e:
        if hasattr(e, 'problem_mark'):
            mark = e.problem_mark
            logging.exception(
                f'config.yaml file syntax error on line: {mark.line + 1 }'
                )
        else:
            logging.exception(f'Syntax error in config.yaml:{e}')
        raise 


def load_config():
    path = Path(__file__).parent.resolve()/'config.yaml'
    config = load_file(path)
    config = normalize_keys(config)
    verify_data(config)
    return config
