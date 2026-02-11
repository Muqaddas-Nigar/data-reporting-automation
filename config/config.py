import yaml
from pathlib import Path
import logging


class MissingDataInConfig(Exception):
    pass


def verify_data(config):
    keys = ['database', 'columns', 'raw_files', 'processed_files']
    missing = []
    for key in keys:
        if not config.get(key, None):
            missing.append(key)
    if missing:
        logging.error(
            'Missing keys in config.yaml' + ', '.join(missing)
            )
        raise MissingDataInConfig()
    if not isinstance(config['columns'], list):
        logging.error(
            'Columns must be provided in a list'
            )
        print(config['columns'])
        raise TypeError()


def load_file(path):
    try:
        with open(path) as yml:
            config = yaml.safe_load(yml)
            logging.info('Configurations loaded.')
            return config
    except FileNotFoundError as e:
        logging.exception(f'config.yaml file missing:{e}')
        raise e
    except yaml.YAMLError as e:
        if hasattr(e, 'problem_mark'):
            mark = e.problem_mark
            logging.exception(
                f'config.yaml file syntax error on line: {mark.line + 1 }'
                )
        else:
            logging.exception('Yaml syntax error')
        raise e


def load_config():
    path = Path(__file__).parent.resolve()/'config.yaml'
    config = load_file(path)
    verify_data(config)
    return config
