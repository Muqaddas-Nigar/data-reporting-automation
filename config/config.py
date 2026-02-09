import yaml
from pathlib import Path
import logging


def verify_data(config):
    key = 'database'
    try:
        config[key]
    except KeyError as e:
        logging.exception(
            f'"{key}" missing in config.yaml'
            )
        raise e



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
            raise e


def load_config():
    path = Path(__file__).parent.resolve()/'config.yaml'
    config = load_file(path)
    verify_data(config)
    return config