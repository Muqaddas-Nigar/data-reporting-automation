import logging
from pathlib import Path
from datetime import datetime
from config.config import load_config

def set_logger():
    log_folder = Path(__file__).parent.resolve()/'logging'
    log_file = log_folder/'log.log'
    log_folder.mkdir(parents=True, exist_ok=True)
    log_file.touch(exist_ok=True)
    logging.basicConfig(
        filename=log_file,
        filemode='a',
        level=logging.DEBUG,
        style='{',
        format="{asctime} -- {levelname} -- {message}"
    )
    logging.info('Logging configured')


def main():
    try:
        set_logger()
        config = load_config()
        create_database('database')
    except Exception:
        print('Error occured check log file.')




if __name__ == "__main__":
    main()
