import logging
from pathlib import Path
from config.config import load_config
from data.db import create_db, load_table, store_data
from ingestion.ingestion import ingest_data
from cleaning.cleaner import clean_data


class Config:
    def __init__(self, config):
        self.raw_table = config['database']['raw_table']
        self.db_path = Path(config['database']['path']) if config['database']['path'] else None
        self.clean_table = config['database']['clean_table']
        self.columns = config['columns']
        if config['raw_files'] == 'default' or config['raw_files'] is None:
            self.raw_files_loc = Path(__file__).parent/'data/unprocessed'
        else:
            self.raw_files_loc = config['raw_files']
        if config['processed_files'] == 'default' or config['processed_files'] is None:
            self.clean_files_loc = Path(__file__).parent/'data/processed'
        else:
            self.clean_files_loc = config['processed_files']
        self.default_db_path = Path(__file__).parent.resolve()/'data/master.db'



def set_logger():
    log_folder = Path(__file__).parent.resolve()/'logs'
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
    set_logger()
    config = load_config()
    config = Config(config)
    config = create_db(config)
    ingest_data(config)
    #data = load_table(config['database']['raw_table'], config['database']['path'])
    #data = clean_data(data)
    #store_data(data, config['database']['clean_table'], config['database']['path'])
    #data = load_table(config['database']['clean_table'], config['database']['path'])
    #print(data.head())




if __name__ == "__main__":
    main()
