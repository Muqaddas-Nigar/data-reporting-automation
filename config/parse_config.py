from pathlib import Path
import logging


class PathError(Exception):
    pass


class MissingColumnsData(Exception):
    pass


class Config:
    def __init__(self, config):
        self.raw_table = config['database']['raw_table']
        if self.raw_table is None:
            self.raw_table = 'unprocessed'
        self.db_path = Path(config['database']['path']) if config['database']['path'] else None
        if self.db_path is None:
            self.db_path = Path(__file__).parent.parent.resolve()/'data/master.db'
        elif not self.db_path.parent.exists():
            logging.error('Invalid path for Database.')
            raise PathError
        self.clean_table = config['database']['clean_table']
        if self.clean_table is None:
            self.clean_table = 'processed'
        if config['raw_files'] == 'default' or config['raw_files'] is None:
            self.raw_files_loc = Path(__file__).parent.parent/'data/unprocessed'
        else:
            self.raw_files_loc = config['raw_files']
            if not Path(self.raw_files_loc).exists():
                logging.error('Invalid path for data files.')
                raise PathError('Error occured check log file')
        if config['processed_files'] == 'default' or config['processed_files'] is None:
            self.clean_files_loc = Path(__file__).parent.parent/'data/processed'
        else:
            self.clean_files_loc = config['processed_files']
            if not Path(self.clean_files_loc).exists():
                logging.error('Invalid path for processed files folder')
                raise PathError('Error occured check log file')
        self.table_name = ''
        self.clean_table_columns = config['columns'][:]
        self.raw_table_columns = config['columns'][:]
        self.raw_table_columns.extend(['status', 'source', 'timestamp'])
        self.data_columns = config['columns']
        self.column_container = []

        if self.data_columns is None:
            logging.error('Missing column names in config.yaml')
            raise MissingColumnsData('Error occured check log file')
