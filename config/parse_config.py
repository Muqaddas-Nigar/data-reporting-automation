from pathlib import Path
import logging


class Config:
    def __init__(self, config):
        self.raw_table = config['database']['raw_table']
        if self.raw_table is None:
            self.raw_table = 'ingested_data'
        self.db_path = Path(config['database']['path']) if config['database']['path'] else None
        if self.db_path is None:
            self.db_path = Path(__file__).parent.parent.resolve()/'data/master.db'
        elif not self.db_path.parent.exists():
            logging.error('Invalid path for Database.')
            raise ValueError
        self.clean_table = config['database']['clean_table']
        if self.clean_table is None:
            self.clean_table = 'clean_data'
        if config['raw_files'] == 'default' or config['raw_files'] is None:
            self.raw_files_loc = Path(__file__).parent.parent/'data/unprocessed'
        else:
            self.raw_files_loc = config['raw_files']
            if not Path(self.raw_files_loc).exists():
                logging.error('Invalid path for data files.')
                raise ValueError
            self.raw_files_loc = Path(self.raw_files_loc) 
        if config['processed_files'] == 'default' or config['processed_files'] is None:
            self.clean_files_loc = Path(__file__).parent.parent/'data/processed'
            self.clean_files_loc.mkdir(parents=True, exist_ok=True)
        else:
            self.clean_files_loc = config['processed_files']
            if not Path(self.clean_files_loc).exists():
                logging.error('Invalid path for processed files folder')
                raise ValueError
            self.clean_files_loc = Path(self.clean_files_loc)
        self.table_name = ''
        self.clean_table_columns = config['columns'][:]
        self.raw_table_columns = config['columns'][:]
        self.raw_table_columns.extend(['status', 'source', 'timestamp'])
        self.ingestion_table_name = 'ingestion_log'
        self.ingestion_table_columns = ['file_name', 'file_fingerprint', 'ingestion_time']
        self.data_columns = config['columns']
        self.column_container = []

