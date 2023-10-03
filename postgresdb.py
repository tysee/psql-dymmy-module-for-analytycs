import yaml
import psycopg2
import pandas as pd
import queue
import threading
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PostgresDB:
    def __init__(self, dbconfig, db, db_required_keys):
        self.db_params = self.get_db_params(dbconfig, db, db_required_keys)

    def __enter__(self):
        self.conn = self.connect_to_db()
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    @staticmethod
    def get_db_params(dbconfig, server, db_required_keys):
        try:
            with open(dbconfig, 'r') as f:
                config = yaml.safe_load(f)

            if server not in config:
                raise ValueError(f"Key '{server}' not found in the configuration.")

            db_params = config[server]

            for key in db_required_keys:
                if key not in db_params:
                    raise ValueError(f"Key '{key}' not found.")

            return db_params

        except FileNotFoundError:
            raise FileNotFoundError("dbconfig.yaml file not found.")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing the YAML file: {e}")

    @staticmethod
    def read_csv_for_db(file_path, delimiter=',', encoding='utf-8'):

        type_mapping = {
            'int32': 'BIGINT',
            'int64': 'BIGINT',
            'float32': 'DOUBLE PRECISION',
            'float64': 'DOUBLE PRECISION',
            'object': 'VARCHAR(255)',
            'str': 'VARCHAR(255)',
        }
        try:
            logger.info(f"Reading CSV file {file_path}...")

            df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
            df = df.fillna(np.nan).replace([np.nan], [None])

            column_types = df.dtypes

            table_structure = {}
            for col, data_type in column_types.items():
                sql_data_type = type_mapping.get(str(data_type), 'UNKNOWN')
                table_structure[col] = sql_data_type

            table_structure_json = table_structure
            logger.info(f"CSV file read successfully. {len(df)} rows read.")

            return df, table_structure_json

        except FileNotFoundError:
            logger.error(f"File {file_path} not found.")

        except Exception as e:
            logger.error(f"Error reading the file {file_path}: {e}")

    def connect_to_db(self):
        return psycopg2.connect(**self.db_params)

    def delete_data_from_table(self, schema_name, table_name):
        with self as m:
            try:
                query = f"DELETE FROM {schema_name}.{table_name}"
                m.execute(query)
                logger.info(f"Data deleted from table {schema_name}.{table_name} successfully.")
            except Exception as e:
                logger.error(f"Error deleting data from table {schema_name}.{table_name}: {e}")

    def create_table(self, schema_name, table_name, table_structure):
        with self as m:
            try:
                columns_definition = ", ".join(
                    f"{column} {definition}" for column, definition in table_structure.items())
                query = f'''
                CREATE TABLE {schema_name}.{table_name} (
                    {columns_definition}
                )
                '''
                m.execute(query)
                logger.info(f"Table {schema_name}.{table_name} created successfully.")
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info(f"Table {schema_name}.{table_name} already exists!")
                else:
                    logger.error(f"Error creating table {schema_name}.{table_name}: {e}")

    def simple_insert_data_to_db(self, df, batch_size, schema_name, table_name):
        with self as cursor:
            logger.info("Starting insertion of data...")
            columns = ', '.join(df.columns)
            values_placeholder = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values_placeholder})"

            batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]

            for batch in batches:
                records = [tuple(x) for x in batch.values]
                cursor.executemany(insert_query, records)

            logger.info("Data inserted successfully.")

    def insert_worker(self, q, schema_name, table_name):
        try:
            conn = self.connect_to_db()
            cursor = conn.cursor()

            while True:
                batch = q.get()
                if batch is None:
                    q.task_done()
                    break
                columns = ', '.join(batch.columns)
                values_placeholder = ', '.join(['%s'] * len(batch.columns))
                insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values_placeholder})"
                records = [tuple(x) for x in batch.values]
                cursor.executemany(insert_query, records)
                conn.commit()
                q.task_done()

            cursor.close()
            conn.close()
            logger.info(f"Worker {threading.current_thread().name} has exited.")
        except Exception as e:
            logger.error(f"Error in worker thread: {e}")
            q.task_done()

    def parallel_insert_data_to_db(self, df, batch_size, schema_name, table_name, num_threads=4):
        try:
            logger.info("Starting parallel insertion of data...")
            q = queue.Queue(maxsize=num_threads * 2)

            threads = []
            for _ in range(num_threads):
                t = threading.Thread(target=self.insert_worker,
                                     args=(q, schema_name, table_name))
                t.start()
                threads.append(t)
                logger.info(f"Thread {t.name} started.")

            batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]
            for batch in batches:
                q.put(batch)

            for _ in range(num_threads):
                q.put(None)

            q.join()
            logger.info("All batches processed.")

            return "Data inserted successfully in parallel"
        except Exception as e:
            logger.error(f"Error in parallel_insert_data_to_db: {e}")
            return f"Error: {e}"
