import yaml
import psycopg2
import pandas as pd
import json


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
    def read_csv_for_db(file_path, delimiter=',', encoding='utf-8', na_values=None):

        type_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'object': 'VARCHAR(255)',
            'str': 'VARCHAR(255)',
        }
        try:
            df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, na_values=na_values)
            df = df.applymap(lambda x: None if pd.isna(x) else x)

            column_types = df.dtypes

            table_structure = {}
            for col, data_type in column_types.items():
                sql_data_type = type_mapping.get(str(data_type), 'UNKNOWN')
                table_structure[col] = sql_data_type

            table_structure_json = json.dumps(table_structure, indent=4)

            return df, table_structure_json

        except FileNotFoundError:
            raise FileNotFoundError(f"File {file_path} not found.")

        except Exception as e:
            raise ValueError(f"Error reading the file {file_path}: {e}")

    def connect_to_db(self):
        return psycopg2.connect(**self.db_params)

    def create_table(self, schema_name, table_name, table_structure):
        with self as m:
            check_query = (f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='{schema_name}' "
                           f"AND table_name='{table_name}')")
            m.execute(check_query)
            table_exists = m.fetchone()[0]

            if table_exists:
                raise ValueError(f"Table {schema_name}.{table_name} already exists!")
            
            columns_definition = ", ".join(f"{column} {definition}" for column, definition in table_structure.items())

            query = f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                {columns_definition}
            )
            '''
            m.execute(query)
            return "Table created successfully"

    def insert_data_to_db(self, df, batch_size, schema_name, table_name):
        with self as cursor:
            columns = ', '.join(df.columns)
            values_placeholder = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values_placeholder})"

            batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]

            for batch in batches:
                records = [tuple(x) for x in batch.values]
                cursor.executemany(insert_query, records)

            return "Data inserted successfully"