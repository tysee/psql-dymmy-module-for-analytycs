### How to use
1) Create a dbconfig.yaml file in the same directory as the script
2) Fill in the following fields in the dbconfig.yaml file:
    ```yaml
    test-server:
      database: postgres
      user: postgres
      password: Qwerty
      host: localhost
      port: 5432
    ```
3) Run the needed methods from postgresdb.py:
    ```python
    from postgresdb import PostgresDB

    dbconfig = "dbconfig.yaml"
    server = "test-server"
    db_required_keys = ['host', 'port', 'user', 'password', 'database']
    schema_name = "test"
    table_name = "csv"
    csv = "test.csv"

    pdb = PostgresDB(dbconfig, server, db_required_keys)

    data, table_structure = pdb.read_csv_for_db(csv)
    pdb.create_table(schema_name, table_name, table_structure)
    pdb.insert_data_to_db(data, 100000, schema_name, table_name)
    ```