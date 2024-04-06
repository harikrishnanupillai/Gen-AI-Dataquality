from util.ask_chat_gpt import ask_open_ai
from util.snowpark_connect import snowflake_connection


def replace_null_values(table_name: str, primary_keys: str, session, client) -> str:
    # session = snowflake_connection()
    columns = session.sql(
        f"SELECT ARRAY_AGG(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    ).collect()[0][0]
    remove_duplicates_sql = ask_open_ai(
        client,
        f"""please generate a snowflake sql statement to replace missing values in key column(s) in table {table_name} in key column(s) {primary_keys} it should replace only missing values with '0' in provided key columns, and please provide some explaination before and after the code block""",
    )
    return remove_duplicates_sql
