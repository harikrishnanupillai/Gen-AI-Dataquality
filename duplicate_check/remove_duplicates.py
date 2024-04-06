from util.open_ai import open_ai
from util.ask_chat_gpt import ask_open_ai


def remove_duplicate_values(table_name: str, primary_keys: str, session, client) -> str:
    # session = snowflake_connection()
    columns = session.sql(
        f"SELECT ARRAY_AGG(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    ).collect()[0][0]
    remove_duplicates_sql = ask_open_ai(
        client,
        f"""please generate a snowflake sql statement to remove duplicated record(s) in table {table_name} with columns {columns} use min function on these columns and key column(s) {primary_keys} it should remove only duplicated record by keeping 1 record from the list of duplicates""",
    )
    return remove_duplicates_sql
