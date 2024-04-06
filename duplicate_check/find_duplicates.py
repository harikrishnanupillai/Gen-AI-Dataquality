from util.snowpark_connect import snowflake_connection
from util.ask_chat_gpt import ask_open_ai
import json


def find_duplicate_values(table_name: str, session, client) -> tuple[str, str]:
    # session = snowflake_connection()
    session.sql(f"SHOW PRIMARY KEYS").collect()

    primary_key_list = session.sql(
        f"""SELECT array_agg("column_name") FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "table_name" = '{table_name}';"""
    ).collect()[0][0]

    duplicates_sql = ask_open_ai(
        client,
        f"""please generate a snowflake sql select statement to find duplicate records in table {table_name} with key column(s) {primary_key_list} in such way it has return duplicate records if any otherwise it has to retun no records, and please do not add semicolon in the query end and also provide some explaining before giving the query code and after the code as well""",
    )

    return primary_key_list, duplicates_sql
