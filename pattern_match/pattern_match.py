from util.snowpark_connect import snowflake_connection
from util.ask_chat_gpt import ask_open_ai


def email_pattern_check(table_name: str, client, column_name, pattern) -> str:
    # session = snowflake_connection()

    pattern_sql = ask_open_ai(
        client,
        f"""please generate a snowflake sql select statement to find rows in table {table_name} for column {column_name} which are not matching the pattern '{pattern}' and please do not add semicolon(;) in the query end and also provide some explaining before giving the query code and after the code as well""",
    )

    return pattern_sql
