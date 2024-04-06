from util.ask_chat_gpt import ask_open_ai


def min_max_range_check(
    table_name: str, client, column_name, min_value, max_value
) -> str:
    # session = snowflake_connection()

    pattern_sql = ask_open_ai(
        client,
        f"""please generate a snowflake sql select statement to validate data to find rows in table {table_name} for column {column_name} value less than '{min_value}' OR more than '{max_value}' please do not add semicolon(;) in the query end and also provide some explaination before giving the query code and after the code as well""",
    )
    return pattern_sql
