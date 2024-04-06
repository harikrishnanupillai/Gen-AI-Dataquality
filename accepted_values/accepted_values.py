from util.ask_chat_gpt import ask_open_ai


def accepted_values_check(
    table_name: str, client, column_name, accepted_values_list
) -> str:
    # session = snowflake_connection()

    pattern_sql = ask_open_ai(
        client,
        f"""please generate a snowflake sql select statement to find rows in table {table_name} for column {column_name} which are not matching any one of the accepted values: '{accepted_values_list}' and please do not add semicolon(;) in the query end and also provide some explaining before giving the query code and after the code as well""",
    )

    return pattern_sql
