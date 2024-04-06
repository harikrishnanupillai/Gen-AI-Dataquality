from util.ask_chat_gpt import ask_open_ai

def referential_integrity_check(
    child_table_name: str, client, reference_table, child_column_name, parent_column_name
) -> str:
    # session = snowflake_connection()

    pattern_sql = ask_open_ai(
        client,
        f"""please generate a snowflake sql select statement to perform referential integrity check in child table {child_table_name} with key column {child_column_name} in parent table {reference_table} for column {parent_column_name}, and please do not add semicolon(;) in the query end and also provide some explaining before giving the query code and after the code as well""",
    )

    return pattern_sql
