from util.snowpark_connect import snowflake_connection
from util.ask_chat_gpt import ask_open_ai
import json


def generate_hash_value(table_name: str, clinet, session) -> str:
    # session = snowflake_connection()
    columns = session.sql(
        f"select ARRAY_AGG(OBJECT_CONSTRUCT(*)) from(SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns where table_name = '{table_name}' order by ordinal_position);"
    ).collect()[0][0]
    result_dict = {}
    columns = json.loads(columns)

    for column_info in columns:
        result_dict[column_info["COLUMN_NAME"]] = column_info["DATA_TYPE"]
    NUM = []
    TEX = []

    for key, value in result_dict.items():
        if value == "NUMBER":
            NUM.append(key)
        else:
            TEX.append(key)
    # print(NUM, TEX)
    hash_value_sql = ask_open_ai(
        clinet,
        f"""please generate a sql select statement by making sum of numeric colums and count of non numeric from table {table_name.lower()}
        numeric columns are {NUM} and non numeric columns are {TEX}  and finally make a full sum all of these values and put it in md5() function as md5(value)
        SQL statement doesn't require to find if they are numeric just a select statement without any conditions and also please provide some explanation before giving the code block and after giving the code block""",
    )
    return hash_value_sql
