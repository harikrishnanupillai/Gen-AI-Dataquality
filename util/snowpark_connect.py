from snowflake.snowpark import Session
from os import environ
from dotenv import load_dotenv

load_dotenv()


def snowflake_connection():
    connection_parameters = {
        "account": f"{environ.get('snowflake_account')}",
        "user": f"{environ.get('snowflake_username')}",
        "password": f"{environ.get('snowflake_password')}",
        "role": f"{environ.get('snowflake_role')}",
        "warehouse": f"{environ.get('snowflake_warehouse')}",
        "database": f"{environ.get('snowflake_database')}",
        "schema": f"{environ.get('snowflake_schema')}",
    }
    try:
        print("Connecting to Snowflake")
        session = Session.builder.configs(connection_parameters).create()
        print("Connection to snowflake is successful")

    except Exception as e:
        print("connection failed")
        raise

    return session
