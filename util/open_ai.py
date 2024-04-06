from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()


def open_ai() -> str:
    try:
        print("Connecting to OpenAI")
        client = OpenAI(api_key=f"{os.environ.get('OPENAI_API_KEY1')}")
        print("Connection to OpenAI is successful")
        return client

    except Exception as e:
        print("Connection to OpenAI failed")
        raise
