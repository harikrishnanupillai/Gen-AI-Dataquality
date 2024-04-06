def ask_open_ai(client, prompt):
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo", messages=[{"role": "user", "content": f"""{prompt}"""}]
    )
    message = completion.choices[0].message.content

    start_index = str(message).find("```")

    end_index = str(message).find("```", start_index + 3)

    code_text = message[start_index + 3 : end_index]

    return code_text.replace("sql", "")
