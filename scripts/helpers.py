from datetime import datetime


def generate_filename(endpoint):
    return f'{datetime.now().strftime("%Y-%m-%d")}/{endpoint}'
