from datetime import datetime


def generate_filename(endpoint):
    """Generate a string filename in format '{today's date}/{endpoint}'"""
    return f'{datetime.now().strftime("%Y-%m-%d")}/{endpoint}'
