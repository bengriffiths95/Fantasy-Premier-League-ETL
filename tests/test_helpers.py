from datetime import datetime
from scripts.helpers import generate_filename


class TestGenerateFileName:
    def test_function_returns_correct_filename(self):
        endpoint = "test"
        current_timestamp = datetime.now().strftime("%Y-%m-%d")
        output = generate_filename(endpoint)
        assert output == f"{current_timestamp}/{endpoint}"
