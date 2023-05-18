import os


def create_get_dir(temp_dir):
    return os.makedirs(temp_dir, exist_ok=True)
