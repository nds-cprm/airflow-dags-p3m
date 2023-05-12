import os


def create_get_dir(temp_dir):
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
