from src.configs.paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR
from src.io.io_utils import load_parquet
import pandas as pd
import numpy as np


path_dataset = FINAL_DATA_DIR / "single/cicids2017.parquet"


def setup_pandas():
    pd.set_option('display.max_columns', 100)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.options.display.float_format = '{:.2f}'.format


def load_dataset(path = path_dataset):
    return load_parquet(path)