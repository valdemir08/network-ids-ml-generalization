from src.configs.paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR
from src.io.io_utils import load_parquet, save_parquet
import pandas as pd
import numpy as np

FINAL_DATA_DIR = FINAL_DATA_DIR / "ml/"

DATASETS = {
    "cicids2017": FINAL_DATA_DIR / "cicids2017_sampled.parquet",
    "unsw_nb15": FINAL_DATA_DIR / "unsw_nb15_sampled.parquet",
    "iot23": FINAL_DATA_DIR / "iot23_sampled.parquet",
}



def setup_pandas():
    pd.set_option('display.max_columns', 100)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.options.display.float_format = '{:.2f}'.format


def load_dataset(dataset="cicids2017"):
    path = DATASETS[dataset]
    df = load_parquet(path)
    return df



