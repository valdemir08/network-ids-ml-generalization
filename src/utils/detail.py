
import pandas as pd
from src.io.io_utils import *

def detail_dataset(path):
    df = load_parquet(path)
    print(f"{len(df)} Registros")
    print(df.value_counts("label"))


