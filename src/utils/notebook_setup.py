from src.configs.paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR
from src.io.io_utils import load_parquet, save_parquet
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


#realocar para outro arquivo
def add_custom_features(df):
    df = df.copy()

    total_packets = df["bidirectional_packets"]
    total_bytes = df["bidirectional_bytes"]
    duration_ms = df["bidirectional_duration_ms"]

    eps = 1e-6  # evitar divisão por zero

    df["bytes_per_packet"] = total_bytes / (total_packets + eps)
    df["flow_symmetry_score"] = (df["src2dst_packets"] - df["dst2src_packets"]).abs() / (total_packets + eps)
    df["packet_size_variation"] = df["bidirectional_stddev_ps"] / (df["bidirectional_mean_ps"] + eps)
    df["piat_variation"] = df["bidirectional_stddev_piat_ms"] / (df["bidirectional_mean_piat_ms"] + eps)

    std_piat = df["bidirectional_stddev_piat_ms"]
    mean_piat = df["bidirectional_mean_piat_ms"]
    df["burstiness"] = (std_piat - mean_piat) / (std_piat + mean_piat + eps)

    ppms = total_packets / (duration_ms + eps)
    df["packets_per_ms_log"] = np.log1p(ppms)

    df["bytes_per_ms"] = total_bytes / (duration_ms + eps)

    return df


def load_dataset(path = path_dataset, custom_features = False):
    df = load_parquet(path)
    if custom_features:
        df = add_custom_features(df)
    return df



