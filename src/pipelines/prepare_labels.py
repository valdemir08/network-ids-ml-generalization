#merge e limpeza de csvs com labels de ataque

from src.configs.datasets import DATASETS
from src.data_processing.dataset_utils import dataset_cleanup, convert_labels_timestamp
from src.io.io_utils import save_parquet
from src.configs.paths import PROJECT_ROOT, INTERMEDIATE_DATA_DIR

import pandas as pd

PROJECT_ROOT = PROJECT_ROOT

def load_and_concat_csvs(dataset_name, scenario):

    dataset_cfg = DATASETS[dataset_name]
    scenario_cfg = dataset_cfg["scenarios"][scenario]

    root = dataset_cfg["root"]
    label_dir = root / dataset_cfg["label_dir"]

    csvs = []

    for csv_file in scenario_cfg["labels"]:
        path = label_dir / csv_file
        print("Carregando:", path)
        csvs.append(pd.read_csv(path))

    return pd.concat(csvs, ignore_index=True)


def prepare_labels(dataset_name, scenario):

    output_path = (
            INTERMEDIATE_DATA_DIR
            / dataset_name
            / f"{scenario}_labels.parquet"
    )

    df = load_and_concat_csvs(dataset_name, scenario)

    df = dataset_cleanup(df)
    df = convert_labels_timestamp(df)


    save_parquet(df, output_path)

    print(df.shape)
    print(df.columns)
    print(df.head())


if __name__ == "__main__":

    prepare_labels(dataset_name = "cicids2017", scenario = "friday")

