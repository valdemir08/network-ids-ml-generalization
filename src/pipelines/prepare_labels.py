#merge e limpeza de csvs com labels de ataque

from src.configs.datasets import DATASETS
from src.data_processing.dataset_utils import (
    dataset_cleanup,
    convert_to_datetime,
    create_column_ts_ms,
    padronize_cols_name,
    adjust_time_for_cic_datasets,
    apply_timezone_offset,
    create_column_dataset_name,
)
from src.io.io_utils import save_parquet
from src.configs.paths import PROJECT_ROOT, INTERMEDIATE_DATA_DIR
from src.configs.column_mappings import CICIDS_MAPPING

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
    df = padronize_cols_name(df, CICIDS_MAPPING)
    #
    df = convert_to_datetime(df)
    #adicionar condicionais para essa função. só executa em bases cic
    df = adjust_time_for_cic_datasets(df,"Timestamp")
    df = apply_timezone_offset(df, "Timestamp", 3)
    df = create_column_ts_ms(df)
    #ideal: criar novo arquivo de configurações para adicionar vars como timestamp, e fuso
    #estudar também uma possibilidade de melhoria no nome da coluna, outros datasets não terão o mesmo nome

    save_parquet(df, output_path)

    print(df.shape)
    print(df.columns)
    print(df.head())


if __name__ == "__main__":

    prepare_labels(dataset_name = "cicids2017", scenario = "friday")

