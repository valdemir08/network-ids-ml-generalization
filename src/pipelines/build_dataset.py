import pandas as pd
from pathlib import Path
from src.io.io_utils import load_parquet, save_parquet
from src.flows.flow_matching import create_flow_key,create_bidirectional_flow_key,match_flows
from src.configs.paths import INTERMEDIATE_DATA_DIR, PROCESSED_DATA_DIR
from src.data_processing.dataset_utils import create_column_dataset_name


def build_dataset(flows_file, labels_file, output_file, dataset_name, time_tolerance):

    flows = load_parquet(flows_file)
    labels = load_parquet(labels_file)
    print(f"Flows carregados: {len(flows)} registros")
    print(f"Labels carregadas: {len(labels)} registros")

    print("Criando chaves para matching...")

    labels = create_flow_key(labels)

    labels = create_bidirectional_flow_key(labels)

    flows = create_flow_key(flows)

    flows = create_bidirectional_flow_key(flows)

    print("linkando flows com labels...")


    merged = match_flows(flows, labels, time_tolerance)

    # remover todos os nan
    merged_clean = merged.dropna(subset=["label"])
    merged_clean = create_column_dataset_name(merged_clean, dataset_name)


    print(f"Salvando resultado em {output_file}...")
    #merged.to_parquet(output_file, index=False)
    save_parquet(merged_clean, output_file)

    # estatísticas finais
    matched = merged['label'].notna().sum()
    print(f"Total de flows com label: {matched} ({matched / len(merged) * 100:.2f}%)")
    print("Concluído!")

TIME_TOLERANCES = {
    "10s": 10_000,
    "30s": 30_000,
    "1min": 60_000,
    "2min": 120_000,
    "3min": 180_000,
    "5min": 300_000,
}

def merge_flows_and_labels(dataset_name, scenario):
    flows_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_flows.parquet"
    labels_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"
    output_file = f"{PROCESSED_DATA_DIR}/{dataset_name}/{scenario}.parquet"

    build_dataset(flows_file, labels_file, output_file, dataset_name, time_tolerance=TIME_TOLERANCES["1min"])


if __name__ == "__main__":
    dataset_name = "cicids2017"
    scenario = "monday"
    #scenario = "friday"

    flows_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_flows.parquet"
    labels_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"
    output_file = f"{PROCESSED_DATA_DIR}/{dataset_name}/{scenario}.parquet"

    build_dataset(flows_file, labels_file, output_file, dataset_name, time_tolerance=TIME_TOLERANCES["1min"])

