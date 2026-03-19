import pandas as pd
from pathlib import Path
from src.io.io_utils import load_parquet, save_parquet
from src.flows.flow_matching import create_flow_key,create_bidirectional_flow_key,match_flows
from src.configs.paths import INTERMEDIATE_DATA_DIR, PROCESSED_DATA_DIR


def build_dataset(flows_file, labels_file, output_file, time_tolerance=10000):
    print("Carregando flows...")
    flows = load_parquet(flows_file)
    print(f"Flows carregados: {len(flows)} registros")
    print(f"Colunas disponíveis: {flows.columns.tolist()}")

    print("Carregando labels...")
    labels = load_parquet(labels_file)
    print(f"Labels carregadas: {len(labels)} registros")
    print(f"Colunas disponíveis: {labels.columns.tolist()}")

    print("Criando chaves para matching...")

    labels = create_flow_key(labels)

    labels = create_bidirectional_flow_key(labels)

    flows = create_flow_key(flows)

    flows = create_bidirectional_flow_key(flows)

    print("linkando flows com labels...")

    merged = match_flows(flows, labels, time_tolerance)

    print(f"Salvando resultado em {output_file}...")
    merged.to_parquet(output_file, index=False)

    # estatísticas finais
    matched = merged['label'].notna().sum()
    print(f"Total de flows com label: {matched} ({matched / len(merged) * 100:.2f}%)")
    print("Concluído!")


if __name__ == "__main__":
    dataset_name = "cicids2017"
    scenario = "friday"

    flows_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_flows.parquet"
    labels_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"
    output_file = f"{PROCESSED_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"

    build_dataset(flows_file, labels_file, output_file, time_tolerance=120000)