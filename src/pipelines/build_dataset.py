import pandas as pd
from pathlib import Path
from src.io.io_utils import load_parquet, save_parquet
from src.flows.flow_matching import (
    create_label_flow_key,
    create_nfstream_flow_key,
    create_bidirectional_flow_key,
    match_flows
)
from src.configs.paths import INTERMEDIATE_DATA_DIR, PROCESSED_DATA_DIR

def build_dataset(flows_file: str, labels_file: str, output_file: str, time_tolerance: int = 2000):
    """
    Cria um dataset unindo os flows do NFStream com as labels do CICIDS2017
    e salva em Parquet.
    """
    # 1. Carregar dados
    print("Carregando flows...")
    flows = load_parquet(flows_file)
    print(f"Flows carregados: {len(flows)} registros")

    print("Carregando labels...")
    labels = pd.read_parquet(labels_file) if labels_file.endswith(".parquet") else pd.read_csv(labels_file)
    print(f"Labels carregadas: {len(labels)} registros")

    # 2. Criar flow_key e flow_key_rev
    labels = create_label_flow_key(labels)
    labels = create_bidirectional_flow_key(
        labels,
        src_ip_col="Source IP",
        dst_ip_col="Destination IP",
        src_port_col="Source Port",
        dst_port_col="Destination Port",
        proto_col="Protocol"
    )
    flows = create_nfstream_flow_key(flows)
    flows = create_bidirectional_flow_key(
        flows,
        src_ip_col="src_ip",
        dst_ip_col="dst_ip",
        src_port_col="src_port",
        dst_port_col="dst_port",
        proto_col="protocol"
    )

    # 3. Casar flows com labels
    print("Casando flows com labels...")
    merged = match_flows(flows, labels, time_tolerance=time_tolerance)
    matched_count = merged['Label'].notna().sum()
    print(f"Flows com labels: {matched_count} / {len(merged)}")

    # 4. Salvar dataset final
    print(f"Salvando dataset final em {output_file} ...")
    save_parquet(merged, output_file)
    print("Concluído!")


if __name__ == "__main__":
    dataset_name = "cicids2017"
    scenario = "friday"

    flows_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_flows.parquet"
    labels_file = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"
    output_file = f"{PROCESSED_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"

    build_dataset(flows_file, labels_file, output_file, time_tolerance=10000)