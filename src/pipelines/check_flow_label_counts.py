import pandas as pd

from src.configs.paths import INTERMEDIATE_DATA_DIR


def check_counts(dataset_name: str, scenario: str):

    flows_path = INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_flows.parquet"
    labels_path = INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_labels.parquet"

    print("\nLoading files...")

    flows_df = pd.read_parquet(flows_path)
    labels_df = pd.read_parquet(labels_path)

    # ---------- contagem ----------
    flows_count = len(flows_df)
    labels_count = len(labels_df)

    print("\n===== FLOWS =====")
    print("Total flows:", flows_count)

    print("\n===== LABELS =====")
    print("Total label records:", labels_count)

    # ---------- distribuição ----------
    print("\n===== LABEL DISTRIBUTION =====")

    if "Label" in labels_df.columns:
        print(labels_df["Label"].value_counts())
    elif " Label" in labels_df.columns:
        print(labels_df[" Label"].value_counts())
    else:
        print("Coluna de label não encontrada")

    # ---------- comparação ----------
    print("\n===== COMPARISON =====")

    ratio = flows_count / labels_count
    print("Flows / Labels ratio:", round(ratio, 3))
###########################################
    print("\n===== FLOW COLUMNS =====")
    print(flows_df.columns.tolist())

    print("\n===== LABEL COLUMNS =====")
    print(labels_df.columns.tolist())

"""
    print("\n===== IP CHECK =====")

    flow_ips = set(flows_df["src_ip"]).union(set(flows_df["dst_ip"]))

    label_ips = set(labels_df[" Source IP"]).union(set(labels_df[" Destination IP"]))

    print("Unique IPs in flows:", len(flow_ips))
    print("Unique IPs in labels:", len(label_ips))
"""

if __name__ == "__main__":

    check_counts(
        dataset_name="cicids2017",
        scenario="friday"
    )