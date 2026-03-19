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
    print("\n===== COMPARAÇÃO=====")

    ratio = flows_count / labels_count
    print("Flows / Labels ratio:", round(ratio, 3))
###########################################
    print("\n===== FLOW COLS =====")
    print(flows_df.columns.tolist())

    print("\n===== LABEL COLS =====")
    print(labels_df.columns.tolist())

if __name__ == "__main__":

    check_counts(
        dataset_name="cicids2017",
        scenario="friday"
    )