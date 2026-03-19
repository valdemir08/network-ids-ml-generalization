import pandas as pd
from src.configs.paths import PROCESSED_DATA_DIR


def visualizar_dataset(dataset_path, n=10):
    """
   informações básicas
    """
    print("=" * 60)
    print(f"DATASET: {dataset_path}")
    print("=" * 60)


    df = pd.read_parquet(dataset_path)

    print(f"\nTotal de registros: {len(df)}")
    print(f"Total de colunas: {len(df.columns)}")


    print(f"\nColunas disponíveis:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")


    print(f"\nPrimeiros {n} registros:")
    print("-" * 60)
    print(df.head(n).to_string())


    if 'label' in df.columns:
        print("\n" + "=" * 60)
        print("DISTRIBUIÇÃO DE LABELS ")
        print("=" * 60)
        print(df['label'].value_counts(dropna=False))

    return df


if __name__ == "__main__":
    dataset_name = "cicids2017"
    scenario = "friday"

    labeled_dataset = f"{PROCESSED_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"

    df = visualizar_dataset(labeled_dataset, n=10)