from pathlib import Path
import pandas as pd


def save_parquet(df, path):

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(path)
    print(f"Salvo em {path}")

def load_parquet(path, columns=None):
    """
    carrega parquet de forma otimizada, caso columns seja fornecido
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"O arquivo {path} não existe")
    return pd.read_parquet(path, columns=columns)
