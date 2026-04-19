from src.configs.paths import PROCESSED_DATA_DIR, FINAL_DATA_DIR
from src.io.io_utils import load_parquet, save_parquet
import pandas as pd



def merge_processed_dataset(dataset_name):
    processed_dir = PROCESSED_DATA_DIR / dataset_name
    output_dir = FINAL_DATA_DIR / "single"
    # mapear melhoria: todas as saídas devem criar o diretório em caso de inxistência
    # em caso de clone, já evita erros em máquinas que não tenham os dirtórios
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{dataset_name}.parquet"

    files = [f for f in processed_dir.iterdir()]
    # caso seja executado antes das outras etapas (não haverá insumo para gerar o final)
    if not files:
        raise ValueError(f"Nenhum arquivo encontrado em {processed_dir}")

    dfs = []

    for file in files:
        df = load_parquet(file)
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)
    save_parquet(df_final, output_file)


if __name__ == "__main__":
    merge_processed_dataset("cicids2017")
