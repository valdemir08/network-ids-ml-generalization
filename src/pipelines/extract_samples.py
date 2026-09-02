import duckdb

from src.configs.paths import FINAL_DATA_DIR
from src.io.io_utils import save_parquet


SOURCE_DIR = FINAL_DATA_DIR / "single"
OUTPUT_DIR = FINAL_DATA_DIR / "ml"

DATASETS = {
    "cicids2017": SOURCE_DIR / "cicids2017.parquet",
    "unsw_nb15": SOURCE_DIR / "unsw_nb15.parquet",
    "iot23": SOURCE_DIR / "iot23.parquet",
}

SAMPLE_SIZES = {
    "BENIGN": 269_132,
    "ATTACK": 67_283,
}

RANDOM_STATE = 1


def _sample_query(dataset_path, label_binary, sample_size):
    source_path = dataset_path.as_posix()

    return f"""
        SELECT *
        FROM (
            SELECT *
            FROM read_parquet('{source_path}')
            WHERE label_binary = '{label_binary}'
        )
        USING SAMPLE {sample_size} ROWS (reservoir, {RANDOM_STATE})
    """


def extract_sample(dataset_name, dataset_path):
    """cria amostra 80/20"""
    print(f"\nAmostrando {dataset_name}")

    with duckdb.connect() as connection:
        #thread = 1 para evitar problemas de aleatóriedade / semente
        connection.execute("SET threads = 1")
        sample = connection.execute(
            f"""
            {_sample_query(dataset_path, 'BENIGN', SAMPLE_SIZES['BENIGN'])}
            UNION ALL
            {_sample_query(dataset_path, 'ATTACK', SAMPLE_SIZES['ATTACK'])}
            """
        ).fetch_df()

    counts = sample["label_binary"].value_counts().to_dict()

    #comparar os 2 dicts diretamente
    if counts != SAMPLE_SIZES:
        raise RuntimeError(
            f"Amostra inválida para {dataset_name}: {counts}."
        )

    # embaralha todas as linhas antes de salvar. frac=1 garante pegar todas as linhaws
    # o union all junta em sequencia, por isso é interessante embaralhar
    sample = sample.sample(frac=1, random_state=RANDOM_STATE)

    output_path = OUTPUT_DIR / f"{dataset_name}_sampled.parquet"
    save_parquet(sample, output_path)

    print(f"Total: {len(sample):,}")
    print(sample["label_binary"].value_counts())
    print(f"Salvo em {output_path}")


def extract_all_samples():
    for dataset_name, dataset_path in DATASETS.items():
        extract_sample(dataset_name, dataset_path)


if __name__ == "__main__":
    extract_all_samples()
