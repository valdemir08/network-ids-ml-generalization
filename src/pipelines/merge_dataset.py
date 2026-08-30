import pyarrow.parquet as pq

from src.configs.datasets import DATASETS
from src.configs.paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR


def merge_processed_dataset(dataset_name):
    if dataset_name == "bot_iot":
        pass
        return None

    processed_dir = PROCESSED_DATA_DIR / dataset_name
    output_dir = FINAL_DATA_DIR / "single"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{dataset_name}.parquet"

    files = sorted(
        path
        for path in processed_dir.rglob("*.parquet")
        if path.is_file()
    )
    if DATASETS[dataset_name].get("flow_output_mode") == "chunked":
        files = [path for path in files if path.parent != processed_dir]
    if not files:
        raise ValueError(f"Nenhum arquivo encontrado em {processed_dir}")

    temporary_output = output_file.with_suffix(".parquet.tmp")
    writer = None
    expected_schema = None
    source_row_count = 0

    try:
        for file in files:
            table = pq.read_table(file)
            if expected_schema is None:
                expected_schema = table.schema
                writer = pq.ParquetWriter(temporary_output, expected_schema)
            elif table.schema != expected_schema:
                raise ValueError(f"Esquema incompatível em {file}")

            writer.write_table(table)
            source_row_count += table.num_rows
    finally:
        if writer is not None:
            writer.close()

    final_row_count = pq.ParquetFile(temporary_output).metadata.num_rows
    if final_row_count != source_row_count:
        raise ValueError(
            "A contagem do arquivo unificado difere da soma dos arquivos de origem"
        )

    temporary_output.replace(output_file)
    print(f"Salvo em {output_file}")
    print(f"Arquivos unificados: {len(files)}")
    print(f"Registros unificados: {final_row_count}")
    return output_file


def merge_datasets(dataset_names: list[str], output_name: str):
    # A união de datasets diferentes será definida após a validação dos três.
    pass


if __name__ == "__main__":
    merge_processed_dataset("cicids2017")
