import json
from collections import Counter

import numpy as np
import pandas as pd

from src.configs.datasets import DATASETS
from src.configs.paths import INTERMEDIATE_DATA_DIR, PROCESSED_DATA_DIR
from src.data_processing.dataset_utils import create_column_dataset_name
from src.flows.flow_matching import (
    MATCH_RESULT_COLUMNS,
    create_bidirectional_flow_key,
    create_flow_key,
    match_flows_for_dataset,
)
from src.io.io_utils import load_parquet, save_parquet


FLOW_MATCH_COLUMNS = [
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "bidirectional_first_seen_ms",
    "bidirectional_last_seen_ms",
]

TIME_TOLERANCES = {
    "10s": 10_000,
    "30s": 30_000,
    "1min": 60_000,
    "2min": 120_000,
    "3min": 180_000,
    "5min": 300_000,
}


def _prepare_keys(df):
    return create_bidirectional_flow_key(create_flow_key(df))


def _count_match_statuses(matched):
    return Counter(
        {
            str(status): int(count)
            for status, count in matched["match_status"].value_counts().items()
        }
    )


def _save_matching_summary(
    dataset_name,
    scenario,
    extracted_count,
    labeled_count,
    status_counts,
):
    dataset_dir = INTERMEDIATE_DATA_DIR / dataset_name
    dataset_dir.mkdir(parents=True, exist_ok=True)
    output_path = dataset_dir / "matching_summary.json"

    summaries = {}
    if output_path.exists():
        summaries = json.loads(output_path.read_text(encoding="utf-8"))

    summaries[scenario] = {
        "extracted": int(extracted_count),
        "labeled": int(labeled_count),
        "statuses": dict(status_counts),
    }

    temporary_path = output_path.with_suffix(".json.tmp")
    temporary_path.write_text(
        json.dumps(summaries, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(output_path)


def build_dataset(
    flows_file,
    labels_file,
    output_file,
    dataset_name,
    scenario,
    time_tolerance,
):
    """Processa o modo tradicional: um Parquet de fluxos por cenário."""
    flows = load_parquet(flows_file)
    labels = load_parquet(labels_file)
    print(f"Flows carregados: {len(flows)} registros")
    print(f"Labels carregadas: {len(labels)} registros")

    print("Criando chaves para matching...")
    labels = _prepare_keys(labels)
    flows = _prepare_keys(flows)

    print("Linkando flows com labels...")
    merged = match_flows_for_dataset(
        dataset_name,
        flows,
        labels,
        time_tolerance,
    )
    merged_clean = merged.dropna(subset=["label"]).copy()
    merged_clean = create_column_dataset_name(merged_clean, dataset_name)

    print(f"Salvando resultado em {output_file}...")
    save_parquet(merged_clean, output_file)

    matched = int(merged["label"].notna().sum())
    _save_matching_summary(
        dataset_name,
        scenario,
        len(merged),
        matched,
        _count_match_statuses(merged),
    )
    match_rate = matched / len(merged) * 100 if len(merged) else 0
    print(f"Total de flows com label: {matched} ({match_rate:.2f}%)")
    print("Concluído!")
    return output_file


def _numeric_pcap_sort_key(path):
    prefix = path.stem.split("_")[0]
    return (0, int(prefix)) if prefix.isdigit() else (1, path.name)


def _per_pcap_flow_files(dataset_name, scenario):
    scenario_dir = INTERMEDIATE_DATA_DIR / dataset_name / scenario
    files = sorted(
        scenario_dir.glob("*_flows.parquet"),
        key=_numeric_pcap_sort_key,
    )
    if not files:
        raise ValueError(f"Nenhum Parquet de fluxo encontrado em {scenario_dir}")
    return files


def _load_overlapping_labels(labels_file, flows, time_tolerance):
    first_seen = int(flows["bidirectional_first_seen_ms"].min())
    last_seen = int(flows["bidirectional_last_seen_ms"].max())
    filters = [
        ("ts_ms", "<=", last_seen + time_tolerance),
        ("last_ts_ms", ">=", first_seen - time_tolerance),
    ]
    return pd.read_parquet(labels_file, filters=filters)


def _load_scenario_flow_index(flow_files):
    frames = []

    for source_file_id, path in enumerate(flow_files):
        frame = load_parquet(path, columns=FLOW_MATCH_COLUMNS)
        frame["_source_file_id"] = source_file_id
        frame["_source_row_id"] = np.arange(len(frame), dtype=np.int64)
        frames.append(frame)

    return pd.concat(frames, ignore_index=True)


def _save_per_pcap_results(
    matched_index,
    flow_files,
    output_dir,
    dataset_name,
):
    output_files = []
    result_columns = [*MATCH_RESULT_COLUMNS, "label_binary"]

    for source_file_id, flows_path in enumerate(flow_files):
        file_matches = matched_index[
            matched_index["_source_file_id"].eq(source_file_id)
        ].sort_values("_source_row_id")

        flows = load_parquet(flows_path)
        expected_rows = np.arange(len(flows), dtype=np.int64)
        actual_rows = file_matches["_source_row_id"].to_numpy(dtype=np.int64)
        if not np.array_equal(actual_rows, expected_rows):
            raise ValueError(
                f"Identidade das linhas não foi preservada para {flows_path}"
            )

        for column in result_columns:
            values = file_matches[column].to_numpy()
            if column == "match_time_diff_ms":
                flows[column] = pd.Series(
                    values,
                    index=flows.index,
                    dtype="float64",
                )
            else:
                flows[column] = pd.Series(
                    values,
                    index=flows.index,
                    dtype="string",
                )

        flows = flows.dropna(subset=["label"]).copy()
        flows = create_column_dataset_name(flows, dataset_name)

        output_name = flows_path.name.replace("_flows.parquet", ".parquet")
        output_path = output_dir / output_name
        save_parquet(flows, output_path)
        output_files.append(output_path)

    return output_files


def build_per_pcap_scenario(
    dataset_name,
    scenario,
    time_tolerance,
):
    """Faz um matching por cenário e preserva um resultado por PCAP."""
    flow_files = _per_pcap_flow_files(dataset_name, scenario)
    labels_file = (
        INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_labels.parquet"
    )
    output_dir = PROCESSED_DATA_DIR / dataset_name / scenario

    print(f"Carregando índice de {len(flow_files)} Parquets de fluxo...")
    flows = _load_scenario_flow_index(flow_files)
    labels = load_parquet(labels_file)
    print(f"Flows carregados: {len(flows)} registros")
    print(f"Labels carregadas: {len(labels)} registros")

    flows = _prepare_keys(flows)
    labels = _prepare_keys(labels)
    matched_index = match_flows_for_dataset(
        dataset_name,
        flows,
        labels,
        time_tolerance,
    )

    output_files = _save_per_pcap_results(
        matched_index,
        flow_files,
        output_dir,
        dataset_name,
    )
    matched_count = int(matched_index["label"].notna().sum())
    _save_matching_summary(
        dataset_name,
        scenario,
        len(matched_index),
        matched_count,
        _count_match_statuses(matched_index),
    )
    match_rate = matched_count / len(matched_index) * 100
    print(
        f"Cenário {scenario}: {matched_count}/{len(matched_index)} "
        f"fluxos rotulados ({match_rate:.2f}%)"
    )
    return output_files


def build_chunked_scenario(dataset_name, scenario, time_tolerance):
    """Rotula separadamente cada bloco temporal extraído do PCAP."""
    flow_files = _per_pcap_flow_files(dataset_name, scenario)
    labels_file = (
        INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_labels.parquet"
    )
    output_dir = PROCESSED_DATA_DIR / dataset_name / scenario
    output_files = []
    total_flows = 0
    total_matched = 0
    status_counts = Counter()

    for block_number, flows_file in enumerate(flow_files, start=1):
        flows = load_parquet(flows_file)
        labels = _load_overlapping_labels(
            labels_file,
            flows,
            time_tolerance,
        )
        flows = _prepare_keys(flows)
        labels = _prepare_keys(labels)
        matched = match_flows_for_dataset(
            dataset_name,
            flows,
            labels,
            time_tolerance,
        )
        matched_clean = matched.dropna(subset=["label"]).copy()
        matched_clean = create_column_dataset_name(
            matched_clean,
            dataset_name,
        )

        output_file = output_dir / f"{block_number:06d}.parquet"
        save_parquet(matched_clean, output_file)
        output_files.append(output_file)
        total_flows += len(matched)
        total_matched += len(matched_clean)
        status_counts.update(_count_match_statuses(matched))
        print(
            f"Bloco {block_number}/{len(flow_files)}: "
            f"{len(matched_clean)}/{len(matched)} fluxos rotulados"
        )

    _save_matching_summary(
        dataset_name,
        scenario,
        total_flows,
        total_matched,
        status_counts,
    )
    match_rate = total_matched / total_flows * 100 if total_flows else 0
    print(
        f"Cenário {scenario}: {total_matched}/{total_flows} "
        f"fluxos rotulados ({match_rate:.2f}%)"
    )
    return output_files


def merge_flows_and_labels(dataset_name, scenario):
    if dataset_name == "bot_iot":
        pass
        return None

    dataset_cfg = DATASETS[dataset_name]
    output_mode = dataset_cfg.get("flow_output_mode", "scenario")
    time_tolerance = dataset_cfg["matching_time_tolerance_ms"]

    if output_mode == "scenario":
        flows_file = (
            INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_flows.parquet"
        )
        labels_file = (
            INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_labels.parquet"
        )
        output_file = PROCESSED_DATA_DIR / dataset_name / f"{scenario}.parquet"
        return build_dataset(
            flows_file,
            labels_file,
            output_file,
            dataset_name,
            scenario,
            time_tolerance,
        )
    elif output_mode == "per_pcap":
        return build_per_pcap_scenario(
            dataset_name,
            scenario,
            time_tolerance,
        )
    elif output_mode == "chunked":
        return build_chunked_scenario(
            dataset_name,
            scenario,
            time_tolerance,
        )
    else:
        raise ValueError(
            f"Modo de saída inválido para {dataset_name}: {output_mode}"
        )


if __name__ == "__main__":
    merge_flows_and_labels("cicids2017", "monday")
