from collections import Counter

import pandas as pd

from src.configs.datasets import DATASETS
from src.configs.paths import (
    ARTIFACTS_DIR,
    FINAL_DATA_DIR,
    INTERMEDIATE_DATA_DIR,
)
from src.flows.flow_matching import (
    create_bidirectional_flow_key,
    create_binary_label,
    create_flow_key,
    match_flows_for_dataset,
)
from src.pipelines.build_dataset import FLOW_MATCH_COLUMNS, TIME_TOLERANCES
from src.utils.select_dataset import select_dataset


LABEL_KEY_COLUMNS = [
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "ts_ms",
    "label",
]


def _numeric_file_order(path):
    prefix = path.stem.split("_")[0]
    if prefix.isdigit():
        return 0, int(prefix)
    return 1, path.name


def _flow_files(dataset_name, scenario):
    dataset_cfg = DATASETS[dataset_name]
    output_mode = dataset_cfg.get("flow_output_mode", "scenario")
    dataset_dir = INTERMEDIATE_DATA_DIR / dataset_name

    if output_mode == "scenario":
        files = [dataset_dir / f"{scenario}_flows.parquet"]
    elif output_mode == "per_pcap":
        files = sorted(
            (dataset_dir / scenario).glob("*_flows.parquet"),
            key=_numeric_file_order,
        )
    else:
        raise ValueError(
            f"Modo de saída inválido para {dataset_name}: {output_mode}"
        )

    if not files or any(not path.exists() for path in files):
        raise FileNotFoundError(
            f"Fluxos intermediários ausentes para {dataset_name} {scenario}"
        )
    return files


def _load_flows(dataset_name, scenario):
    frames = [
        pd.read_parquet(path, columns=FLOW_MATCH_COLUMNS)
        for path in _flow_files(dataset_name, scenario)
    ]
    flows = pd.concat(frames, ignore_index=True)
    flows = create_flow_key(flows)
    return create_bidirectional_flow_key(flows)


def _load_labels(dataset_name, scenario):
    path = (
        INTERMEDIATE_DATA_DIR
        / dataset_name
        / f"{scenario}_labels.parquet"
    )
    columns = LABEL_KEY_COLUMNS.copy()
    if dataset_name == "unsw_nb15":
        columns.extend(["label_binary_source", "protocol_name"])

    labels = pd.read_parquet(path, columns=columns)
    labels = create_flow_key(labels)
    return create_bidirectional_flow_key(labels)


def _source_binary_labels(dataset_name, labels):
    if dataset_name == "unsw_nb15":
        return labels["label_binary_source"]
    return create_binary_label(labels["label"])


def _format_integer(value):
    return f"{int(value):,}".replace(",", ".")


def _format_percentage(value):
    return f"{value:.2f}".replace(".", ",") + "%"


def _format_decimal(value):
    return f"{value:.2f}".replace(".", ",")


def _percentage(count, total):
    if total == 0:
        return 0.0
    return count / total * 100


def _distribution_lines(title, counts):
    total = sum(counts.values())
    lines = [title]
    for label in ["BENIGN", "ATTACK"]:
        count = counts.get(label, 0)
        lines.append(
            f"  {label}: {_format_integer(count)} "
            f"({_format_percentage(_percentage(count, total))})"
        )
    return lines


def _difference_lines(source_counts, reconstructed_counts):
    source_total = sum(source_counts.values())
    reconstructed_total = sum(reconstructed_counts.values())
    lines = ["Diferença entre as distribuições:"]

    for label in ["BENIGN", "ATTACK"]:
        source_count = source_counts.get(label, 0)
        reconstructed_count = reconstructed_counts.get(label, 0)
        count_difference = reconstructed_count - source_count
        percentage_point_difference = (
            _percentage(reconstructed_count, reconstructed_total)
            - _percentage(source_count, source_total)
        )
        lines.append(
            f"  {label}: diferença de {_format_integer(count_difference)} "
            "registros; diferença de "
            f"{_format_decimal(percentage_point_difference)} "
            "pontos percentuais"
        )
    return lines


def generate_dataset_report(dataset_name):
    if dataset_name not in DATASETS:
        raise ValueError(f"Dataset não configurado: {dataset_name}")

    tolerance_ms = TIME_TOLERANCES["1min"]
    source_counts = Counter()
    extracted_count = 0
    labeled_count = 0
    without_match_count = 0
    discarded_ambiguity_count = 0

    for scenario in DATASETS[dataset_name]["scenarios"]:
        print(f"Analisando {dataset_name} {scenario}...")
        flows = _load_flows(dataset_name, scenario)
        labels = _load_labels(dataset_name, scenario)
        matched = match_flows_for_dataset(
            dataset_name,
            flows,
            labels,
            tolerance_ms,
        )

        source_counts.update(
            _source_binary_labels(dataset_name, labels).dropna().tolist()
        )
        extracted_count += len(matched)
        labeled_count += int(matched["label"].notna().sum())

        if dataset_name == "unsw_nb15":
            without_match = matched["match_status"].eq(
                "benign_by_exclusion"
            )
        else:
            without_match = matched["match_status"].isin(
                ["unmatched", "invalid_key_or_time"]
            )
        without_match_count += int(without_match.sum())

        discarded_ambiguity = (
            matched["match_status"].str.startswith("ambiguous", na=False)
            & matched["label"].isna()
        )
        discarded_ambiguity_count += int(discarded_ambiguity.sum())

        del flows, labels, matched

    final_path = FINAL_DATA_DIR / "single" / f"{dataset_name}.parquet"
    final_labels = pd.read_parquet(final_path, columns=["label_binary"])
    reconstructed_counts = Counter(final_labels["label_binary"].dropna())
    final_count = len(final_labels)

    if dataset_name == "unsw_nb15":
        without_match_description = "Fluxos sem correspondência de ataque"
        without_match_note = (
            "  Esses fluxos foram rotulados como BENIGN por exclusão."
        )
    else:
        without_match_description = "Fluxos sem correspondência"
        without_match_note = None

    lines = [
        f"RELATÓRIO DO DATASET {dataset_name}",
        "",
        f"Tolerância temporal: {_format_integer(tolerance_ms)} ms",
        f"Fluxos extraídos/reconstruídos: {_format_integer(extracted_count)}",
        f"Fluxos que receberam rótulo: {_format_integer(labeled_count)}",
        f"{without_match_description}: {_format_integer(without_match_count)}",
        "Registros descartados por ambiguidade: "
        f"{_format_integer(discarded_ambiguity_count)}",
        "",
        *_distribution_lines("Distribuição oficial:", source_counts),
        "",
        *_distribution_lines(
            "Distribuição reconstruída:",
            reconstructed_counts,
        ),
        "",
        *_difference_lines(source_counts, reconstructed_counts),
        "",
        "Verificação do arquivo consolidado:",
        f"  Registros: {_format_integer(final_count)}",
        "  Coincide com os fluxos rotulados: "
        f"{'SIM' if final_count == labeled_count else 'NÃO'}",
    ]
    if without_match_note is not None:
        lines.insert(7, without_match_note)

    output_dir = ARTIFACTS_DIR / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{dataset_name}.txt"
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Relatório salvo em {output_path}")
    return output_path


if __name__ == "__main__":
    selected_name, _ = select_dataset(DATASETS)
    generate_dataset_report(selected_name)
