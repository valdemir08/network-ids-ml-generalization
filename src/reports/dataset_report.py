import json
from collections import Counter

import pandas as pd

from src.configs.datasets import DATASETS
from src.configs.paths import (
    ARTIFACTS_DIR,
    FINAL_DATA_DIR,
    INTERMEDIATE_DATA_DIR,
)
from src.flows.flow_matching import create_binary_label
from src.utils.select_dataset import select_dataset


def _load_source_labels(dataset_name, scenario):
    path = (
        INTERMEDIATE_DATA_DIR
        / dataset_name
        / f"{scenario}_labels.parquet"
    )
    columns = ["label"]
    if dataset_name in {"unsw_nb15", "iot23"}:
        columns.append("label_binary_source")
    return pd.read_parquet(path, columns=columns)


def _load_matching_summary(dataset_name):
    path = (
        INTERMEDIATE_DATA_DIR
        / dataset_name
        / "matching_summary.json"
    )
    return json.loads(path.read_text(encoding="utf-8"))


def _source_binary_labels(dataset_name, labels):
    if dataset_name in {"unsw_nb15", "iot23"}:
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


def _attack_distribution_lines(title, counts):
    total = sum(counts.values())
    lines = [title]
    for label, count in counts.most_common():
        lines.append(
            f"  {label}: {_format_integer(count)} "
            f"({_format_percentage(_percentage(count, total))})"
        )
    return lines


def _matching_status_lines(counts):
    lines = ["Resultados por status do matching:"]
    for status, count in counts.most_common():
        lines.append(f"  {status}: {_format_integer(count)}")
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


def generate_dataset_report(dataset_name, scenarios=None):
    if dataset_name not in DATASETS:
        raise ValueError(f"Dataset não configurado: {dataset_name}")

    dataset = DATASETS[dataset_name]
    if scenarios is None:
        scenarios = dataset.get(
            "pipeline_scenarios",
            dataset["scenarios"],
        )

    tolerance_ms = dataset["matching_time_tolerance_ms"]
    source_counts = Counter()
    source_attack_counts = Counter()
    status_counts = Counter()
    matching_summary = _load_matching_summary(dataset_name)
    extracted_count = 0
    labeled_count = 0

    for scenario in scenarios:
        print(f"Analisando {dataset_name} {scenario}...")
        labels = _load_source_labels(dataset_name, scenario)
        scenario_summary = matching_summary[scenario]

        source_binary = _source_binary_labels(dataset_name, labels)
        source_counts.update(source_binary.dropna())
        source_attack_counts.update(
            labels.loc[source_binary.eq("ATTACK"), "label"].dropna()
        )
        extracted_count += scenario_summary["extracted"]
        labeled_count += scenario_summary["labeled"]
        status_counts.update(
            scenario_summary["statuses"]
        )

        del labels

    benign_matched_count = status_counts["benign_matched"]
    unmatched_count = status_counts["unmatched"]
    invalid_count = status_counts["invalid_key_or_time"]
    discarded_ambiguity_count = sum(
        count
        for status, count in status_counts.items()
        if status.startswith("ambiguous")
    )

    final_path = FINAL_DATA_DIR / "single" / f"{dataset_name}.parquet"
    final_labels = pd.read_parquet(
        final_path,
        columns=["label_binary", "label"],
    )
    reconstructed_counts = Counter(final_labels["label_binary"].dropna())
    reconstructed_attack_counts = Counter(
        final_labels.loc[
            final_labels["label_binary"].eq("ATTACK"),
            "label",
        ].dropna()
    )
    final_count = len(final_labels)

    lines = [
        f"RELATÓRIO DO DATASET {dataset_name}",
        "",
        f"Tolerância temporal: {_format_integer(tolerance_ms)} ms",
        f"Fluxos extraídos/reconstruídos: {_format_integer(extracted_count)}",
        f"Fluxos que receberam rótulo: {_format_integer(labeled_count)}",
        "Fluxos benignos com correspondência explícita: "
        f"{_format_integer(benign_matched_count)}",
        "Fluxos sem correspondência descartados: "
        f"{_format_integer(unmatched_count)}",
        "Fluxos com chave ou tempo inválido: "
        f"{_format_integer(invalid_count)}",
        "Registros descartados por ambiguidade: "
        f"{_format_integer(discarded_ambiguity_count)}",
        "",
        *_matching_status_lines(status_counts),
        "",
        *_distribution_lines("Distribuição oficial:", source_counts),
        "",
        *_distribution_lines(
            "Distribuição reconstruída:",
            reconstructed_counts,
        ),
        "",
        *_attack_distribution_lines(
            "Tipos de ataque na distribuição oficial:",
            source_attack_counts,
        ),
        "",
        *_attack_distribution_lines(
            "Tipos de ataque na distribuição reconstruída:",
            reconstructed_attack_counts,
        ),
        "",
        *_difference_lines(source_counts, reconstructed_counts),
        "",
        "Verificação do arquivo consolidado:",
        f"  Registros: {_format_integer(final_count)}",
        "  Coincide com os fluxos rotulados: "
        f"{'SIM' if final_count == labeled_count else 'NÃO'}",
    ]
    output_dir = ARTIFACTS_DIR / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{dataset_name}.txt"
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Relatório salvo em {output_path}")
    return output_path


if __name__ == "__main__":
    selected_name, _ = select_dataset(DATASETS)
    generate_dataset_report(selected_name)
