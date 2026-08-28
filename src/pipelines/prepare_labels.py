import re

import dpkt.ip
import pandas as pd

from src.configs.column_mappings import (
    CICIDS_MAPPING,
    UNSW_NB15_LABEL_COLUMNS,
    UNSW_NB15_LABEL_USECOLS,
)
from src.configs.datasets import DATASETS
from src.configs.paths import INTERMEDIATE_DATA_DIR
from src.data_processing.dataset_utils import (
    adjust_time_for_cic_datasets,
    apply_timezone_offset,
    convert_to_datetime,
    create_column_ts_ms,
    dataset_cleanup,
    padronize_cols_name,
)
from src.io.io_utils import save_parquet


UNSW_READ_CHUNK_SIZE = 250_000


def read_csv_safe(path):
    try:
        return pd.read_csv(path, dtype=str, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(
            path,
            encoding="latin1",
            dtype=str,
            low_memory=False,
        )


def load_and_concat_csvs(dataset_name, scenario):
    dataset_cfg = DATASETS[dataset_name]
    scenario_cfg = dataset_cfg["scenarios"][scenario]

    root = dataset_cfg["root"]
    label_dir = root / dataset_cfg["label_dir"]

    csvs = []

    for csv_file in scenario_cfg["labels"]:
        path = label_dir / csv_file
        print("Carregando:", path)
        csvs.append(read_csv_safe(path))

    if not csvs:
        raise ValueError(
            f"Nenhum arquivo de rótulos configurado para {dataset_name} {scenario}"
        )

    return pd.concat(csvs, ignore_index=True)


def _prepare_cicids_labels(dataset_name, scenario):
    output_path = (
        INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_labels.parquet"
    )
    df = load_and_concat_csvs(dataset_name, scenario)
    df = dataset_cleanup(df)
    df = padronize_cols_name(df, CICIDS_MAPPING)
    df = convert_to_datetime(df)
    df = adjust_time_for_cic_datasets(df, "Timestamp")
    df = apply_timezone_offset(df, "Timestamp", 3)
    df = create_column_ts_ms(df)
    save_parquet(df, output_path)
    return output_path


def _normalize_protocol_key(value):
    return re.sub(r"[^a-z0-9]", "", str(value).strip().lower())


def _build_unsw_protocol_mapping():
    mapping = {
        _normalize_protocol_key(name.removeprefix("IP_PROTO_")): number
        for name, number in vars(dpkt.ip).items()
        if name.startswith("IP_PROTO_") and isinstance(number, int)
    }

    # Nomes usados pelo UNSW-NB15 que diferem dos identificadores do dpkt.
    aliases = {
        "ipnip": 4,
        "st2": 5,
        "xnsidp": 22,
        "dcn": 19,
        "isotp4": 29,
        "mfensp": 31,
        "idprcmtp": 38,
        "tp": 39,
        "ipv6": 41,
        "ipv6route": 43,
        "ipv6frag": 44,
        "idrp": 45,
        "bna": 49,
        "ipv6no": 59,
        "ipv6opts": 60,
        "any": 114,
        "satexpak": 64,
        "isoip": 80,
        "securevmtp": 82,
        "nsfnetigp": 85,
        "spriterpc": 90,
        "ipip": 94,
        "aessp3d": 96,
        "prienc": 99,
        "ipxnip": 111,
        "sccopmce": 128,
        "zero": 0,
    }
    mapping.update(aliases)
    return mapping


UNSW_PROTOCOL_MAPPING = _build_unsw_protocol_mapping()


def _normalize_port(series):
    values = series.astype("string").str.strip()
    normalized = pd.to_numeric(values, errors="coerce")

    hexadecimal = values.str.fullmatch(r"0[xX][0-9a-fA-F]+", na=False)
    if hexadecimal.any():
        normalized.loc[hexadecimal] = values.loc[hexadecimal].map(
            lambda value: int(value, 16)
        )

    normalized.loc[values.eq("-")] = 0
    normalized = normalized.where(normalized.between(0, 65_535))
    return normalized.astype("Int64")


def _scenario_flow_files(dataset_name, scenario):
    dataset_cfg = DATASETS[dataset_name]
    output_mode = dataset_cfg.get("flow_output_mode", "scenario")
    dataset_dir = INTERMEDIATE_DATA_DIR / dataset_name

    if output_mode == "scenario":
        files = [dataset_dir / f"{scenario}_flows.parquet"]
    elif output_mode == "per_pcap":
        files = sorted(
            (dataset_dir / scenario).glob("*_flows.parquet"),
            key=lambda path: int(path.stem.split("_")[0]),
        )
    else:
        raise ValueError(
            f"Modo de saída inválido para {dataset_name}: {output_mode}"
        )

    missing = [path for path in files if not path.exists()]
    if not files or missing:
        raise FileNotFoundError(
            f"Fluxos intermediários ausentes para {dataset_name} {scenario}"
        )
    return files


def _scenario_time_window_ms(dataset_name, scenario):
    first_seen = None
    last_seen = None

    for path in _scenario_flow_files(dataset_name, scenario):
        timestamps = pd.read_parquet(
            path,
            columns=[
                "bidirectional_first_seen_ms",
                "bidirectional_last_seen_ms",
            ],
        )
        current_first = timestamps["bidirectional_first_seen_ms"].min()
        current_last = timestamps["bidirectional_last_seen_ms"].max()
        first_seen = (
            current_first
            if first_seen is None
            else min(first_seen, current_first)
        )
        last_seen = (
            current_last if last_seen is None else max(last_seen, current_last)
        )

    return int(first_seen), int(last_seen)


def _load_unsw_scenario_labels(dataset_name, scenario):
    dataset_cfg = DATASETS[dataset_name]
    scenario_cfg = dataset_cfg["scenarios"][scenario]
    label_dir = dataset_cfg["root"] / dataset_cfg["label_dir"]
    margin_ms = dataset_cfg.get("label_time_margin_ms", 0)
    first_seen_ms, last_seen_ms = _scenario_time_window_ms(
        dataset_name,
        scenario,
    )
    lower_bound_s = (first_seen_ms - margin_ms) / 1_000
    upper_bound_s = (last_seen_ms + margin_ms) / 1_000

    selected_chunks = []
    source_record_count = 0
    boundary_duplicate_count = 0
    previous_last_identity = None
    identity_columns = [
        "src_ip",
        "src_port",
        "dst_ip",
        "dst_port",
        "protocol_name",
        "start_time_s",
        "last_time_s",
        "attack_category",
        "source_binary_label",
    ]

    for csv_file in scenario_cfg["labels"]:
        path = label_dir / csv_file
        print("Carregando em blocos:", path)

        chunks = pd.read_csv(
            path,
            header=None,
            usecols=UNSW_NB15_LABEL_USECOLS,
            dtype=str,
            encoding="cp1252",
            chunksize=UNSW_READ_CHUNK_SIZE,
            low_memory=False,
        )
        first_chunk = True
        current_file_last_identity = None
        for chunk in chunks:
            chunk.columns = UNSW_NB15_LABEL_COLUMNS
            current_file_last_identity = tuple(
                chunk.iloc[-1][identity_columns].fillna("")
            )

            if first_chunk and previous_last_identity is not None:
                current_first_identity = tuple(
                    chunk.iloc[0][identity_columns].fillna("")
                )
                if current_first_identity == previous_last_identity:
                    chunk = chunk.iloc[1:].copy()
                    boundary_duplicate_count += 1
            first_chunk = False

            start_time = pd.to_numeric(
                chunk["start_time_s"],
                errors="coerce",
            )
            last_time = pd.to_numeric(
                chunk["last_time_s"],
                errors="coerce",
            )
            overlaps_scenario = (
                last_time.ge(lower_bound_s)
                & start_time.le(upper_bound_s)
            )
            selected = chunk.loc[overlaps_scenario].copy()
            source_record_count += len(selected)
            if not selected.empty:
                selected_chunks.append(selected)

        previous_last_identity = current_file_last_identity

    if not selected_chunks:
        raise ValueError(
            f"Nenhum rótulo do UNSW sobrepõe o cenário {scenario}"
        )

    labels = pd.concat(selected_chunks, ignore_index=True)

    labels["src_ip"] = (
        labels["src_ip"]
        .astype("string")
        .str.strip()
        .str.lstrip("\ufeffï»¿")
    )
    labels["dst_ip"] = labels["dst_ip"].astype("string").str.strip()
    labels["src_port"] = _normalize_port(labels["src_port"])
    labels["dst_port"] = _normalize_port(labels["dst_port"])

    protocol_keys = labels["protocol_name"].map(_normalize_protocol_key)
    labels["protocol"] = protocol_keys.map(UNSW_PROTOCOL_MAPPING).astype("Int64")

    labels["ts_ms"] = (
        pd.to_numeric(labels["start_time_s"], errors="coerce") * 1_000
    ).round().astype("Int64")
    labels["last_ts_ms"] = (
        pd.to_numeric(labels["last_time_s"], errors="coerce") * 1_000
    ).round().astype("Int64")

    source_binary = labels["source_binary_label"].astype("string").str.strip()
    invalid_binary = ~source_binary.isin(["0", "1"])
    if invalid_binary.any():
        raise ValueError(
            f"Foram encontrados {int(invalid_binary.sum())} rótulos binários inválidos"
        )

    attack_category = (
        labels["attack_category"].astype("string").fillna("").str.strip()
    )
    attack_category = attack_category.replace(
        {"Backdoor": "Backdoors", "Backdoors": "Backdoors"}
    )
    missing_attack_category = source_binary.eq("1") & attack_category.eq("")
    benign_with_attack_category = source_binary.eq("0") & attack_category.ne("")
    if missing_attack_category.any() or benign_with_attack_category.any():
        raise ValueError(
            "Inconsistência entre attack_category e Label no UNSW-NB15"
        )

    labels["label"] = attack_category.mask(source_binary.eq("0"), "BENIGN")
    labels["label_binary_source"] = source_binary.map(
        {"0": "BENIGN", "1": "ATTACK"}
    ).astype("string")

    output_columns = [
        "src_ip",
        "dst_ip",
        "src_port",
        "dst_port",
        "protocol",
        "ts_ms",
        "last_ts_ms",
        "label",
        "label_binary_source",
        "protocol_name",
    ]
    labels = labels[output_columns]

    invalid_key_count = int(
        labels[
            ["src_ip", "dst_ip", "src_port", "dst_port", "protocol", "ts_ms"]
        ].isna().any(axis=1).sum()
    )
    print(f"Registros selecionados: {source_record_count}")
    print(
        "Duplicações de fronteira removidas: "
        f"{boundary_duplicate_count}"
    )
    print(f"Registros preparados: {len(labels)}")
    print(f"Registros com chave ou tempo inválido: {invalid_key_count}")
    return labels


def _prepare_unsw_labels(dataset_name, scenario):
    output_path = (
        INTERMEDIATE_DATA_DIR / dataset_name / f"{scenario}_labels.parquet"
    )
    labels = _load_unsw_scenario_labels(dataset_name, scenario)
    save_parquet(labels, output_path)
    return output_path


def prepare_labels(dataset_name, scenario):
    print(f"Preparando labels para {dataset_name} {scenario}")

    if dataset_name == "cicids2017":
        return _prepare_cicids_labels(dataset_name, scenario)
    elif dataset_name == "unsw_nb15":
        return _prepare_unsw_labels(dataset_name, scenario)
    elif dataset_name == "bot_iot":
        pass
    else:
        raise ValueError(f"Dataset sem preparação configurada: {dataset_name}")


if __name__ == "__main__":
    prepare_labels(dataset_name="cicids2017", scenario="monday")
