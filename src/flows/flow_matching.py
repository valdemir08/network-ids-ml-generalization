import time

import numpy as np
import pandas as pd


MATCH_RESULT_COLUMNS = [
    "label",
    "match_status",
    "match_direction",
    "match_time_diff_ms",
]

UNSW_PROTOCOL_WILDCARD_NAMES = {"unas", "ib"}


def create_flow_key(df):
    """
    Cria a chave de 5-tupla usada para localizar fluxos candidatos.

    O timestamp permanece em uma coluna separada porque ele é usado na
    etapa posterior de desambiguação temporal.
    """
    components = [
        df[column].astype("string").str.strip()
        for column in [
            "src_ip",
            "dst_ip",
            "src_port",
            "dst_port",
            "protocol",
        ]
    ]
    df["flow_key"] = components[0].str.cat(components[1:], sep="|")
    return df


def create_bidirectional_flow_key(df):
    """
    Cria a 5-tupla no sentido inverso para a segunda tentativa de matching.

    O matching direto sempre tem prioridade. A chave reversa só é usada
    quando nenhum candidato temporal foi encontrado no sentido direto.
    """
    components = [
        df[column].astype("string").str.strip()
        for column in [
            "dst_ip",
            "src_ip",
            "dst_port",
            "src_port",
            "protocol",
        ]
    ]
    df["flow_key_rev"] = components[0].str.cat(components[1:], sep="|")
    return df


def create_protocol_wildcard_keys(df):
    """
    Cria chaves sem protocolo para o fallback documentado do UNSW-NB15.

    Essa chave não substitui a 5-tupla normal. Ela só é usada quando a
    própria fonte oficial informa ``unas`` ou ``ib`` e, portanto, não fornece
    um número de protocolo que possa compor a 5-tupla.
    """
    direct_components = [
        df[column].astype("string").str.strip()
        for column in ["src_ip", "dst_ip", "src_port", "dst_port"]
    ]
    reverse_components = [
        df[column].astype("string").str.strip()
        for column in ["dst_ip", "src_ip", "dst_port", "src_port"]
    ]
    df["flow_key"] = direct_components[0].str.cat(
        direct_components[1:], sep="|"
    )
    df["flow_key_rev"] = reverse_components[0].str.cat(
        reverse_components[1:], sep="|"
    )
    return df


def _validate_columns(df, required_columns, dataframe_name):
    missing_columns = set(required_columns).difference(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Colunas ausentes em {dataframe_name}: {missing}")


def _empty_match_result():
    return pd.DataFrame(
        columns=["_flow_row_id", *MATCH_RESULT_COLUMNS]
    )


def _match_jointly_unique_keys(
    flows_df,
    labels_df,
    flow_key_col,
    unique_keys,
    time_tolerance,
    direction,
):
    """Executa o caminho rápido para chaves 1:1 nos dois DataFrames."""
    if len(unique_keys) == 0:
        return _empty_match_result()

    unique_flows = flows_df[flows_df[flow_key_col].isin(unique_keys)][
        ["_flow_row_id", flow_key_col, "ts_ms"]
    ].rename(columns={flow_key_col: "_match_key", "ts_ms": "_flow_ts_ms"})

    unique_labels = labels_df[labels_df["flow_key"].isin(unique_keys)][
        ["flow_key", "ts_ms", "label"]
    ].rename(columns={"flow_key": "_match_key", "ts_ms": "_label_ts_ms"})

    matched = unique_flows.merge(
        unique_labels,
        on="_match_key",
        how="inner",
        validate="one_to_one",
    )
    matched["match_time_diff_ms"] = (
        matched["_flow_ts_ms"] - matched["_label_ts_ms"]
    ).abs()
    matched = matched[matched["match_time_diff_ms"] <= time_tolerance].copy()

    if matched.empty:
        return _empty_match_result()

    matched["match_status"] = "matched_unique"
    matched["match_direction"] = direction
    return matched[["_flow_row_id", *MATCH_RESULT_COLUMNS]]


def _prepare_temporal_label_points(labels_df):
    """
    Consolida registros que possuem exatamente a mesma chave e timestamp.

    Repetições com o mesmo rótulo representam o mesmo ponto temporal. Se
    rótulos diferentes ocuparem o mesmo ponto, ele é marcado como ambíguo.
    """
    label_points = labels_df[["flow_key", "ts_ms", "label"]].rename(
        columns={"flow_key": "_match_key", "ts_ms": "_label_ts_ms"}
    )
    group_columns = ["_match_key", "_label_ts_ms"]
    different_labels = label_points.groupby(group_columns, sort=False)[
        "label"
    ].transform("nunique")
    label_points["_label_conflict"] = different_labels > 1
    return label_points.drop_duplicates(group_columns, keep="first")


def _resolve_by_nearest_timestamp(
    flows_df,
    label_points,
    flow_key_col,
    time_tolerance,
    direction,
):
    """
    Resolve chaves 1:N, N:1 e N:M pela menor diferença temporal.

    São procurados o candidato imediatamente anterior e o imediatamente
    posterior. Um empate entre rótulos diferentes permanece ambíguo.
    """
    if flows_df.empty or label_points.empty:
        return _empty_match_result()

    flows_work = flows_df[["_flow_row_id", flow_key_col, "ts_ms"]].rename(
        columns={flow_key_col: "_match_key", "ts_ms": "_flow_ts_ms"}
    )
    available_keys = label_points["_match_key"].drop_duplicates()
    flows_work = flows_work[flows_work["_match_key"].isin(available_keys)]
    if flows_work.empty:
        return _empty_match_result()

    # merge_asof exige ordenação pela coluna temporal usada na aproximação.
    flows_sorted = flows_work.sort_values("_flow_ts_ms")
    labels_sorted = label_points.sort_values("_label_ts_ms")

    backward = pd.merge_asof(
        flows_sorted,
        labels_sorted,
        left_on="_flow_ts_ms",
        right_on="_label_ts_ms",
        by="_match_key",
        tolerance=time_tolerance,
        direction="backward",
    ).rename(
        columns={
            "_label_ts_ms": "_backward_ts_ms",
            "label": "_backward_label",
            "_label_conflict": "_backward_conflict",
        }
    )

    forward = pd.merge_asof(
        flows_sorted,
        labels_sorted,
        left_on="_flow_ts_ms",
        right_on="_label_ts_ms",
        by="_match_key",
        tolerance=time_tolerance,
        direction="forward",
    ).rename(
        columns={
            "_label_ts_ms": "_forward_ts_ms",
            "label": "_forward_label",
            "_label_conflict": "_forward_conflict",
        }
    )

    backward = backward.set_index("_flow_row_id")
    forward = forward.set_index("_flow_row_id")
    resolved = flows_work.set_index("_flow_row_id")[["_flow_ts_ms"]].copy()
    resolved = resolved.join(
        backward[["_backward_ts_ms", "_backward_label", "_backward_conflict"]]
    )
    resolved = resolved.join(
        forward[["_forward_ts_ms", "_forward_label", "_forward_conflict"]]
    )

    resolved["_backward_diff"] = (
        resolved["_flow_ts_ms"] - resolved["_backward_ts_ms"]
    ).abs()
    resolved["_forward_diff"] = (
        resolved["_flow_ts_ms"] - resolved["_forward_ts_ms"]
    ).abs()

    has_backward = resolved["_backward_ts_ms"].notna()
    has_forward = resolved["_forward_ts_ms"].notna()
    resolved = resolved[has_backward | has_forward].copy()

    if resolved.empty:
        return _empty_match_result()

    has_backward = resolved["_backward_ts_ms"].notna()
    has_forward = resolved["_forward_ts_ms"].notna()
    choose_backward = has_backward & (
        ~has_forward | (resolved["_backward_diff"] < resolved["_forward_diff"])
    )
    choose_forward = has_forward & (
        ~has_backward | (resolved["_forward_diff"] < resolved["_backward_diff"])
    )
    equal_distance = has_backward & has_forward & (
        resolved["_backward_diff"] == resolved["_forward_diff"]
    )

    resolved["label"] = pd.NA
    resolved["match_time_diff_ms"] = np.nan
    resolved["_selected_conflict"] = False

    if choose_backward.any():
        resolved.loc[choose_backward, "label"] = resolved.loc[
            choose_backward, "_backward_label"
        ]
        resolved.loc[choose_backward, "match_time_diff_ms"] = resolved.loc[
            choose_backward, "_backward_diff"
        ]
        resolved.loc[choose_backward, "_selected_conflict"] = resolved.loc[
            choose_backward, "_backward_conflict"
        ].fillna(False).astype(bool)

    if choose_forward.any():
        resolved.loc[choose_forward, "label"] = resolved.loc[
            choose_forward, "_forward_label"
        ]
        resolved.loc[choose_forward, "match_time_diff_ms"] = resolved.loc[
            choose_forward, "_forward_diff"
        ]
        resolved.loc[choose_forward, "_selected_conflict"] = resolved.loc[
            choose_forward, "_forward_conflict"
        ].fillna(False).astype(bool)

    same_label_on_tie = (
        resolved["_backward_label"].astype("string")
        == resolved["_forward_label"].astype("string")
    ).fillna(False)
    conflict_on_tie = (
        resolved["_backward_conflict"].fillna(False)
        | resolved["_forward_conflict"].fillna(False)
    )
    accepted_tie = equal_distance & same_label_on_tie & ~conflict_on_tie
    resolved.loc[accepted_tie, "label"] = resolved.loc[
        accepted_tie, "_backward_label"
    ]
    resolved.loc[accepted_tie, "match_time_diff_ms"] = resolved.loc[
        accepted_tie, "_backward_diff"
    ]

    ambiguous_tie = equal_distance & ~accepted_tie
    ambiguous_timestamp = resolved["_selected_conflict"] | (
        equal_distance & conflict_on_tie
    )
    ambiguous = ambiguous_tie | ambiguous_timestamp

    resolved["match_status"] = "matched_nearest"
    resolved.loc[ambiguous_tie, "match_status"] = "ambiguous_nearest_tie"
    resolved.loc[ambiguous_timestamp, "match_status"] = (
        "ambiguous_timestamp_labels"
    )
    resolved.loc[ambiguous, "label"] = pd.NA
    resolved["match_direction"] = direction

    resolved = resolved.reset_index()
    return resolved[["_flow_row_id", *MATCH_RESULT_COLUMNS]]


def _match_one_direction(
    flows_df,
    labels_df,
    label_points,
    flow_key_col,
    time_tolerance,
    direction,
    flow_count_reference=None,
):
    """Aplica o caminho 1:1 e depois o temporal para uma direção."""
    if flows_df.empty or labels_df.empty:
        return _empty_match_result()

    if flow_count_reference is None:
        flow_count_reference = flows_df

    flow_counts = flow_count_reference[flow_key_col].value_counts(sort=False)
    label_counts = labels_df["flow_key"].value_counts(sort=False)
    unique_keys = flow_counts[flow_counts == 1].index.intersection(
        label_counts[label_counts == 1].index
    )

    unique_results = _match_jointly_unique_keys(
        flows_df,
        labels_df,
        flow_key_col,
        unique_keys,
        time_tolerance,
        direction,
    )

    complex_flows = flows_df[~flows_df[flow_key_col].isin(unique_keys)]
    complex_results = _resolve_by_nearest_timestamp(
        complex_flows,
        label_points,
        flow_key_col,
        time_tolerance,
        direction,
    )

    return pd.concat([unique_results, complex_results], ignore_index=True)


def _apply_match_results(match_table, direction_results):
    if direction_results.empty:
        return

    direction_results = direction_results.set_index("_flow_row_id")
    row_ids = direction_results.index
    for column in MATCH_RESULT_COLUMNS:
        values = direction_results.loc[row_ids, column]
        if column == "match_time_diff_ms":
            values = values.to_numpy(dtype="float64", na_value=np.nan)
        else:
            values = values.to_numpy(dtype="object")
        match_table.loc[row_ids, column] = values


def match_flows_simple(flows, labels, time_tolerance):
    """
    Encontra o rótulo de cada fluxo pela 5-tupla e proximidade temporal.

    A identidade técnica da linha é preservada em ``_flow_row_id`` apenas
    durante o cálculo. Ela não participa do matching e não é salva no resultado.
    """
    _validate_columns(
        flows,
        ["flow_key", "flow_key_rev", "bidirectional_first_seen_ms"],
        "flows",
    )
    _validate_columns(labels, ["flow_key", "ts_ms", "label"], "labels")

    if time_tolerance < 0:
        raise ValueError("time_tolerance deve ser maior ou igual a zero")

    print("\nIniciando matching...")
    print(f"Tolerância: {time_tolerance / 1000:.1f}s")
    started_at = time.perf_counter()

    flows_subset = flows[
        ["flow_key", "flow_key_rev", "bidirectional_first_seen_ms"]
    ].copy()
    flows_subset.insert(0, "_flow_row_id", np.arange(len(flows_subset)))
    flows_subset = flows_subset.rename(
        columns={"bidirectional_first_seen_ms": "ts_ms"}
    )
    labels_subset = labels[["flow_key", "ts_ms", "label"]].copy()

    flows_subset["ts_ms"] = pd.to_numeric(flows_subset["ts_ms"], errors="coerce")
    labels_subset["ts_ms"] = pd.to_numeric(labels_subset["ts_ms"], errors="coerce")

    valid_flows_mask = flows_subset[
        ["flow_key", "flow_key_rev", "ts_ms"]
    ].notna().all(axis=1)
    valid_labels_mask = labels_subset[["flow_key", "ts_ms", "label"]].notna().all(
        axis=1
    )
    valid_flows = flows_subset[valid_flows_mask].copy()
    valid_labels = labels_subset[valid_labels_mask].copy()

    valid_flows["ts_ms"] = valid_flows["ts_ms"].astype("int64")
    valid_labels["ts_ms"] = valid_labels["ts_ms"].astype("int64")
    for column in ["flow_key", "flow_key_rev"]:
        valid_flows[column] = valid_flows[column].astype(str)
    valid_labels["flow_key"] = valid_labels["flow_key"].astype(str)
    label_points = _prepare_temporal_label_points(valid_labels)

    match_table = pd.DataFrame(index=np.arange(len(flows_subset)))
    match_table.index.name = "_flow_row_id"
    match_table["label"] = pd.NA
    match_table["match_status"] = "invalid_key_or_time"
    match_table["match_direction"] = pd.NA
    match_table["match_time_diff_ms"] = np.nan
    match_table.loc[valid_flows["_flow_row_id"], "match_status"] = "unmatched"

    direct_results = _match_one_direction(
        valid_flows,
        valid_labels,
        label_points,
        "flow_key",
        time_tolerance,
        "direct",
    )
    _apply_match_results(match_table, direct_results)

    # Ambiguidades diretas são preservadas. Somente fluxos sem qualquer
    # candidato temporal direto seguem para a tentativa reversa.
    reverse_row_ids = match_table.index[match_table["match_status"] == "unmatched"]
    reverse_flows = valid_flows[valid_flows["_flow_row_id"].isin(reverse_row_ids)]
    reverse_results = _match_one_direction(
        reverse_flows,
        valid_labels,
        label_points,
        "flow_key_rev",
        time_tolerance,
        "reverse",
        flow_count_reference=valid_flows,
    )
    _apply_match_results(match_table, reverse_results)

    elapsed = time.perf_counter() - started_at
    print(f"Matching concluído em {elapsed:.2f}s")
    print("Resumo do matching:")
    for status, count in match_table["match_status"].value_counts().items():
        print(f"- {status}: {count}")

    return match_table.reset_index()


def create_binary_label(label_series, benign_label="BENIGN"):
    """Converte rótulos válidos em BENIGN/ATTACK sem alterar o original."""
    labels_as_string = label_series.astype("string").str.strip()
    normalized_labels = labels_as_string.str.upper()
    valid_labels = labels_as_string.notna() & labels_as_string.ne("")

    binary_label = pd.Series(pd.NA, index=label_series.index, dtype="string")
    binary_label.loc[valid_labels & normalized_labels.eq(benign_label.upper())] = (
        "BENIGN"
    )
    binary_label.loc[valid_labels & ~normalized_labels.eq(benign_label.upper())] = (
        "ATTACK"
    )
    return binary_label


def match_flows(flows, labels, time_tolerance):
    """Retorna os fluxos originais com rótulos e metadados do matching."""
    match_table = match_flows_simple(flows, labels, time_tolerance).set_index(
        "_flow_row_id"
    )
    expected_row_ids = pd.Index(np.arange(len(flows)), name="_flow_row_id")
    match_table = match_table.reindex(expected_row_ids)

    result = flows.copy()
    for column in MATCH_RESULT_COLUMNS:
        result[column] = match_table[column].to_numpy()
    result["label_binary"] = create_binary_label(result["label"])
    return result


def match_unsw_flows(flows, labels, time_tolerance):
    """
    Aplica a política de ataques-primeiro usada na reconstrução do UNSW-NB15.

    O ground truth original é exaustivo para os períodos selecionados. Assim,
    fluxos tecnicamente válidos sem ataque temporalmente compatível são
    classificados como benignos por exclusão. Divergências entre categorias de
    ataque permanecem explícitas, mas continuam sendo ataques na coluna binária.

    A 5-tupla é sempre a regra principal. Um segundo passo é aplicado apenas aos
    registros oficiais ``unas`` e ``ib``, que não contêm um número de protocolo
    mapeável. Nesses casos, IPs, portas e tempo são usados como fallback e o
    status registra explicitamente que o protocolo não participou do match.
    """
    attack_labels = labels[labels["label"].ne("BENIGN")].copy()
    result = match_flows(flows, attack_labels, time_tolerance)

    strict_ambiguous_attack = result["match_status"].str.startswith(
        "ambiguous",
        na=False,
    )
    result.loc[strict_ambiguous_attack, "label"] = "ATTACK_AMBIGUOUS"
    result.loc[strict_ambiguous_attack, "label_binary"] = "ATTACK"

    can_use_protocol_fallback = {
        "protocol_name",
        "src_ip",
        "dst_ip",
        "src_port",
        "dst_port",
        "ts_ms",
        "label",
    }.issubset(labels.columns)
    unmatched_mask = result["match_status"].eq("unmatched")

    if can_use_protocol_fallback and unmatched_mask.any():
        protocol_names = (
            labels["protocol_name"].astype("string").str.strip().str.lower()
        )
        wildcard_labels = labels.loc[
            labels["label"].ne("BENIGN")
            & protocol_names.isin(UNSW_PROTOCOL_WILDCARD_NAMES),
            [
                "src_ip",
                "dst_ip",
                "src_port",
                "dst_port",
                "ts_ms",
                "label",
            ],
        ].copy()

        if not wildcard_labels.empty:
            wildcard_flows = flows.loc[
                unmatched_mask,
                [
                    "src_ip",
                    "dst_ip",
                    "src_port",
                    "dst_port",
                    "bidirectional_first_seen_ms",
                ],
            ].copy()
            wildcard_flows = create_protocol_wildcard_keys(wildcard_flows)
            wildcard_labels = create_protocol_wildcard_keys(wildcard_labels)

            wildcard_result = match_flows(
                wildcard_flows,
                wildcard_labels,
                time_tolerance,
            )
            wildcard_ambiguous = wildcard_result[
                "match_status"
            ].str.startswith("ambiguous", na=False)
            wildcard_attack = wildcard_result["label"].notna() | wildcard_ambiguous

            if wildcard_attack.any():
                wildcard_rows = wildcard_result.index[wildcard_attack]
                wildcard_labels_result = wildcard_result.loc[
                    wildcard_rows, "label"
                ].astype("string")
                wildcard_labels_result.loc[
                    wildcard_ambiguous.loc[wildcard_rows]
                ] = "ATTACK_AMBIGUOUS"

                status_prefix = {
                    "matched_unique": "matched_protocol_wildcard_unique",
                    "matched_nearest": "matched_protocol_wildcard_nearest",
                    "ambiguous_nearest_tie": (
                        "ambiguous_protocol_wildcard_nearest_tie"
                    ),
                    "ambiguous_timestamp_labels": (
                        "ambiguous_protocol_wildcard_timestamp_labels"
                    ),
                }
                result.loc[wildcard_rows, "label"] = (
                    wildcard_labels_result.to_numpy()
                )
                result.loc[wildcard_rows, "label_binary"] = "ATTACK"
                result.loc[wildcard_rows, "match_status"] = (
                    wildcard_result.loc[wildcard_rows, "match_status"]
                    .map(status_prefix)
                    .to_numpy()
                )
                result.loc[wildcard_rows, "match_direction"] = (
                    wildcard_result.loc[wildcard_rows, "match_direction"]
                    .to_numpy()
                )
                result.loc[wildcard_rows, "match_time_diff_ms"] = (
                    wildcard_result.loc[wildcard_rows, "match_time_diff_ms"]
                    .to_numpy()
                )

    benign_by_exclusion = result["match_status"].eq("unmatched")
    result.loc[benign_by_exclusion, "label"] = "BENIGN"
    result.loc[benign_by_exclusion, "label_binary"] = "BENIGN"
    result.loc[benign_by_exclusion, "match_status"] = "benign_by_exclusion"
    return result


def match_flows_for_dataset(dataset_name, flows, labels, time_tolerance):
    """Seleciona, em uma camada única, a política de matching do dataset."""
    if dataset_name == "cicids2017":
        return match_flows(flows, labels, time_tolerance)
    elif dataset_name == "unsw_nb15":
        return match_unsw_flows(flows, labels, time_tolerance)
    elif dataset_name == "bot_iot":
        pass
    else:
        raise ValueError(f"Dataset sem política de matching: {dataset_name}")
