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


def _prepare_interval_groups(labels):
    """Agrupa intervalos por chave para evitar um produto cartesiano."""
    work = labels[
        ["flow_key", "ts_ms", "last_ts_ms", "label"]
    ].copy()
    work["ts_ms"] = pd.to_numeric(work["ts_ms"], errors="coerce")
    work["last_ts_ms"] = pd.to_numeric(
        work["last_ts_ms"],
        errors="coerce",
    )
    valid = work[
        work[["flow_key", "ts_ms", "last_ts_ms", "label"]]
        .notna()
        .all(axis=1)
    ].copy()
    valid = valid[valid["last_ts_ms"] >= valid["ts_ms"]]
    if valid.empty:
        return {}, {}

    valid["flow_key"] = valid["flow_key"].astype(str)
    valid["ts_ms"] = valid["ts_ms"].astype("int64")
    valid["last_ts_ms"] = valid["last_ts_ms"].astype("int64")

    label_names = sorted(valid["label"].astype(str).unique())
    label_to_code = {
        label: code for code, label in enumerate(label_names, start=1)
    }
    valid["_label_code"] = (
        valid["label"].astype(str).map(label_to_code).astype("int32")
    )

    groups = {}
    valid = valid.sort_values(["flow_key", "ts_ms"], kind="stable")
    for key, group in valid.groupby("flow_key", sort=False):
        starts = group["ts_ms"].to_numpy(dtype=np.int64)
        ends = group["last_ts_ms"].to_numpy(dtype=np.int64)
        codes = group["_label_code"].to_numpy(dtype=np.int32)
        maximum_duration = int(np.max(ends - starts))
        groups[key] = (starts, ends, codes, maximum_duration)

    code_to_label = {code: label for label, code in label_to_code.items()}
    return groups, code_to_label


def _resolve_interval_direction(
    flow_keys,
    flow_starts,
    flow_ends,
    label_groups,
    time_tolerance,
    row_ids,
):
    """Resolve uma direção pela sobreposição dos intervalos."""
    codes = np.zeros(len(flow_keys), dtype=np.int32)
    differences = np.full(len(flow_keys), np.nan)

    for row_id in row_ids:
        group = label_groups.get(str(flow_keys[row_id]))
        if group is None:
            continue

        label_starts, label_ends, label_codes, maximum_duration = group
        flow_start = int(flow_starts[row_id])
        flow_end = int(flow_ends[row_id])

        left = np.searchsorted(
            label_starts,
            flow_start - time_tolerance - maximum_duration,
            side="left",
        )
        right = np.searchsorted(
            label_starts,
            flow_end + time_tolerance,
            side="right",
        )
        if left == right:
            continue

        starts = label_starts[left:right]
        ends = label_ends[left:right]
        possible = ends >= flow_start - time_tolerance
        if not possible.any():
            continue

        starts = starts[possible]
        ends = ends[possible]
        candidate_codes = label_codes[left:right][possible]
        gaps = np.maximum(
            np.maximum(starts - flow_end, flow_start - ends),
            0,
        )
        inside = gaps <= time_tolerance
        if not inside.any():
            continue

        unique_codes = np.unique(candidate_codes[inside])
        codes[row_id] = unique_codes[0] if len(unique_codes) == 1 else -1
        differences[row_id] = float(gaps[inside].min())

    return codes, differences


def match_flows_by_interval_simple(flows, labels, time_tolerance):
    """Encontra rótulos pela 5-tupla e sobreposição temporal."""
    _validate_columns(
        flows,
        [
            "flow_key",
            "flow_key_rev",
            "bidirectional_first_seen_ms",
            "bidirectional_last_seen_ms",
        ],
        "flows",
    )
    _validate_columns(
        labels,
        ["flow_key", "ts_ms", "last_ts_ms", "label"],
        "labels",
    )
    if time_tolerance < 0:
        raise ValueError("time_tolerance deve ser maior ou igual a zero")

    flow_starts = pd.to_numeric(
        flows["bidirectional_first_seen_ms"],
        errors="coerce",
    )
    flow_ends = pd.to_numeric(
        flows["bidirectional_last_seen_ms"],
        errors="coerce",
    )
    valid = (
        flows[["flow_key", "flow_key_rev"]].notna().all(axis=1)
        & flow_starts.notna()
        & flow_ends.notna()
        & flow_ends.ge(flow_starts)
    ).to_numpy()

    flow_keys = flows["flow_key"].astype("string").to_numpy()
    reverse_keys = flows["flow_key_rev"].astype("string").to_numpy()
    starts = flow_starts.fillna(0).to_numpy(dtype=np.int64)
    ends = flow_ends.fillna(0).to_numpy(dtype=np.int64)
    label_groups, code_to_label = _prepare_interval_groups(labels)

    valid_rows = np.flatnonzero(valid)
    codes, differences = _resolve_interval_direction(
        flow_keys,
        starts,
        ends,
        label_groups,
        time_tolerance,
        valid_rows,
    )
    directions = np.full(len(flows), None, dtype=object)
    directions[codes != 0] = "direct"

    reverse_rows = np.flatnonzero(valid & (codes == 0))
    reverse_codes, reverse_differences = _resolve_interval_direction(
        reverse_keys,
        starts,
        ends,
        label_groups,
        time_tolerance,
        reverse_rows,
    )
    use_reverse = reverse_codes != 0
    codes[use_reverse] = reverse_codes[use_reverse]
    differences[use_reverse] = reverse_differences[use_reverse]
    directions[use_reverse] = "reverse"

    result = pd.DataFrame({"_flow_row_id": np.arange(len(flows))})
    result["label"] = pd.NA
    result["match_status"] = "invalid_key_or_time"
    result["match_direction"] = directions
    result["match_time_diff_ms"] = differences
    result.loc[valid, "match_status"] = "unmatched"

    matched = codes > 0
    for code, label in code_to_label.items():
        result.loc[codes == code, "label"] = label
    result.loc[matched, "match_status"] = "matched_interval"
    result.loc[codes < 0, "match_status"] = "ambiguous_interval_labels"
    return result


def _match_any_interval_one_direction(
    flows,
    labels,
    flow_key_column,
    time_tolerance,
    direction,
):
    """Localiza qualquer intervalo compatível quando há uma única classe."""
    if flows.empty or labels.empty:
        return _empty_match_result()

    flow_work = flows[
        [
            "_flow_row_id",
            flow_key_column,
            "bidirectional_first_seen_ms",
            "bidirectional_last_seen_ms",
        ]
    ].rename(
        columns={
            flow_key_column: "_match_key",
            "bidirectional_first_seen_ms": "_flow_start_ms",
            "bidirectional_last_seen_ms": "_flow_end_ms",
        }
    )
    label_work = labels[
        ["flow_key", "ts_ms", "last_ts_ms"]
    ].rename(
        columns={
            "flow_key": "_match_key",
            "ts_ms": "_label_start_ms",
            "last_ts_ms": "_label_end_ms",
        }
    )

    flow_work["_query_end_ms"] = (
        flow_work["_flow_end_ms"] + time_tolerance
    )
    label_work = label_work.sort_values(
        ["_match_key", "_label_start_ms"],
        kind="stable",
    )
    label_work["_maximum_end_ms"] = label_work.groupby(
        "_match_key",
        sort=False,
    )["_label_end_ms"].cummax()

    matched = pd.merge_asof(
        flow_work.sort_values("_query_end_ms"),
        label_work.sort_values("_label_start_ms"),
        left_on="_query_end_ms",
        right_on="_label_start_ms",
        by="_match_key",
        direction="backward",
    )
    overlaps = matched["_maximum_end_ms"].ge(
        matched["_flow_start_ms"] - time_tolerance
    )
    matched = matched[overlaps].copy()
    if matched.empty:
        return _empty_match_result()

    matched["label"] = "BENIGN"
    matched["match_status"] = "matched_interval"
    matched["match_direction"] = direction
    matched["match_time_diff_ms"] = np.nan
    return matched[["_flow_row_id", *MATCH_RESULT_COLUMNS]]


def match_any_interval_simple(flows, labels, time_tolerance):
    """Marca fluxos que sobrepõem ao menos um intervalo de uma única classe."""
    _validate_columns(
        flows,
        [
            "flow_key",
            "flow_key_rev",
            "bidirectional_first_seen_ms",
            "bidirectional_last_seen_ms",
        ],
        "flows",
    )
    _validate_columns(
        labels,
        ["flow_key", "ts_ms", "last_ts_ms"],
        "labels",
    )

    flows_work = flows[
        [
            "flow_key",
            "flow_key_rev",
            "bidirectional_first_seen_ms",
            "bidirectional_last_seen_ms",
        ]
    ].copy()
    flows_work.insert(0, "_flow_row_id", np.arange(len(flows_work)))
    for column in ["bidirectional_first_seen_ms", "bidirectional_last_seen_ms"]:
        flows_work[column] = pd.to_numeric(flows_work[column], errors="coerce")

    labels_work = labels[["flow_key", "ts_ms", "last_ts_ms"]].copy()
    for column in ["ts_ms", "last_ts_ms"]:
        labels_work[column] = pd.to_numeric(labels_work[column], errors="coerce")

    valid_flows = flows_work[
        flows_work[
            [
                "flow_key",
                "flow_key_rev",
                "bidirectional_first_seen_ms",
                "bidirectional_last_seen_ms",
            ]
        ].notna().all(axis=1)
        & flows_work["bidirectional_last_seen_ms"].ge(
            flows_work["bidirectional_first_seen_ms"]
        )
    ].copy()
    valid_labels = labels_work[
        labels_work[["flow_key", "ts_ms", "last_ts_ms"]]
        .notna()
        .all(axis=1)
        & labels_work["last_ts_ms"].ge(labels_work["ts_ms"])
    ].copy()
    for column in ["flow_key", "flow_key_rev"]:
        valid_flows[column] = valid_flows[column].astype(str)
    for column in ["bidirectional_first_seen_ms", "bidirectional_last_seen_ms"]:
        valid_flows[column] = valid_flows[column].astype("int64")
    valid_labels["flow_key"] = valid_labels["flow_key"].astype(str)
    for column in ["ts_ms", "last_ts_ms"]:
        valid_labels[column] = valid_labels[column].astype("int64")

    result = pd.DataFrame({"_flow_row_id": np.arange(len(flows))})
    result["label"] = pd.NA
    result["match_status"] = "invalid_key_or_time"
    result["match_direction"] = pd.NA
    result["match_time_diff_ms"] = np.nan
    result.loc[valid_flows["_flow_row_id"], "match_status"] = "unmatched"

    direct = _match_any_interval_one_direction(
        valid_flows,
        valid_labels,
        "flow_key",
        time_tolerance,
        "direct",
    )
    result = result.set_index("_flow_row_id")
    _apply_match_results(result, direct)
    reverse_ids = result.index[result["match_status"].eq("unmatched")]
    reverse_flows = valid_flows[
        valid_flows["_flow_row_id"].isin(reverse_ids)
    ]
    reverse = _match_any_interval_one_direction(
        reverse_flows,
        valid_labels,
        "flow_key_rev",
        time_tolerance,
        "reverse",
    )
    _apply_match_results(result, reverse)
    return result.reset_index()


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


def _split_labels(labels):
    normalized = labels["label"].astype("string").str.strip().str.upper()
    benign = normalized.eq("BENIGN")
    return labels[~benign & normalized.notna()].copy(), labels[benign].copy()


def _apply_unsw_protocol_fallback(
    attack_matches,
    flows,
    attack_labels,
    time_tolerance,
):
    """Testa ataques ``unas``/``ib`` sem transformar conflitos em rótulos."""
    required = {
        "protocol_name",
        "src_ip",
        "dst_ip",
        "src_port",
        "dst_port",
        "ts_ms",
        "last_ts_ms",
        "label",
    }
    if not required.issubset(attack_labels.columns):
        return attack_matches

    protocol_names = (
        attack_labels["protocol_name"].astype("string").str.strip().str.lower()
    )
    wildcard_labels = attack_labels[
        protocol_names.isin(UNSW_PROTOCOL_WILDCARD_NAMES)
    ].copy()
    unmatched_rows = np.flatnonzero(
        attack_matches["match_status"].eq("unmatched").to_numpy()
    )
    if wildcard_labels.empty or len(unmatched_rows) == 0:
        return attack_matches

    wildcard_flows = flows.iloc[unmatched_rows][
        [
            "src_ip",
            "dst_ip",
            "src_port",
            "dst_port",
            "bidirectional_first_seen_ms",
            "bidirectional_last_seen_ms",
        ]
    ].copy()
    wildcard_flows = create_protocol_wildcard_keys(wildcard_flows)
    wildcard_labels = create_protocol_wildcard_keys(wildcard_labels)
    wildcard_matches = match_flows_by_interval_simple(
        wildcard_flows,
        wildcard_labels,
        time_tolerance,
    )

    has_candidate = ~wildcard_matches["match_status"].isin(
        ["unmatched", "invalid_key_or_time"]
    )
    if not has_candidate.any():
        return attack_matches

    local_rows = wildcard_matches.loc[
        has_candidate, "_flow_row_id"
    ].to_numpy(dtype=np.int64)
    target_rows = unmatched_rows[local_rows]
    selected = wildcard_matches.loc[has_candidate].copy()
    selected["match_status"] = selected["match_status"].replace(
        {
            "matched_interval": "matched_protocol_wildcard_interval",
            "ambiguous_interval_labels": (
                "ambiguous_protocol_wildcard_interval_labels"
            ),
        }
    )
    for column in MATCH_RESULT_COLUMNS:
        attack_matches.loc[target_rows, column] = selected[column].to_numpy()
    return attack_matches


def _finish_attacks_first(
    flows,
    attack_matches,
    benign_labels,
    benign_matcher,
    time_tolerance,
):
    """Mantém somente ataques e benignos com correspondência positiva."""
    labels = attack_matches["label"].astype("object").to_numpy(copy=True)
    statuses = attack_matches["match_status"].astype("object").to_numpy(copy=True)
    directions = (
        attack_matches["match_direction"].astype("object").to_numpy(copy=True)
    )
    differences = attack_matches["match_time_diff_ms"].to_numpy(
        dtype="float64",
        na_value=np.nan,
    ).copy()
    binary_labels = np.full(len(flows), pd.NA, dtype=object)

    accepted_attacks = ~pd.isna(labels)
    wildcard_attacks = accepted_attacks & pd.Series(statuses).str.startswith(
        "matched_protocol_wildcard",
        na=False,
    ).to_numpy()
    statuses[accepted_attacks] = "attack_matched"
    statuses[wildcard_attacks] = "attack_matched_protocol_wildcard"
    binary_labels[accepted_attacks] = "ATTACK"

    remaining_rows = np.flatnonzero(statuses == "unmatched")
    if len(remaining_rows):
        print(
            "\nEtapa 2/2: procurando correspondências benignas "
            f"explícitas entre {len(remaining_rows)} fluxos sem ataque..."
        )
        benign_matches = benign_matcher(
            flows.iloc[remaining_rows],
            benign_labels,
            time_tolerance,
        )
        accepted_benign = benign_matches["label"].notna().to_numpy()
        local_rows = benign_matches["_flow_row_id"].to_numpy(dtype=np.int64)
        benign_rows = remaining_rows[local_rows[accepted_benign]]

        labels[benign_rows] = "BENIGN"
        binary_labels[benign_rows] = "BENIGN"
        statuses[benign_rows] = "benign_matched"
        directions[benign_rows] = benign_matches.loc[
            accepted_benign, "match_direction"
        ].to_numpy()
        differences[benign_rows] = benign_matches.loc[
            accepted_benign, "match_time_diff_ms"
        ].to_numpy(dtype="float64", na_value=np.nan)

        ambiguous_benign = benign_matches["match_status"].str.startswith(
            "ambiguous",
            na=False,
        ).to_numpy()
        if ambiguous_benign.any():
            ambiguous_rows = remaining_rows[local_rows[ambiguous_benign]]
            statuses[ambiguous_rows] = "ambiguous_benign"

    result = flows.copy()
    result["label"] = labels
    result["match_status"] = statuses
    result["match_direction"] = directions
    result["match_time_diff_ms"] = differences
    result["label_binary"] = binary_labels
    return result


def match_cic_flows(flows, labels, time_tolerance):
    """Aplica ataques-primeiro ao CICIDS2017 com proximidade dos inícios."""
    attack_labels, benign_labels = _split_labels(labels)
    print("\nEtapa 1/2: procurando correspondências de ataque...")
    attack_matches = match_flows_simple(
        flows,
        attack_labels,
        time_tolerance,
    )
    return _finish_attacks_first(
        flows,
        attack_matches,
        benign_labels,
        match_flows_simple,
        time_tolerance,
    )


def match_unsw_flows(flows, labels, time_tolerance):
    """Aplica ataques-primeiro ao UNSW-NB15 usando intervalos completos."""
    attack_labels, benign_labels = _split_labels(labels)
    print("\nEtapa 1/2: procurando correspondências de ataque...")
    attack_matches = match_flows_by_interval_simple(
        flows,
        attack_labels,
        time_tolerance,
    )
    attack_matches = _apply_unsw_protocol_fallback(
        attack_matches,
        flows,
        attack_labels,
        time_tolerance,
    )
    return _finish_attacks_first(
        flows,
        attack_matches,
        benign_labels,
        match_any_interval_simple,
        time_tolerance,
    )


def match_iot23_flows(flows, labels, time_tolerance):
    """Aplica ataques-primeiro ao IoT-23 usando intervalos completos."""
    attack_labels, benign_labels = _split_labels(labels)
    print("\nEtapa 1/2: procurando correspondências de ataque...")
    attack_matches = match_flows_by_interval_simple(
        flows,
        attack_labels,
        time_tolerance,
    )
    return _finish_attacks_first(
        flows,
        attack_matches,
        benign_labels,
        match_any_interval_simple,
        time_tolerance,
    )


def match_flows_for_dataset(dataset_name, flows, labels, time_tolerance):
    """Seleciona, em uma camada única, a política de matching do dataset."""
    if dataset_name == "cicids2017":
        return match_cic_flows(flows, labels, time_tolerance)
    elif dataset_name == "unsw_nb15":
        return match_unsw_flows(flows, labels, time_tolerance)
    elif dataset_name == "iot23":
        return match_iot23_flows(flows, labels, time_tolerance)
    elif dataset_name == "bot_iot":
        pass
    else:
        raise ValueError(f"Dataset sem política de matching: {dataset_name}")
