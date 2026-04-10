import pandas as pd
import time
import numpy as np

def print_match_stats(step_name, matched_count, total_count):
    perc = (matched_count / total_count) * 100 if total_count > 0 else 0
    print(f"[{step_name}] {matched_count}/{total_count} ({perc:.2f}%)")

def create_flow_key(df):
    """
    Criar flow_key em qualquer dataset
    (desde que tenha colunas src_ip, dst_ip, src_port, dst_port, protocol)
    """
    df["flow_key"] = (
        df["src_ip"].astype(str) + "|" +
        df["dst_ip"].astype(str) + "|" +
        df["src_port"].astype(str) + "|" +
        df["dst_port"].astype(str) + "|" +
        df["protocol"].astype(str)
    )
    return df


def create_bidirectional_flow_key(df):
    """
    cria chaves bidirecionais para casar fluxos invertidos.
    flow_key: src->dst
    flow_key_rev: dst->src

    medida de segurança para não perder flows que estejam reversos no csv (depende do csv)
    """

    # Chave reversa (dst->src)
    df['flow_key_rev'] = (
            df['dst_ip'].astype(str) + "|" +
            df['src_ip'].astype(str) + "|" +
            df['dst_port'].astype(str) + "|" +
            df['src_port'].astype(str) + "|" +
            df['protocol'].astype(str)
    )

    return df


def match_flows_simple(flows, labels, time_tolerance):
    """
    casar flows com labels.
    retorna coluna 'label' para cada flow.
    """
    print("\n\n\n")
    print("Iniciando matching...")
    print(f"Tolerância: {time_tolerance / 1000:.1f}s")

    # Usar a coluna correta de timestamp nos flows
    flows_subset = flows[['flow_key', 'flow_key_rev', 'bidirectional_first_seen_ms']].copy()
    flows_subset = flows_subset.rename(columns={'bidirectional_first_seen_ms': 'ts_ms'})
    labels_subset = labels[['flow_key', 'flow_key_rev', 'ts_ms', 'label']].copy()

    # preparar timestamps
    flows_subset["ts_ms"] = pd.to_numeric(flows_subset["ts_ms"], errors="coerce")
    labels_subset["ts_ms"] = pd.to_numeric(labels_subset["ts_ms"], errors="coerce")

    # Remover NaNs
    flows_subset = flows_subset.dropna(subset=["ts_ms", "flow_key"])
    labels_subset = labels_subset.dropna(subset=["ts_ms", "flow_key"])

    # Converter para string
    flows_subset['flow_key'] = flows_subset['flow_key'].astype(str)
    flows_subset['flow_key_rev'] = flows_subset['flow_key_rev'].astype(str)
    labels_subset['flow_key'] = labels_subset['flow_key'].astype(str)
    labels_subset['flow_key_rev'] = labels_subset['flow_key_rev'].astype(str)


    # separando em 2 sets para melhoria em performace
    flows_with_unique_key, flows_with_duplicate_keys = split_by_key_count(flows_subset, "flow_key")
    labels_with_unique_key, labels_with_duplicate_keys = split_by_key_count(labels_subset, "flow_key")

####################

    # ordenar.
    flows_with_unique_key = flows_with_unique_key.sort_values(["flow_key", "ts_ms"]).reset_index(drop=True)
    labels_with_unique_key = labels_with_unique_key.sort_values(["flow_key", "ts_ms"]).reset_index(drop=True)


    #ordenar para possíveis melhorias futuras na busca da solução "manual"
    flows_with_duplicate_keys = flows_with_duplicate_keys.sort_values(["flow_key", "ts_ms"]).reset_index(drop=True)
    labels_with_duplicate_keys = labels_with_duplicate_keys.sort_values(["flow_key", "ts_ms"]).reset_index(drop=True)

###########################################################
    t0 = time.perf_counter()
    print("match com flow key única")
    #

    matched_unique = flows_with_unique_key.merge(
        labels_with_unique_key[['flow_key', 'ts_ms', 'label']],
        on='flow_key',
        how='left',
        suffixes=('', '_label')
    )

    # criar diferença de tempo
    matched_unique['time_diff'] = abs(matched_unique['ts_ms'] - matched_unique['ts_ms_label'])
    matched_unique['label'] = matched_unique['label'].where(matched_unique['time_diff'] <= time_tolerance)
    # limpar colunas temporárias
    matched_unique = matched_unique.drop(columns=['ts_ms_label', 'time_diff'])

    t1 = time.perf_counter()
    print(f"merge key unique: {t1 - t0:.4f}s")
    matched_unique_count = matched_unique['label'].count()
    print(f"merge key unique qtd: {matched_unique_count}")

    t0 = time.perf_counter()
    unmatched_unique_mask = matched_unique["label"].isna()

    if unmatched_unique_mask.any():
        t0 = time.perf_counter()
        flows_unmatched = matched_unique[unmatched_unique_mask].copy()

        # merge por chave reversa
        matched_rev = flows_unmatched.merge(
            labels_with_unique_key[['flow_key_rev', 'ts_ms', 'label']],
            left_on='flow_key_rev',
            right_on='flow_key_rev',
            how='left',
            suffixes=('', '_label')
        )

        # diferença de tempo
        matched_rev['time_diff'] = abs(matched_rev['ts_ms'] - matched_rev['ts_ms_label'])
        matched_rev['label_rev'] = matched_rev['label'].where(matched_rev['time_diff'] <= time_tolerance)
        # atualizar labels originais
        matched_unique.loc[unmatched_unique_mask, 'label'] = matched_rev['label_rev'].values

        t1 = time.perf_counter()
        print(f"merge unique reverso: {t1 - t0:.4f}s")
        matched_unique_count = matched_unique['label'].count()
        print(f"merge key rev unique qtd: {matched_unique_count}")
#############################################################

    print("match manual para flow_key múltipla")
    t0 = time.perf_counter()
    labels_resolved_duplicate_by_key = resolve_duplicate_matches(
        flows_with_duplicate_keys,
        labels_with_duplicate_keys,
        "flow_key",
        time_tolerance
    )

    matched_duplicate = flows_with_duplicate_keys.copy()
    matched_duplicate["label"] = labels_resolved_duplicate_by_key
    t1 = time.perf_counter()
    print(f"merge manual duplicate: {t1 - t0:.4f}s")
    t0 = time.perf_counter()
    unmatched_duplicate_mask = matched_duplicate["label"].isna()

    labels_resolved_duplicate_by_reverse_key = resolve_duplicate_matches(
        matched_duplicate[unmatched_duplicate_mask],
        labels_with_duplicate_keys,
        "flow_key_rev",
        time_tolerance
    )

    matched_duplicate.loc[unmatched_duplicate_mask, "label"] = labels_resolved_duplicate_by_reverse_key
    t1 = time.perf_counter()
    print(f"merge manual duplicate reverso: {t1 - t0:.4f}s")


    result = pd.concat([
        matched_unique,
        matched_duplicate
    ]).sort_index()

    return result['label']


def match_flows(flows, labels, time_tolerance):
    """
    retorna os flows originais com a coluna label adicionada
    """
    print(f"Flows: {len(flows)} | Labels: {len(labels)}")

    # matching
    labels_matched = match_flows_simple(flows, labels, time_tolerance)

    # adiciona ao DataFrame original
    result = flows.copy()
    result['label'] = labels_matched.values

    return result

def split_by_key_count(dataset, key):
    #cria series que quantifica occorrencias da key
    # como uma lista de key : quantidade, onde
    # os índices são as próprias keys
    counts = dataset[key].value_counts()

    # retorna todos os índices onde counts == 1. chaves únicas
    # filtro[op. pandas].indice
    unique_keys = counts[counts == 1].index
    # retorna todos os índices onde counts > 1. chaves duplicadas
    duplicate_keys = counts[counts > 1].index

    # separa os datasets
    dataset_unique = dataset[dataset[key].isin(unique_keys)]
    dataset_duplicate = dataset[dataset[key].isin(duplicate_keys)]
    # procurar por pandas bolean indexing na documentação
    return dataset_unique, dataset_duplicate


def resolve_duplicate_matches(flows_df, labels_df, key_col, time_tolerance):

    if flows_df.empty or labels_df.empty:
        return [None] * len(flows_df)

    # reset index para preservar posição original
    flows_work = flows_df.reset_index(drop=False)
    flows_work = flows_work.rename(columns={'index': 'original_idx'})

    # merge para encontrar todos os candidatos
    merged = flows_work.merge(
        labels_df[[key_col, 'ts_ms', 'label']],
        on=key_col,
        how='left',
        suffixes=('_flow', '_label')
    )

    merged['time_diff'] = abs(merged['ts_ms_flow'] - merged['ts_ms_label'])
    merged = merged[merged['time_diff'] <= time_tolerance]

    if merged.empty:
        return [None] * len(flows_df)

    #verificar necessidade disso aqui
    label_consistency = merged.groupby('original_idx')['label'].agg(
        unique_labels=lambda x: x.nunique(),
        label=lambda x: x.iloc[0] if x.nunique() == 1 else None
    )


    results = [None] * len(flows_df)
    for idx, row in label_consistency.iterrows():
        if row['unique_labels'] == 1:  # só atribui se todas labels forem iguais
            # encontrar posição no array original
            results[idx] = row['label']

    return results