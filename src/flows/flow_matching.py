import pandas as pd
import numpy as np


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


def match_flows_simple(flows, labels, time_tolerance=10000):
    """
    casar flows com labels.
    retorna coluna 'label' para cada flow.
    """
    print("Iniciando matching...")

    # Usar a coluna correta de timestamp nos flows
    flows_subset = flows[['flow_key', 'flow_key_rev', 'bidirectional_first_seen_ms']].copy()
    labels_subset = labels[['flow_key', 'flow_key_rev', 'ts_ms', 'label']].copy()

    # padronizar subset usado na func
    flows_subset = flows_subset.rename(columns={'bidirectional_first_seen_ms': 'ts_ms'})
    """
    # DEBUG - verificar timestamps
    print("\n=== DEBUG TIMESTAMPS ===")
    print(f"Flows ts_ms - min: {flows_subset['ts_ms'].min()}, max: {flows_subset['ts_ms'].max()}")
    print(f"Labels ts_ms - min: {labels_subset['ts_ms'].min()}, max: {labels_subset['ts_ms'].max()}")

    # DEBUG - verificar amostra das chaves
    print("\n=== DEBUG CHAVES ===")
    print(f"Amostra flow_key (flows): {flows_subset['flow_key'].iloc[0]}")
    print(f"Amostra flow_key (labels): {labels_subset['flow_key'].iloc[0]}")
    """








    print(f"Flows: {len(flows_subset)} | Labels: {len(labels_subset)}")

    # criando coluna nova com timestamp arredondado para facilitar o match
    flows_subset['ts_round'] = (flows_subset['ts_ms'] / time_tolerance).round() * time_tolerance
    labels_subset['ts_round'] = (labels_subset['ts_ms'] / time_tolerance).round() * time_tolerance
    #ex: 1499428790123 / 10000 = 149942879.0123 -> round(149942879.0123) = 149942879 -> 149942879 * 10000 = 1499428790000

    # criar chave composta: flow_key + timestamp_round
    flows_subset['match_key'] = flows_subset['flow_key'] + '|' + flows_subset['ts_round'].astype(str)
    labels_subset['match_key'] = labels_subset['flow_key'] + '|' + labels_subset['ts_round'].astype(str)

    # o mesmo para chave reversa
    flows_subset['match_key_rev'] = flows_subset['flow_key_rev'] + '|' + flows_subset['ts_round'].astype(str)
    labels_subset['match_key_rev'] = labels_subset['flow_key_rev'] + '|' + labels_subset['ts_round'].astype(str)

    #subsets prontos para match
    #-------------------------------------------------
    # 1 tentativa - match com flow_key
    print("Tentando match com flow_key...")

    #cria dicionário com match_key : label
    label_dict = {}
    for key, label in zip(labels_subset['match_key'], labels_subset['label']):
        if key not in label_dict:
            label_dict[key] = label

    # mapear labels para flows
    # .map(label_dict) procura cada chave e retorna o valor correspondente.
    # por fim, entra na coluna label
    flows_subset['label'] = flows_subset['match_key'].map(label_dict)



    #########################
    qtd_matches_witch_mk = flows_subset['label'].notna().sum()
    total_size = len(flows_subset)

    print(f"Matches com flow_key: {qtd_matches_witch_mk} de {total_size} ({qtd_matches_witch_mk / total_size * 100:.2f}%)")
    ############################

    # -------------------------------------------------
    # 2 tentativa - para itens que não houve o match, tentar com flow_key_rev
    # .isna() cria uma máscara booleana indicando quais linhas não tem label (true onde não há label)
    unmatched = flows_subset['label'].isna()
    if unmatched.any():
        print(f"Tentando match reverso para {unmatched.sum()} flows...")

        # dicionário para chave reversa
        label_dict_rev = {}
        for key, label in zip(labels_subset['match_key_rev'], labels_subset['label']):
            if key not in label_dict_rev:
                label_dict_rev[key] = label

        # mapear usando chave reversa
        #.loc seleciona apenas as linhas que ainda não têm label
        flows_subset.loc[unmatched, 'label'] = flows_subset.loc[unmatched, 'match_key_rev'].map(label_dict_rev)

    # e tentativa - match sem timestamp (apenas flow_key)
    unmatched = flows_subset['label'].isna()
    if unmatched.any():
        print(f"Tentando match sem timestamp para {unmatched.sum()} flows...")

        # dicionário sem timestamp. verificar forma de pegar o label mais frequente para cada flow_key
        label_dict_no_ts = {}
        for key, label in zip(labels_subset['flow_key'], labels_subset['label']):
            if key not in label_dict_no_ts:
                label_dict_no_ts[key] = label

        flows_subset.loc[unmatched, 'label'] = flows_subset.loc[unmatched, 'flow_key'].map(label_dict_no_ts)

        # tentar com reverso sem timestamp
        unmatched = flows_subset['label'].isna()
        if unmatched.any():
            label_dict_rev_no_ts = {}
            for key, label in zip(labels_subset['flow_key_rev'], labels_subset['label']):
                if key not in label_dict_rev_no_ts:
                    label_dict_rev_no_ts[key] = label

            flows_subset.loc[unmatched, 'label'] = flows_subset.loc[unmatched, 'flow_key_rev'].map(label_dict_rev_no_ts)

    # Estatísticas
    matched = flows_subset['label'].notna().sum()
    print(f"Match final: {matched} flows com label ({matched / len(flows_subset) * 100:.2f}%)")

    return flows_subset['label']


def match_flows(flows, labels, time_tolerance=10000):
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