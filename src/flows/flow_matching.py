import pandas as pd

def create_label_flow_key(df):
    """
    Cria a coluna 'flow_key' no dataset de labels do cicids2017.
    """
    df = df.copy()
    df["flow_key"] = (
        df["Source IP"].astype(str) + "|" +
        df["Destination IP"].astype(str) + "|" +
        df["Source Port"].astype(str) + "|" +
        df["Destination Port"].astype(str) + "|" +
        df["Protocol"].astype(str)
    )
    return df


def create_nfstream_flow_key(df):
    """
    Cria a coluna 'flow_key' no dataset de flows do NFStream
    e adiciona a coluna 'ts_ms' baseada no bidirectional_first_seen_ms.
    """
    df = df.copy()
    df["flow_key"] = (
        df["src_ip"].astype(str) + "|" +
        df["dst_ip"].astype(str) + "|" +
        df["src_port"].astype(str) + "|" +
        df["dst_port"].astype(str) + "|" +
        df["protocol"].astype(str)
    )
    # timestamp em milissegundos
    df["ts_ms"] = df["bidirectional_first_seen_ms"]
    return df


def create_bidirectional_flow_key(df, src_ip_col, dst_ip_col, src_port_col, dst_port_col, proto_col):
    """
    Cria chaves bidirecionais para casar fluxos invertidos.
    """
    df = df.copy()
    key1 = (
        df[src_ip_col].astype(str) + "|" +
        df[dst_ip_col].astype(str) + "|" +
        df[src_port_col].astype(str) + "|" +
        df[dst_port_col].astype(str) + "|" +
        df[proto_col].astype(str)
    )
    key2 = (
        df[dst_ip_col].astype(str) + "|" +
        df[src_ip_col].astype(str) + "|" +
        df[dst_port_col].astype(str) + "|" +
        df[src_port_col].astype(str) + "|" +
        df[proto_col].astype(str)
    )
    df['flow_key'] = key1
    df['flow_key_rev'] = key2
    return df




def match_flows(flows, labels, time_tolerance=10000):
    """
    Casamento vetorizado de flows com labels usando flow_key e flow_key_rev.
    Considera tolerância de tempo em milissegundos.

    Retorna: flows com coluna 'Label' preenchida.
    """
    flows = flows.copy()
    labels = labels.copy()
    flows['Label'] = None

    # Merge normal (flow_key)
    merged = flows.merge(
        labels[['flow_key', 'ts_ms', 'Label']],
        on='flow_key',
        how='left',
        suffixes=('', '_label')
    )

    # Calcula diferença de tempo
    merged['time_diff'] = (merged['ts_ms_label'] - merged['ts_ms']).abs()

    # Mantém apenas correspondências dentro da tolerância
    merged.loc[merged['time_diff'] > time_tolerance, 'Label'] = None

    # Remove colunas auxiliares
    merged = merged.drop(columns=['ts_ms_label', 'time_diff'])

    # Para fluxos sem match, tenta flow_key_rev
    no_label = merged['Label'].isna()
    if no_label.any():
        flows_rev = merged.loc[no_label, flows.columns]
        merged_rev = flows_rev.merge(
            labels[['flow_key_rev', 'ts_ms', 'Label']],
            left_on='flow_key_rev',
            right_on='flow_key_rev',
            how='left',
            suffixes=('', '_label')
        )
        merged_rev['time_diff'] = (merged_rev['ts_ms_label'] - merged_rev['ts_ms']).abs()
        merged_rev.loc[merged_rev['time_diff'] > time_tolerance, 'Label'] = None
        merged_rev = merged_rev.drop(columns=['ts_ms_label', 'time_diff'])

        # Atualiza os labels na tabela original
        merged.loc[no_label, 'Label'] = merged_rev['Label'].values

    return merged