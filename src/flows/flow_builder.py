import pandas as pd
from nfstream import NFStreamer


FLOW_STRING_COLUMNS = {
    "src_ip",
    "src_mac",
    "src_oui",
    "dst_ip",
    "dst_mac",
    "dst_oui",
    "application_name",
    "application_category_name",
    "requested_server_name",
    "client_fingerprint",
    "server_fingerprint",
    "user_agent",
    "content_type",
}


def _create_streamer(pcap_path):
    return NFStreamer(
        source=str(pcap_path),
        statistical_analysis=True,
        idle_timeout=10,
        active_timeout=120,
        accounting_mode=1,
    )


def build_flows_from_pcap(pcap_path):
    return _create_streamer(pcap_path).to_pandas()


def iter_flow_chunks_from_pcap(pcap_path, chunk_size):
    """Extrai fluxos em blocos para limitar o uso de memória."""
    if chunk_size <= 0:
        raise ValueError("chunk_size deve ser maior que zero")

    rows = []
    columns = None

    for flow in _create_streamer(pcap_path):
        if columns is None:
            columns = flow.keys()
        rows.append(flow.values())

        if len(rows) == chunk_size:
            yield _create_flow_dataframe(rows, columns)
            rows = []

    if rows:
        yield _create_flow_dataframe(rows, columns)


def _create_flow_dataframe(rows, columns):
    flows = pd.DataFrame.from_records(rows, columns=columns)
    for column in FLOW_STRING_COLUMNS.intersection(flows.columns):
        flows[column] = flows[column].astype("string").replace("", pd.NA)
    return flows
    return streamer.to_pandas()
