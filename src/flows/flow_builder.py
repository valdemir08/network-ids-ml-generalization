from nfstream import NFStreamer


def build_flows_from_pcap(pcap_path):

    streamer = NFStreamer(
        source=str(pcap_path),
        statistical_analysis=True,
        idle_timeout=120,
        active_timeout=120,
        accounting_mode=1,
    )

    return streamer.to_pandas()