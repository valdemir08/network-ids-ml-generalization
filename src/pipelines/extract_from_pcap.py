from src.flows.flow_builder import build_flows_from_pcap
from src.configs.datasets import DATASETS
from src.configs.paths import INTERMEDIATE_DATA_DIR


def extract_from_pcap(dataset_name, scenario):
    print(f"Extraindo flows do pcap para {dataset_name} {scenario}")

    dataset = DATASETS[dataset_name]

    root = dataset["root"]
    pcap_dir = root / dataset["pcap_dir"]

    scenario_cfg = dataset["scenarios"][scenario]

    output_dir = INTERMEDIATE_DATA_DIR / dataset_name
    output_dir.mkdir(parents=True, exist_ok=True)

    for pcap_file in scenario_cfg["pcaps"]:

        pcap_path = pcap_dir / pcap_file
        #é dito que pcaps grandes podem gerar problemas com nfstream, sendo ideal o try
        try:
            flows = build_flows_from_pcap(pcap_path)
            print(f"Extraído {len(flows)} flows")
        except Exception as e:
            print(f"Error processing {pcap_path}: {e}")
            continue

        output_file = output_dir / f"{scenario}_flows.parquet"

        flows.to_parquet(output_file)

if __name__ == "__main__":

    extract_from_pcap(
        dataset_name="cicids2017",
        scenario="friday"
    )
