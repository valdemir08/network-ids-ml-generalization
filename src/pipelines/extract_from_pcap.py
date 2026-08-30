from src.flows.flow_builder import (
    build_flows_from_pcap,
    iter_flow_chunks_from_pcap,
)
from src.configs.datasets import DATASETS
from src.configs.paths import INTERMEDIATE_DATA_DIR


def get_flow_output_path(dataset_name, scenario, pcap_path):
    dataset = DATASETS[dataset_name]
    output_mode = dataset.get("flow_output_mode", "scenario")
    output_dir = INTERMEDIATE_DATA_DIR / dataset_name

    if output_mode == "scenario":
        output_file = output_dir / f"{scenario}_flows.parquet"
    elif output_mode == "per_pcap":
        output_dir = output_dir / scenario
        output_file = output_dir / f"{pcap_path.stem}_flows.parquet"
    else:
        raise ValueError(
            f"Modo de saída inválido para {dataset_name}: {output_mode}"
        )

    return output_file


def _extract_pcap_in_chunks(dataset_name, scenario, pcap_path, chunk_size):
    output_dir = INTERMEDIATE_DATA_DIR / dataset_name / scenario
    output_dir.mkdir(parents=True, exist_ok=True)
    total_flows = 0
    output_files = []

    for chunk_number, flows in enumerate(
        iter_flow_chunks_from_pcap(pcap_path, chunk_size),
        start=1,
    ):
        output_file = output_dir / f"{chunk_number:06d}_flows.parquet"
        flows.to_parquet(output_file, index=False)
        total_flows += len(flows)
        output_files.append(output_file)
        print(
            f"Bloco {chunk_number}: {len(flows)} fluxos; "
            f"total {total_flows}"
        )

    if not output_files:
        raise ValueError(f"Nenhum fluxo extraído de {pcap_path}")
    return output_files


def get_pcaps_to_process(scenario_cfg, selected_pcap=None):
    pcaps = scenario_cfg["pcaps"]

    if selected_pcap is None:
        return pcaps

    if selected_pcap not in pcaps:
        raise ValueError(f"PCAP não configurado no cenário: {selected_pcap}")

    return [selected_pcap]


def extract_from_pcap(
    dataset_name,
    scenario,
    selected_pcap=None,
    chunk_size=None,
):
    print(f"Extraindo flows do pcap para {dataset_name} {scenario}")

    dataset = DATASETS[dataset_name]

    root = dataset["root"]
    pcap_dir = root / dataset["pcap_dir"]

    scenario_cfg = dataset["scenarios"][scenario]
    output_mode = dataset.get("flow_output_mode", "scenario")
    pcaps_to_process = get_pcaps_to_process(
        scenario_cfg,
        selected_pcap,
    )

    for pcap_file in pcaps_to_process:

        pcap_path = pcap_dir / pcap_file
        if output_mode == "chunked":
            selected_chunk_size = (
                chunk_size
                if chunk_size is not None
                else dataset["flow_chunk_size"]
            )
            _extract_pcap_in_chunks(
                dataset_name,
                scenario,
                pcap_path,
                selected_chunk_size,
            )
            continue

        #é dito que pcaps grandes podem gerar problemas com nfstream, sendo ideal o try
        try:
            flows = build_flows_from_pcap(pcap_path)
            print(f"Extraído {len(flows)} flows")
        except Exception as e:
            print(f"Error processing {pcap_path}: {e}")
            continue

        output_file = get_flow_output_path(
            dataset_name,
            scenario,
            pcap_path,
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)

        flows.to_parquet(output_file, index=False)

if __name__ == "__main__":
    jobs = {
        index: job
        for index, job in enumerate(
            (
                (dataset_name, scenario)
                for dataset_name, dataset in DATASETS.items()
                for scenario in dataset["scenarios"]
            ),
            start=1,
        )
    }

    print("Escolha a extração:")

    for index, (dataset_name, scenario) in jobs.items():
        print(f"{index} - {dataset_name} / {scenario}")

    choice = int(input(">>> "))

    if choice not in jobs:
        raise ValueError("Opção inválida")

    dataset_name, scenario = jobs[choice]

    scenario_cfg = DATASETS[dataset_name]["scenarios"][scenario]
    configured_pcaps = scenario_cfg["pcaps"]

    print("Escolha os PCAPs:")
    print("0 - Processar todos")

    for index, pcap_file in enumerate(configured_pcaps, start=1):
        print(f"{index} - {pcap_file}")

    pcap_choice = int(input(">>> "))

    if pcap_choice < 0 or pcap_choice > len(configured_pcaps):
        raise ValueError("Opção de PCAP inválida")

    selected_pcap = (
        None
        if pcap_choice == 0
        else configured_pcaps[pcap_choice - 1]
    )

    extract_from_pcap(
        dataset_name=dataset_name,
        scenario=scenario,
        selected_pcap=selected_pcap,
    )
