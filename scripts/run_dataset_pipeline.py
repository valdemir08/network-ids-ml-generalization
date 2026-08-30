from src.pipelines.prepare_labels import prepare_labels
from src.pipelines.extract_from_pcap import extract_from_pcap
from src.pipelines.build_dataset import merge_flows_and_labels
from src.pipelines.merge_dataset import merge_processed_dataset
from src.reports.dataset_report import generate_dataset_report
from src.configs.datasets import DATASETS
from src.utils.select_dataset import select_dataset


if __name__ == "__main__":
    dataset_name, dataset = select_dataset(DATASETS)
    scenarios = dataset.get(
        "pipeline_scenarios",
        list(dataset["scenarios"]),
    )

    print("Cenários que serão processados:")
    for scenario in scenarios:
        print(f"- {scenario}")

    for scenario in scenarios:
        print(f"\n {dataset_name} {scenario}")
        prepare_labels(dataset_name, scenario)
        extract_from_pcap(dataset_name, scenario)
        merge_flows_and_labels(dataset_name, scenario)

    merge_processed_dataset(dataset_name)
    generate_dataset_report(dataset_name, scenarios)
