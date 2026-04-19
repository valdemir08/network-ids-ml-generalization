from src.pipelines.prepare_labels import prepare_labels
from src.pipelines.extract_from_pcap import extract_from_pcap
from src.pipelines.build_dataset import merge_flows_and_labels
from src.pipelines.merge_dataset import merge_processed_dataset
from src.configs.datasets import DATASETS
from src.utils.select_dataset import select_dataset

if __name__ == "__main__":

    dataset_name, dataset = select_dataset(DATASETS)

    for scenario in dataset["scenarios"]:
        print(f"\n {dataset_name} {scenario}")
        prepare_labels(dataset_name, scenario)
        extract_from_pcap(dataset_name, scenario)
        merge_flows_and_labels(dataset_name, scenario)

    merge_processed_dataset(dataset_name)


"""
if __name__ == "__main__":
    prepare_labels("cicids2017", "wednesday")
"""