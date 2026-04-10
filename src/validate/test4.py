from src.utils.detail import detail_dataset
from src.configs.paths import *


dataset_name = "cicids2017"
scenario = "friday"
path_file_processed = f"{PROCESSED_DATA_DIR}/{dataset_name}/{scenario}.parquet"
path_file_intermediate = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"



if __name__ == "__main__":
    print("Dataset de entrada. Gerado através dos CSVs CIC")

    detail_dataset(path_file_intermediate)

    print("\nDataset processado")
    detail_dataset(path_file_processed)