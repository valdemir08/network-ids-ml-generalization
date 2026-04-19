
def select_dataset(datasets):
    dataset_names = list(datasets.keys())

    print("Escolha um dataset:")
    for i, name in enumerate(dataset_names):
        print(f"{i} - {name}")

    choice = int(input(">>> "))

    if choice < 0 or choice >= len(dataset_names):
        raise ValueError("Opção inválida")

    dataset_name = dataset_names[choice]
    dataset = datasets[dataset_name]

    return dataset_name, dataset