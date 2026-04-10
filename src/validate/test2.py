import pandas as pd
from src.configs.paths import PROCESSED_DATA_DIR, INTERMEDIATE_DATA_DIR
import pandas as pd
from src.configs.paths import PROCESSED_DATA_DIR


"""

    questionamento: aqueles que não conseguem o match com timestamp
        situação 1: existe apenas 1 registro identificador em cada dataset. match seguro (provavelmente)
        situação 2: existe mais de 1 registro identificador nos datasets.
            Se label_x ocorrer acima de x% (ex.70): atribui a maior ocorrência (algo assim seria aceitável)
            Do contrário seria um registro duvidoso
"""



def analise_identifier(dataset_path):
    """
    analisa a quantidade de identificadores únicos no dataset
    """
    print("=" * 60)
    print("analise de identificadores")
    print("=" * 60)

    df = pd.read_parquet(dataset_path)
    print(f"Total de flows: {len(df)}")

    # Cardinalidade
    unique_flow_keys = df['flow_key'].nunique()
    print(f"\nCARDINALIDADE:")
    print(f"- flow_keys distintas: {unique_flow_keys}")

    # Chaves com 1 ocorrencia vs multiplas ocorrencias
    ocorrencias_por_chave = df['flow_key'].value_counts()

    chaves_1_ocorrencia = ocorrencias_por_chave[ocorrencias_por_chave == 1]
    chaves_mais_1_ocorrencia = ocorrencias_por_chave[ocorrencias_por_chave > 1]

    qtd_chaves_1 = len(chaves_1_ocorrencia)
    qtd_chaves_mais_1 = len(chaves_mais_1_ocorrencia)

    print(f"\nDISTRIBUICAO DE OCORRENCIAS:")
    print(f"- Chaves com 1 ocorrencia: {qtd_chaves_1} ({qtd_chaves_1 / unique_flow_keys * 100:.2f}% das chaves)")
    print(
        f"- Chaves com mais de 1 ocorrencia: {qtd_chaves_mais_1} ({qtd_chaves_mais_1 / unique_flow_keys * 100:.2f}% das chaves)")
    print(
        f"  -> Estas {qtd_chaves_mais_1} chaves respondem por {chaves_mais_1_ocorrencia.sum()} flows ({chaves_mais_1_ocorrencia.sum() / len(df) * 100:.2f}% do total)")

    # 3. Das chaves com mais de 1 ocorrencia, quantas tem mais de 1 label diferente?
    df_multiplas = df[df['flow_key'].isin(chaves_mais_1_ocorrencia.index)]

    labels_por_chave_multipla = df_multiplas.groupby('flow_key')['label'].nunique()

    chaves_conflitantes = labels_por_chave_multipla[labels_por_chave_multipla > 1]

    print(f"\nANALISE DE CONFLITOS (apenas chaves com >1 ocorrencia):")
    print(f"- Chaves com mais de 1 ocorrencia e MAIS DE 1 LABEL diferente: {len(chaves_conflitantes)}")
    print(
        f"- Percentual sobre chaves com multiplas ocorrencias: {len(chaves_conflitantes) / qtd_chaves_mais_1 * 100:.2f}%")
    print(f"- Percentual sobre total de chaves: {len(chaves_conflitantes) / unique_flow_keys * 100:.2f}%")

    if len(chaves_conflitantes) > 0:
        print(f"\n   Exemplo de chave conflitante:")
        exemplo = chaves_conflitantes.index[0]
        exemplo_df = df[df['flow_key'] == exemplo]
        print(f"   Chave: {exemplo}")
        print("   Labels encontrados:")
        for label in exemplo_df['label'].unique():
            count = len(exemplo_df[exemplo_df['label'] == label])
            print(f"      - {label}: {count} ocorrencias")
    else:
        print(f"\n   Nenhuma chave com multiplas ocorrencias apresenta labels diferentes!")

    # 4. Resumo final
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"Total de chaves distintas: {unique_flow_keys}")
    print(f"Chaves com 1 ocorrencia: {qtd_chaves_1}")
    print(f"Chaves com >1 ocorrencia: {qtd_chaves_mais_1}")
    print(f"Destas, com labels diferentes: {len(chaves_conflitantes)}")
    print("=" * 60)

    return {
        'total_chaves': unique_flow_keys,
        'chaves_1_ocorrencia': qtd_chaves_1,
        'chaves_mais_1_ocorrencia': qtd_chaves_mais_1,
        'chaves_conflitantes': len(chaves_conflitantes)
    }


if __name__ == "__main__":
    dataset_name = "cicids2017"
    scenario = "friday"

    dataset_path = f"{PROCESSED_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"
    resultado = analise_identifier(dataset_path)



"""

    questionamento: aqueles que não conseguem o match com timestamp
        situação 1: existe apenas 1 registro identificador em cada dataset. match seguro (provavelmente)
        situação 2: existe mais de 1 registro identificador nos datasets.
            Se label_x ocorrer acima de x% (ex.70): atribui a maior ocorrência (algo assim seria aceitável)
            Do contrário seria um registro duvidoso
"""



def analise_identifier(dataset_path):
    """
    analisa a quantidade de identificadores únicos no dataset
    """
    print("=" * 60)
    print("analise de identificadores")
    print("=" * 60)

    df = pd.read_parquet(dataset_path)
    print(f"Total de flows: {len(df)}")

    # Cardinalidade
    unique_flow_keys = df['flow_key'].nunique()
    print(f"\nCARDINALIDADE:")
    print(f"- flow_keys distintas: {unique_flow_keys}")

    # Chaves com 1 ocorrencia vs multiplas ocorrencias
    ocorrencias_por_chave = df['flow_key'].value_counts()

    chaves_1_ocorrencia = ocorrencias_por_chave[ocorrencias_por_chave == 1]
    chaves_mais_1_ocorrencia = ocorrencias_por_chave[ocorrencias_por_chave > 1]

    qtd_chaves_1 = len(chaves_1_ocorrencia)
    qtd_chaves_mais_1 = len(chaves_mais_1_ocorrencia)

    print(f"\nDISTRIBUICAO DE OCORRENCIAS:")
    print(f"- Chaves com 1 ocorrencia: {qtd_chaves_1} ({qtd_chaves_1 / unique_flow_keys * 100:.2f}% das chaves)")
    print(
        f"- Chaves com mais de 1 ocorrencia: {qtd_chaves_mais_1} ({qtd_chaves_mais_1 / unique_flow_keys * 100:.2f}% das chaves)")
    print(
        f"  -> Estas {qtd_chaves_mais_1} chaves respondem por {chaves_mais_1_ocorrencia.sum()} flows ({chaves_mais_1_ocorrencia.sum() / len(df) * 100:.2f}% do total)")

    # 3. Das chaves com mais de 1 ocorrencia, quantas tem mais de 1 label diferente?
    df_multiplas = df[df['flow_key'].isin(chaves_mais_1_ocorrencia.index)]

    labels_por_chave_multipla = df_multiplas.groupby('flow_key')['label'].nunique()

    chaves_conflitantes = labels_por_chave_multipla[labels_por_chave_multipla > 1]

    print(f"\nANALISE DE CONFLITOS (apenas chaves com >1 ocorrencia):")
    print(f"- Chaves com mais de 1 ocorrencia e MAIS DE 1 LABEL diferente: {len(chaves_conflitantes)}")
    print(
        f"- Percentual sobre chaves com multiplas ocorrencias: {len(chaves_conflitantes) / qtd_chaves_mais_1 * 100:.2f}%")
    print(f"- Percentual sobre total de chaves: {len(chaves_conflitantes) / unique_flow_keys * 100:.2f}%")

    if len(chaves_conflitantes) > 0:
        print(f"\n   Exemplo de chave conflitante:")
        exemplo = chaves_conflitantes.index[0]
        exemplo_df = df[df['flow_key'] == exemplo]
        print(f"   Chave: {exemplo}")
        print("   Labels encontrados:")
        for label in exemplo_df['label'].unique():
            count = len(exemplo_df[exemplo_df['label'] == label])
            print(f"      - {label}: {count} ocorrencias")
    else:
        print(f"\n   Nenhuma chave com multiplas ocorrencias apresenta labels diferentes!")

    # 4. Resumo final
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"Total de chaves distintas: {unique_flow_keys}")
    print(f"Chaves com 1 ocorrencia: {qtd_chaves_1}")
    print(f"Chaves com >1 ocorrencia: {qtd_chaves_mais_1}")
    print(f"Destas, com labels diferentes: {len(chaves_conflitantes)}")
    print("=" * 60)

    return {
        'total_chaves': unique_flow_keys,
        'chaves_1_ocorrencia': qtd_chaves_1,
        'chaves_mais_1_ocorrencia': qtd_chaves_mais_1,
        'chaves_conflitantes': len(chaves_conflitantes)
    }


if __name__ == "__main__":
    dataset_name = "cicids2017"
    scenario = "friday"

    dataset_path = f"{INTERMEDIATE_DATA_DIR}/{dataset_name}/{scenario}_labels.parquet"
    resultado = analise_identifier(dataset_path)
