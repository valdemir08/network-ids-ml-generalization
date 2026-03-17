import numpy as np
import pandas as pd


def clean_column_names(df):
    df.columns = df.columns.str.strip()
    return df


def replace_infinities(df):
    df.replace([np.inf, -np.inf], np.nan)
    return df

def remove_duplicated_cols(df):
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def remove_duplicated_rows(df):
    df = df.drop_duplicates()
    return df

def dataset_cleanup(df):
    #verificar necessidade de condicionais para outros datasets
    #lembrete: estudar metodologia de limpeza dos datasets de trabalhos relacionados
    #remover comentário quando estiver pronto

    df = clean_column_names(df)
    df = replace_infinities(df)
    df = remove_duplicated_cols(df)
    df = remove_duplicated_rows(df)

    return df

# função para cicids2017, provavelmente será necessário em mais bases
def convert_labels_timestamp(df, timestamp_col="Timestamp"):
    """
    Converte a coluna Timestamp do csv para datetime e
    cria ts_ms em milissegundos desde epoch.
    """
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], format="%m/%d/%Y %H:%M")
    df["ts_ms"] = (df[timestamp_col].astype("int64") // 1_000_000)
    return df