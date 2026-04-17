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
    # verificar necessidade de condicionais para outros datasets
    # lembrete: estudar metodologia de limpeza dos datasets de trabalhos relacionados
    # remover comentário quando estiver pronto

    df = clean_column_names(df)
    df = replace_infinities(df)
    df = remove_duplicated_cols(df)
    df = remove_duplicated_rows(df)

    return df


# função para cicids2017, provavelmente será necessário em mais bases
def convert_to_datetime(df, timestamp_col="Timestamp"):
    """
    Converte a coluna Timestamp do csv para datetime e
    """

    df[timestamp_col] = pd.to_datetime(
        df[timestamp_col],
        format="%m/%d/%Y %H:%M",
        errors="coerce")


    return df

def create_column_ts_ms(df, timestamp_col="Timestamp"):
    """
    cria ts_ms em milissegundos desde epoch.
    """
    df = df.copy()

    df["ts_ms"] = df[timestamp_col].astype("int64") // 1_000

    return df


def padronize_cols_name(df, column_mapping: dict):
    """
    padroniza nomes das colunas conforme mapeamento fornecido.
    """
    return df.rename(columns=column_mapping)


def adjust_time_for_cic_datasets(df, column_name):
    """
    Os datasets do CIC apresentam uma limitação na coluna de tempo (Timestamp).

    O problema:
    Os horários estão no formato de 12 horas, porém não há indicação explícita de AM/PM,
    o que torna os valores ambíguos.

    De acordo com a documentação oficial, as capturas foram realizadas em horário comercial,
    no intervalo entre 09:00 e 17:00.

    Solução:
    Considerando esse intervalo, valores de hora entre 1 e 5 são interpretados como pertencentes
    ao período da tarde (13:00–17:00). Portanto, nesses casos, é aplicado um ajuste de +12 horas
    para normalizar os timestamps para o formato de 24 horas.
    """
    df = df.copy()

    df[column_name] = pd.to_datetime(df[column_name], errors="coerce")

    mask = df[column_name].dt.hour.between(1, 5)

    df.loc[mask, column_name] = df.loc[mask, column_name] + pd.Timedelta(hours=12)

    return df

def apply_timezone_offset(df, column_name, offset_hours):
    df = df.copy()
    df[column_name] = df[column_name] + pd.Timedelta(hours=offset_hours)

    return df

def create_column_dataset_name(df, dataset_name):
    df = df.copy()
    df["dataset_name"] = dataset_name
    return df