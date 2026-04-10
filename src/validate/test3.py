import pandas as pd
from src.configs.paths import INTERMEDIATE_DATA_DIR


def analyze_flow_duration_distribution(
    parquet_name,
    duration_col="Flow Duration"
):
    """
 distribuição percentual das durações dos flows.
    """

    parquet_path = INTERMEDIATE_DATA_DIR / "cicids2017"/parquet_name

    df = pd.read_parquet(parquet_path)

    if duration_col not in df.columns:
        raise ValueError(f"Coluna '{duration_col}' não encontrada.")

    durations = pd.to_numeric(df[duration_col], errors="coerce").dropna()

    print(f"Total de registros: {len(durations)}")

    # -------------------------
    # Faixas (em microssegundos)
    # -------------------------
    bins = [
        0,
        1_000,         # até 1 ms
        10_000,        # até 10 ms
        100_000,       # até 100 ms
        1_000_000,     # até 1 s
        10_000_000,    # até 10 s
        60_000_000,    # até 1 min
        120_000_000,   # até 2 min
        300_000_000,   # até 5 min
        max(durations.max() + 1, 300_000_001)  # garante aumento
    ]

    labels = [
        "0-1ms",
        "1-10ms",
        "10-100ms",
        "100ms-1s",
        "1s-10s",
        "10s-1min",
        "1-2min",
        "2-5min",
        "5min+"
    ]

    bins_cut = pd.cut(
        durations,
        bins=bins,
        labels=labels,
        include_lowest=True
    )

    counts = bins_cut.value_counts().sort_index()
    percentages = (counts / len(durations)) * 100

    result = pd.DataFrame({
        "countagem": counts,
        "porcentagem": percentages.round(2)
    })

    print("\n===== DISTRIBUIÇÃO DE DURAÇÃO =====")
    print(result)

    max_duration = durations.max()

    print(f"Máximo (microsegundos): {max_duration}")
    print(f"Máximo (segundos): {max_duration / 1_000_000:.2f}s")
    print(f"Máximo (minutos): {max_duration / 60_000_000:.2f}min")

if __name__ == "__main__":
    analyze_flow_duration_distribution("friday_labels.parquet")