# verificar tamanhos de labels csv e flows enfstream
from src.configs.paths import INTERMEDIATE_DATA_DIR
from src.io.io_utils import load_parquet

dir = INTERMEDIATE_DATA_DIR/"cicids2017/"

arq = [arq.name for arq in dir.iterdir()]

for item in arq:
    df = load_parquet(dir/item)
    print(f"{item} - {len(df)}")
