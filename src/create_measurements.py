import sys
import random
import time
from pathlib import Path
from statistics import mean
from typing import Iterable, List

# --- Caminhos ---------------------------------------------------------------

CSV_FILE_PATH = Path(r"C:\Users\Administrador\Documents\GitHub\estudos_4\data\weather_stations.csv")
TXT_FILE_PATH = Path("data/weather_stations.txt")   # criaremos a pasta se não existir
TXT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

# --- Utilidades -------------------------------------------------------------

def convert_bytes(num: float) -> str:
    """Converte bytes em valor legível (KB, MB, ...)."""
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:,.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"

# --- Ingestão ---------------------------------------------------------------

def build_station_names(path: Path) -> List[str]:
    """Lê o CSV de estações e devolve nomes únicos sem comentários."""
    station_names: List[str] = []
    with path.open(encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            station_names.append(line.split(";")[0])
    return list(set(station_names))

# --- Estimativa de tamanho --------------------------------------------------

def estimate_file_size(station_names: Iterable[str], num_rows: int) -> str:
    avg_len = mean(len(name) for name in station_names)
    payload_len = len(";-123.4")       # separador + valor
    newline_len = 2 if sys.platform.startswith("win") else 1
    per_record = avg_len + payload_len + newline_len
    total = per_record * num_rows
    return (f"Tamanho aproximado: {convert_bytes(total)} "
            f"({total:,.0f} bytes).")

# --- Gerador de dados sintéticos -------------------------------------------

def build_test_data(
    names: List[str],
    rows: int,
    outfile: Path,
    batch_size: int = 10_000,
    t_min: float = -99.9,
    t_max: float = 99.9,
) -> None:
    """Gera arquivo sintético de leituras de temperatura."""
    t0 = time.time()
    print("Gerando arquivo...")

    full_batches, remainder = divmod(rows, batch_size)

    try:
        with outfile.open("w", encoding="utf-8") as f:
            for _ in range(full_batches):
                batch = random.choices(names, k=batch_size)
                f.writelines(
                    f"{st};{random.uniform(t_min, t_max):.1f}\n"
                    for st in batch
                )
            # grava linhas restantes
            if remainder:
                batch = random.choices(names, k=remainder)
                f.writelines(
                    f"{st};{random.uniform(t_min, t_max):.1f}\n"
                    for st in batch
                )
    except OSError as e:
        print("Erro de E/S:", e)
        sys.exit(1)

    print(f"Concluído em {time.time() - t0:.1f} s")

# --- Script principal -------------------------------------------------------

def main() -> None:
    station_names = build_station_names(CSV_FILE_PATH)
    print(f"{len(station_names)} estações carregadas.")

    num_rows = 1_000_000_000  # exemplo mais realista
    print(estimate_file_size(station_names, num_rows))

    build_test_data(station_names, num_rows, TXT_FILE_PATH)

if __name__ == "__main__":
    main()
