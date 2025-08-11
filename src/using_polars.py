import polars as pl
from pathlib import Path

DATA_PATH = Path("data/weather_stations.txt")      # saída do gerador

def create_polars_df(path: Path = DATA_PATH) -> pl.DataFrame:
    """
    Lê o arquivo gigante `weather_stations.txt` e devolve
    max/min/mean por estação, em modo streaming.
    """
    # 1) Ajustes globais – experimente valores diferentes no seu hardware
    pl.Config.set_streaming_chunk_size(8_000_000)   # 8 Mi linhas por chunk
    pl.Config.set_global_allocator("mimalloc")      # opcional (↑ alocação)

    # 2) Lazy scan em modo streaming
    scan = pl.scan_csv(
        path,
        separator=";",
        has_header=False,
        new_columns=["station", "measure"],
        dtypes={"station": pl.String, "measure": pl.Float64},
        null_values=[],               # evita checagem de nulos
        rechunk=False,                # só reenfileira no collect()
        low_memory=True,              # buffers menores → menos pico de RAM
        csv_buffer_size=32 * 1024,    # 32 KiB por sys-call (SSD NVMe = ok)
        n_threads=0,                  # 0 = usar todos
        infer_schema_length=0,        # não inferir – já informamos dtypes
    )

    df = (
        scan
        .group_by("station", maintain_order=False)  # agrupamento hash
        .agg(
            pl.col("measure").max().alias("max"),
            pl.col("measure").min().alias("min"),
            pl.col("measure").mean().alias("mean"),
        )
        # remova esta linha se não precisar da saída ordenada
        .sort("station")             
        .collect(streaming=True)      # executa de fato, chunk → chunk
    )
    return df

if __name__ == "__main__":
    import time
    t0 = time.time()
    df = create_polars_df()
    print(df)
    print(f"Polars took: {time.time() - t0:.2f} s")
