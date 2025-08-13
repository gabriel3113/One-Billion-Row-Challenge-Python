import os
from pathlib import Path
from typing import Sequence, Union
from glob import glob
import time
import polars as pl

# Garanta todos os cores (ideal setar antes de inicializar o plano)
os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count()))

def create_polars_df_ultra(
    paths: Union[str, Path, Sequence[Union[str, Path]], None] = None,
    sort_output: bool = False,
) -> pl.DataFrame:
    # chunks grandes reduzem overhead (ajuste se faltar RAM)
    pl.Config.set_streaming_chunk_size(32_000_000)

    # Default: use shards "weather_stations-*.txt"; se não existir, use o arquivo único
    if paths is None:
        found = sorted(glob("data/weather_stations-*.txt"))
        paths = found if found else "data/weather_stations.txt"

    # scan_csv aceita string, Path único ou lista de strings/Paths
    scan = pl.scan_csv(
        paths,
        separator=";",
        has_header=False,
        new_columns=["station", "measure"],
        schema_overrides={"station": pl.Utf8, "measure": pl.Float32},
        infer_schema_length=0,
        quote_char=None,     # parser 100% paralelo
        encoding="utf8",
        try_parse_dates=False,
    )

    with pl.StringCache():
        lf = (
            scan
            .with_columns([
                pl.col("station").cast(pl.Categorical),
                (pl.col("measure") * 10).round(0).cast(pl.Int16).alias("t10"),
            ])
            .select(["station", "t10"])  # solta 'measure' cedo
            .group_by("station")
            .agg([
                pl.col("t10").min().alias("min_t10"),
                pl.col("t10").max().alias("max_t10"),
                pl.col("t10").sum().cast(pl.Int64).alias("sum_t10"),  # evita overflow
                pl.len().cast(pl.Int64).alias("n"),
            ])
            .with_columns([
                (pl.col("min_t10") / 10).alias("min"),
                (pl.col("max_t10") / 10).alias("max"),
                (pl.col("sum_t10") / pl.col("n") / 10).alias("mean"),
            ])
            .select(["station", "min", "max", "mean"])
        )

        if sort_output:
            lf = lf.sort("station")  # deixe False para máximo throughput

        return lf.collect(streaming=True)

if __name__ == "__main__":
    t0 = time.time()
    df = create_polars_df_ultra(sort_output=False)
    print(df.head())
    print(f"Took: {time.time() - t0:.2f}s")
