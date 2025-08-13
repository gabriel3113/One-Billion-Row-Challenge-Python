import os
import time
from glob import glob
import duckdb

def create_duckdb_fast(paths_glob: str = "data/weather_stations-*.txt", sort_output: bool = False):
    duckdb.sql(f"PRAGMA threads={os.cpu_count()};")

    files = sorted(glob(paths_glob))
    src = paths_glob if files else "data/weather_stations.txt"

    sql = f"""
    WITH data AS (
        SELECT
            station,
            CAST(ROUND(temperature * 10) AS SMALLINT) AS t10
        FROM read_csv('{src}',
            AUTO_DETECT=FALSE,
            delim=';',
            header=FALSE,
            columns={{'station':'VARCHAR','temperature':'DOUBLE'}},
            quote='',      -- sem aspas => parser paralelo
            escape=''
        )
    ),
    agg AS (
        SELECT
            station,
            MIN(t10) AS min_t10,
            MAX(t10) AS max_t10,
            SUM(t10)::BIGINT AS sum_t10,
            COUNT(*)::BIGINT AS n
        FROM data
        GROUP BY station
    )
    SELECT
        station,
        (min_t10 / 10.0)            AS min_temperature,
        ROUND((sum_t10::DOUBLE / n) / 10.0, 1) AS mean_temperature,
        (max_t10 / 10.0)            AS max_temperature
    FROM agg
    {"ORDER BY station" if sort_output else ""}
    """
    return duckdb.sql(sql)  # Relation na conexão global

if __name__ == "__main__":
    t0 = time.time()
    rel = create_duckdb_fast(sort_output=False)

    # >>> TABELA BONITA NO TERMINAL
    rel.limit(20).show()         # imprime como tabela (ASCII)

    # Alternativas (se preferir DataFrame):
    # print(rel.limit(20).df().to_string(index=False))   # pandas
    # print(rel.limit(20).pl())                          # polars

    print(f"DuckDB Took: {time.time() - t0:.2f} sec")
