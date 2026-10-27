"""Runs Bellwether Spark manifests through the shim for `tests/spark.rs`.

`python spark_driver.py <shim dir> <jobs.json> <results.json>`: one local
Spark session (session time zone Asia/Kolkata, so no literal may depend on
it) runs every job in `jobs.json` (`{manifest, out, deny}`): it makes each
file in `deny` unreadable at the operating system for the job, so a run
that opens one fails; runs `bellwether_plan.read`; and writes the rows to
`out` as one Parquet file. Results (`{error}`, `null` on success) go to
`results.json` in job order.
"""

import json
import os
import stat
import sys


def main():
    shim, jobs_path, results_path = sys.argv[1:4]
    sys.path.insert(0, shim)
    from pyspark.sql import SparkSession

    from bellwether_plan import read

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("bellwether-adapters-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    with open(jobs_path, encoding="utf-8") as fh:
        jobs = json.load(fh)
    results = []
    for job in jobs:
        modes = {}
        for p in job["deny"]:
            modes[p] = stat.S_IMODE(os.stat(p).st_mode)
            os.chmod(p, 0)
        try:
            df = read(spark, job["manifest"])
            df.coalesce(1).write.mode("overwrite").parquet(job["out"])
            results.append({"error": None})
        except Exception as e:  # noqa: BLE001 - reported to the Rust test verbatim
            results.append({"error": str(e)[:2000]})
        finally:
            for p, m in modes.items():
                os.chmod(p, m)
    spark.stop()
    with open(results_path, "w", encoding="utf-8") as fh:
        json.dump(results, fh)


if __name__ == "__main__":
    main()
