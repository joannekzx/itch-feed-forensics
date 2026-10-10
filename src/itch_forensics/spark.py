import os
import sys
import pathlib

from pyspark.sql import SparkSession

SQL_DIR = pathlib.Path(__file__).resolve().parents[2] / "sql"
TABLES = ["messages", "add_order", "execution", "cancel", "delete", "replace", "trade", "cross", "noii", "directory", "trading_action"]


def get_spark(app="itch", shuffle_partitions=16, cores="*"):
    # Python workers must use the same interpreter (and so the same installed package) as the driver
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (SparkSession.builder.master(f"local[{cores}]").appName(app)
            .config("spark.driver.memory", "3g").config("spark.sql.shuffle.partitions", shuffle_partitions)
            .config("spark.sql.session.timeZone", "UTC").config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def register(spark, parquet_dir):
    for t in TABLES:
        spark.read.parquet(str(pathlib.Path(parquet_dir) / f"{t}.parquet")).createOrReplaceTempView(t)


def run_sql(spark, name, **params):
    """Run sql/<name>.sql with named parameters and return a pandas DataFrame."""
    return spark.sql((SQL_DIR / f"{name}.sql").read_text(), args=params or None).toPandas()
