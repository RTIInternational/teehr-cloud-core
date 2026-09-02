from pyspark.sql import Row
from pyspark.sql import functions as F
from teehr import RemoteReadWriteEvaluation
from teehr.evaluation.spark_session_utils import create_spark_session
import time

spark = create_spark_session(
    update_configs={"spark.kubernetes.executor.node.selector.teehr-hub/nodegroup-name": "spark-r5-4xlarge"},
    start_spark_cluster=True,
    use_authmanager=True,
    executor_instances=1,
    executor_cores=1,
    executor_memory="1g"
)

ev = RemoteReadWriteEvaluation(spark=spark)

print(ev.list_tables())

print(ev.configurations.to_sdf().show())

def probe_executor_env(spark, expected_env_keys, partitions=8):
    """
    Returns one row per partition attempt with only booleans + executor identity.
    Never returns secret values.
    """
    keys = list(expected_env_keys)

    def _probe_partition(it):
        import os
        import socket
        # Force execution of partition iterator so Spark doesn't prune the task.
        _ = list(it)
        result = {
            "executor_host": socket.gethostname(),
            "pid_present": os.getpid() > 0,
        }
        for k in keys:
            result[f"has_{k}"] = bool(os.environ.get(k))
        yield Row(**result)

    # Use enough partitions to spread across executors
    rdd = spark.sparkContext.parallelize(range(partitions), partitions)
    rows = rdd.mapPartitions(_probe_partition).collect()
    return rows

expected = [
    "POLARIS_DEFAULT_REALM",
    "POLARIS_BROKER_SESSION_TOKEN"
]
rows = probe_executor_env(spark, expected, partitions=12)
for r in rows:
    print(r.asDict())

    catalog = "iceberg"
namespace = "teehr"
table = f"executor_probe_{int(time.time())}"
full_table = f"{catalog}.{namespace}.{table}"

# 1) Build distributed data (force executor work via repartition)
n = 200_000
parts = 12
df = (
    spark.range(0, n)
    .repartition(parts)
    .withColumn("grp", (F.col("id") % 17).cast("int"))
    .withColumn("payload", F.concat(F.lit("v-"), F.col("id").cast("string")))
)

# Optional: materialize first to ensure tasks run
print("input_count:", df.count())

# 2) Real distributed WRITE to Polaris/Iceberg
df.writeTo(full_table).using("iceberg").create()

# 3) Real distributed READ from Polaris/Iceberg
read_df = spark.read.table(full_table).repartition(parts)
print("table_count:", read_df.count())

# 4) A distributed aggregate to exercise more executor paths
agg = (
    read_df.groupBy("grp")
    .count()
    .orderBy("grp")
)
agg.show(20, truncate=False)

# 5) Cleanup
spark.sql(f"DROP TABLE {full_table}")
print("dropped:", full_table)

spark.stop()