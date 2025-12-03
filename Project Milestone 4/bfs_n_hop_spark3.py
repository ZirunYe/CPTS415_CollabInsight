from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim, lit


def load_graph_from_citation_csv(spark, csv_path):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )

    edges = (
        df
        .filter(col("references").isNotNull() & (col("references") != ""))
        .withColumn("ref", explode(split(col("references"), ";")))
        .withColumn("ref", trim(col("ref")))
        .select(
            col("id").alias("src"),
            col("ref").alias("dst")
        )
        .where(col("dst") != "")
    )

    return edges


def bfs_n_hop(edges, start_node, n_hop):
    spark = edges.sql_ctx.sparkSession

    # Spark CSV IDs are integers
    start_node = int(start_node)

    visited = spark.createDataFrame([(start_node, 0)], ["id", "hop"])
    frontier = visited

    print(f"[INFO] Start node: {start_node}", flush=True)

    for h in range(1, n_hop + 1):
        print(f"[INFO] Expanding hop {h}...", flush=True)

        next_frontier = (
            frontier.join(edges, frontier.id == edges.src, "inner")
            .select(edges.dst.alias("id"))
            .distinct()
            .join(visited, ["id"], "left_anti")
            .withColumn("hop", lit(h))
        )

        count_next = next_frontier.count()
        print(f"[INFO] Hop {h} count: {count_next}", flush=True)

        if count_next == 0:
            print(f"[INFO] No nodes found at hop {h}. Stopping BFS early.", flush=True)
            break

        visited = visited.union(next_frontier)
        frontier = next_frontier

    return visited.orderBy("hop", "id")


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to citation_network.csv")
    parser.add_argument("--start", required=True, help="Start node ID")
    parser.add_argument("--hop", required=True, type=int, help="Number of hops")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("CitationGraphBFS")
        .getOrCreate()
    )

    print("[INFO] Loading citation network...", flush=True)
    edges = load_graph_from_citation_csv(spark, args.csv)

    print("[INFO] Running BFS...", flush=True)
    result = bfs_n_hop(edges, args.start, args.hop)

    print("[INFO] Final hop counts:", flush=True)
    result.groupBy("hop").count().orderBy("hop").show(truncate=False)

    print("[INFO] Sample results (first 50 rows):", flush=True)
    result.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
