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

    visited = spark.createDataFrame([(start_node, 0)], ["id", "hop"])
    frontier = visited

    for h in range(1, n_hop + 1):
        next_frontier = (
            frontier.join(edges, frontier.id == edges.src, "inner")
            .select(edges.dst.alias("id"))
            .distinct()
            .join(visited, ["id"], "left_anti")
            .withColumn("hop", lit(h))
        )

        visited = visited.union(next_frontier)
        frontier = next_frontier

    return visited.orderBy("hop")


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

    print("[INFO] Loading citation network...")
    edges = load_graph_from_citation_csv(spark, args.csv)

    print("[INFO] Running BFS...")
    result = bfs_n_hop(edges, args.start, args.hop)

    print("[INFO] Result:")
    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
