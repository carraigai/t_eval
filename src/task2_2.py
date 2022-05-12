from pyspark.sql import SparkSession

import argparse

if __name__ == "__main__":

    ## Part 2, task 2 - min/max/count
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--filename", default="sf-airbnb-clean.parquet", help="parquet file to process")
    parser.add_argument("-s", "--summary-stats-output", default="out/out_2_2.txt",
                        help="output path to save summary stats")
    parser.add_argument("-m", "--master", default="local[1]", help="run on local/cluster mode")

    args = vars(parser.parse_args())

    input_file_name = args["filename"]
    master_config = args["master"]
    summary_stats_output = args["summary_stats_output"]

    spark = SparkSession.builder \
        .appName("Part_2_task_2") \
        .master(master_config) \
        .getOrCreate()

    df = spark.read.parquet(input_file_name)

    df.createOrReplaceTempView("properties")

    sqlDF = spark.sql("SELECT max(price) as max_price, min(price) as min_price from properties")

    summary_stats = sqlDF.rdd.map(lambda p: (p.min_price, p.max_price)).collect()[0]

    with open(summary_stats_output, 'w') as f:
        f.write(f'{summary_stats[0]}, {summary_stats[1]}, {df.count()}\n')
