from pyspark.sql import SparkSession

import argparse

if __name__ == "__main__":

    ## Part 2, task 4 -
    ## How many people can be accommodated by the property with the lowest price and highest rating?

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--filename", default="sf-airbnb-clean.parquet", help="parquet file to process")
    parser.add_argument("-s", "--summary-stats-output", default="out/out_2_4.txt",
                        help="output path to save summary stats")
    parser.add_argument("-m", "--master", default="local[1]", help="run on local/cluster mode")

    args = vars(parser.parse_args())

    input_file_name = args["filename"]
    master_config = args["master"]
    summary_stats_output = args["summary_stats_output"]

    spark = SparkSession.builder \
        .appName("Part_2_task_4") \
        .master(master_config) \
        .getOrCreate()

    df = spark.read.parquet(input_file_name)

    df.createOrReplaceTempView("properties")

    sqlDF = spark.sql("SELECT review_scores_rating, price, accommodates from properties "
                      "ORDER BY price ASC, review_scores_rating DESC LIMIT 1")

    summary_stats = sqlDF.rdd.map(lambda p: p.accommodates).collect()[0]

    with open(summary_stats_output, 'w') as f:
        f.write(f'{summary_stats} \n')

