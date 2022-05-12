from pyspark.sql import SparkSession

import argparse

if __name__ == "__main__":

    ## Part 2, task 3 -
    ## Calculate the average number of bathrooms and bedrooms across all the properties listed in this data set
    ## with a price of > 5000
    ## and a review score being exactly equal to 10.

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--filename", default="sf-airbnb-clean.parquet", help="parquet file to process")
    parser.add_argument("-s", "--summary-stats-output", default="out/out_2_3.txt",
                        help="output path to save summary stats")
    parser.add_argument("-m", "--master", default="local[1]", help="run on local/cluster mode")

    args = vars(parser.parse_args())

    input_file_name = args["filename"]
    master_config = args["master"]
    summary_stats_output = args["summary_stats_output"]

    spark = SparkSession.builder \
        .appName("Part_2_task_3") \
        .master(master_config) \
        .getOrCreate()

    df = spark.read.parquet(input_file_name)

    df.createOrReplaceTempView("properties")

    sqlDF = spark.sql("SELECT avg(bathrooms) as avg_bathrooms, avg(bedrooms) as avg_bedrooms from properties "
                      "WHERE price > 5000 AND review_scores_value = 10")

    summary_stats = sqlDF.rdd.map(lambda p: (p.avg_bathrooms, p.avg_bedrooms)).collect()[0]
    with open(summary_stats_output, 'w') as f:
        f.write(f'{summary_stats[0]}, {summary_stats[1]} \n')

