from pyspark.sql import SparkSession
from pyspark import StorageLevel
from operator import add
import argparse


def produce_summary(input_file_name, top_products_path, unique_products_path, product_count_path, master_config):

    spark = SparkSession.builder\
        .appName("Part_1")\
        .master(master_config)\
        .getOrCreate()

    rdd = spark.sparkContext.textFile(input_file_name)
    products = rdd.flatMap(lambda x: x.split(',')) \
        .map(lambda x: (x.strip().lower(), 1)) \
        .persist(StorageLevel.MEMORY_ONLY)

    # Part 1 task 2a - create a list of unique products
    write_unique_products(products, unique_products_path)

    # Part 1 task 2b - write product count
    write_product_count(products, product_count_path)

    # Part 1 task 3 - top 5 purchased products
    write_top_products(products, top_products_path)


def write_unique_products(products, output_file_path):

    unique_products = products.map(lambda x: x[0]).distinct().collect()

    # Output files to path out/out_1_2a.txt
    with open(output_file_path, 'w') as f:
        for p in unique_products:
            f.write(f'{p}\n')


def write_product_count(products, output_file_path):

    # Output files to path out/out_1_2b.txt
    with open(output_file_path, 'w') as f:
        f.write('Count:\n')
        f.write(f'{products.count()}\n')


def write_top_products(products, output_file_path):

    top_n = products.reduceByKey(add) \
        .top(5, lambda x: x[1])

    # Output files to path out/out_1_3.txt
    with open(output_file_path, 'w') as f:
        for tup in top_n:
            f.write(f'{tup}\n')


if __name__ == "__main__":

    ## Part 1, task 3 - top 5 purchased products
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--filename", default="groceries.csv", help="input filename to process")
    parser.add_argument("-t", "--top-products-output", default="out/out_1_3.txt", help="output path to save top products result")
    parser.add_argument("-u", "--unique-products-path", default="out/out_1_2a.txt", help="output path to list of unique products")
    parser.add_argument("-c", "--product-count-path", default="out/out_1_2b.txt", help="output path to product count")
    parser.add_argument("-m", "--master", default="local[1]", help="run on local/cluster mode")

    args = vars(parser.parse_args())

    input_file_name = args["filename"]
    top_products_path = args["top_products_output"]
    unique_products_path = args["unique_products_path"]
    product_count_path = args["product_count_path"]

    master_config = args["master"]

    produce_summary(input_file_name, top_products_path, unique_products_path, product_count_path, master_config)

