from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString

import argparse
from pyspark.sql.types import StructType, DoubleType, StringType

if __name__ == "__main__":

    ## Part 3 - LogisticRegression
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--filename", default="/tmp/iris.csv", help="parquet file to process")
    parser.add_argument("-s", "--predictions-output", default="out/out_3_2.txt",
                        help="output path to save predictions")
    parser.add_argument("-m", "--master", default="local[1]", help="run on local/cluster mode")

    args = vars(parser.parse_args())

    input_file_name = args["filename"]
    master_config = args["master"]
    predictions_output = args["predictions_output"]

    spark = SparkSession.builder \
        .appName("Part_3") \
        .master(master_config) \
        .getOrCreate()

    schema = StructType() \
        .add("sepal_length", DoubleType(), True) \
        .add("sepal_width", DoubleType(), True) \
        .add("petal_length", DoubleType(), True) \
        .add("petal_width", DoubleType(), True) \
        .add("class", StringType(), True)

    df = spark.read.format("csv") \
        .options(header="False") \
        .schema(schema) \
        .load(input_file_name)

    assembler = VectorAssembler(
        inputCols=['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
        outputCol='features')

    training_data = assembler.transform(df)

    stringIndexer = StringIndexer(inputCol="class", outputCol="label", handleInvalid="error",
                                  stringOrderType="frequencyDesc")

    model = stringIndexer.fit(training_data)
    td = model.transform(training_data)

    # Build the model
    lr = LogisticRegression(labelCol="label", family="multinomial", regParam=1e5)
    lrModel = lr.fit(td)

    ## Test the model with data
    test_data = assembler.transform(spark.createDataFrame(
            [(5.1, 3.5, 1.4, 0.2),
            (6.2, 3.4, 5.4, 2.3)],
            ["sepal_length", "sepal_width", "petal_length", "petal_width"]))

    predictions = lrModel.transform(test_data)
    inverter = IndexToString(inputCol="prediction", outputCol="prediction_label", labels=model.labels)
    itd = inverter.transform(predictions)
    result = itd.select("prediction_label").collect()

    with open(predictions_output, 'w') as f:
        for row in result:
            print(":", str(row.prediction_label))
            f.write(f'{row.prediction_label} \n')
