from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, IndexToString, StringIndexerModel

import argparse
from pyspark.sql.types import StructType, DoubleType, StringType
from pyspark.ml import Pipeline

CLASS_LABELS = ['Iris-setosa', 'Iris-versicolor', 'Iris-virginica']
FEATURE_COLS = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']


def run(master_config, input_file_name, predictions_output):

    spark = SparkSession.builder \
        .appName("Part_3") \
        .master(master_config) \
        .getOrCreate()

    ## load data
    df = load_df(input_file_name, spark)

    ## train model
    lr_model = train_model(df)

    ## test model
    result = get_predictions(lr_model, spark.createDataFrame(
        [(5.1, 3.5, 1.4, 0.2),
         (6.2, 3.4, 5.4, 2.3)], FEATURE_COLS))

    ## write test predictions to file
    write_predictions(predictions_output, result)


def train_model(df):

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol='features')

    string_indexer = StringIndexerModel.from_labels(CLASS_LABELS,
                                                    inputCol="class", outputCol="label", handleInvalid="error")

    lr = LogisticRegression(labelCol="label", family="multinomial", regParam=1e5)

    pipeline = Pipeline(stages=[assembler, string_indexer, lr])
    lr_model = pipeline.fit(df)

    return lr_model


def load_df(input_file_name, spark):

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

    return df


def get_predictions(lr_model, test_data):

    predictions = lr_model.transform(test_data)
    inverter = IndexToString(inputCol="prediction", outputCol="prediction_label", labels=CLASS_LABELS)
    inverted = inverter.transform(predictions)

    return inverted.select("prediction_label").collect()


def write_predictions(predictions_output, result):
    with open(predictions_output, 'w') as f:
        f.write('class\n')
        for row in result:
            f.write(f'{row.prediction_label} \n')


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

    run(master_config, input_file_name, predictions_output)
