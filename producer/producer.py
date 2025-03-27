import uuid, json, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, lit, udf
from pyspark.sql.types import StringType
from kafka import KafkaProducer
from dotenv import load_dotenv

envPath = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(envPath)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# path = kagglehub.dataset_download("ahmettalhabektas/new-york-cars-big-data-2023")
# pathData = path + "\\" + "New_York_cars.csv"

def generate_uuid():
    return uuid.uuid4()

generate_uuid_udf = udf(generate_uuid, StringType())

def extractData(path):
    spark = SparkSession.builder.appName("NewyorkCarsBigData").getOrCreate()
    df = spark.read.csv(path)
    return df


def tranformData(rawData):
    columnsIsNull = ["_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13", "_c14", "_c15", "_c16", "_c17", "_c18", "_c19"]
    formatedColumesNull = [
        when(col(c).isNull(), "Not specified").otherwise(col(c)).alias(c) 
        for c in columnsIsNull
    ]

    df = rawData.select(
        col("_c0"),
        col("_c1"),
        col("_c2").cast("float").alias("_c2"),
        when(col("_c3").rlike("^\d+(\.\d+)?$"), "Not specified").otherwise(col("_c3")).alias("_c3"),
        *formatedColumesNull,
        col("_c20"),
        col("_c21").cast("integer").alias("_c21"),
        col("_c22"),
        regexp_replace(col("_c23"), r"\$", "USD").alias("_c23")
    )
    df = df.withColumn("id", lit(None).cast("string"))
    return df


# path = kagglehub.dataset_download("ahmettalhabektas/new-york-cars-big-data-2023")
# pathData = path + "\\" + "New_York_cars.csv"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pathData = os.path.join(BASE_DIR, "dataset", "New_York_cars.csv")
print(pathData)
readCSV = extractData(pathData)
formattedData = tranformData(readCSV)

formattedData = formattedData.rdd.zipWithIndex().filter(lambda row: row[1] > 0).map(lambda row: row[0]).toDF(formattedData.schema)

pandasDF = formattedData.toPandas()
dataList = pandasDF.to_dict(orient="records") 

for record in dataList:
    record["id"] = str(uuid.uuid4())
    producer.send("cars", value=record)

producer.flush()
print("send data successfully!!")