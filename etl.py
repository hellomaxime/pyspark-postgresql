from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import psycopg2


conn = psycopg2.connect(
    host='localhost',
    database='mypostgresdb',
    user='admin',
    password='admin'
)

cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS movie
    (rating numeric NOT NULL,
     count_movie integer NOT NULL
);""")

spark = SparkSession.builder \
                    .master("local[*]") \
                    .appName("spark-to-postgres") \
                    .getOrCreate()

sqlctx = SQLContext(spark.sparkContext)

data = sqlctx.read.option("header", True).csv('file:///home/maxime/spark-postgresql/ratings.csv')

a = [tuple(x) for x in data.groupby("rating").count().collect()]
b = ','.join(['%s']*len(a))

query = f"INSERT INTO movie(rating, count_movie) VALUES {b}"
cur.execute(query, a)
conn.commit()

"""

- start postgresql
- CREATE DATABASE mypostgresdb;

"""