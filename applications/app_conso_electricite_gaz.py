import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, explode
from pyspark.sql.types import *
import pyspark.sql.functions as F

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('app_conso_eletricite_gaz') \
            .config("spark.jars.package","org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-client:3.3.4")\
            .getOrCreate()
            #.master("local[*]") \

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("============================== Spark connection created successfully!===========@@@@@@@")
    except Exception as e:
        logging.error(f"============================ Couldn't create the spark session due to exception {e} ==")

    return s_conn

spark_conn = create_spark_connection()
sc= spark_conn.sparkContext

# you should create it from aws account
accessKeyId="ACCESS KEY"
secretAccessKey="SECRET KEY"

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKeyId)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey)
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
 
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
# spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")

#change schema conso_par_adresse
schema = StructType([\
    StructField("annee",StringType(),True), \
    StructField("code_iris",StringType(),True), \
    StructField("nom_iris",StringType(),True), \
    StructField("numero_de_voie",StringType(),True), \
    StructField("indice_de_repetition",StringType(),True), \
    StructField("type_de_voie",StringType(),True), \
    StructField("libelle_de_voie",StringType(),True), \
    StructField("code_commune",StringType(),True), \
    StructField("nom_commune",StringType(),True), \
    StructField("segment_de_client",StringType(),True), \
    StructField("nombre_de_logements",IntegerType(),True), \
    StructField("consommation_annuelle_totale_de_l_adresse_mwh",DoubleType(),True), \
    StructField("consommation_annuelle_moyenne_par_site_de_l_adresse_mwh",DoubleType(),True), \
    StructField("consommation_annuelle_moyenne_de_la_commune_mwh",DoubleType(),True), \
    StructField("code_epci",StringType(),True), \
    StructField("code_departement",StringType(),True), \
    StructField("code_region",StringType(),True), \
])

df = spark_conn.read.schema(schema).option("header",True).csv('./data/conso_annuelle_electricite_gaz_adresse.csv')

#df.printSchema()

#add adresse column
df_with_adresse = df.withColumn("adresse",
                        F.concat(
                            F.col("numero_de_voie"),
                            F.lit(" "),
                            F.coalesce(F.col("indice_de_repetition"),F.lit(" ")),
                            F.coalesce(F.col("type_de_voie"),F.lit(" ")),
                            F.lit(" "),
                            F.col("libelle_de_voie")
                        ))

#selecting columns
df_adresse = df_with_adresse.select(["annee","numero_de_voie","indice_de_repetition","type_de_voie",
                                     "libelle_de_voie","code_commune","nom_commune","segment_de_client",
                                     "nombre_de_logements","consommation_annuelle_totale_de_l_adresse_mwh",
                                     "consommation_annuelle_moyenne_par_site_de_l_adresse_mwh",
                                     "consommation_annuelle_moyenne_de_la_commune_mwh","code_departement",
                                     "code_region","adresse"])

#df_adresse.printSchema()

df_adresse.write.format("csv").option("header","true").save("s3a://project-perso/data/projectConso/conso_electricite_gaz_adresse", mode="overwrite")

