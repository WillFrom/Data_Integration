from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, sum, when
import datetime

# Étape 1 : Configurer Spark pour accéder à HDFS
spark = SparkSession.builder \
    .appName("PySpark HDFS Example with Versioning") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Fonction pour générer un chemin de version
def generate_versioned_path(base_path, dataset_name):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_path}/{dataset_name}/{timestamp}"

# Étape 2 : Définir les chemins des fichiers dans HDFS
file1 = "hdfs://namenode:9000/data/aggregate-household-income-in-the-past-12-months-in-2015-inflation-adjusted-dollars.csv"
file2 = "hdfs://namenode:9000/data/self-employment-income-in-the-past-12-months-for-households.csv"
file3 = "hdfs://namenode:9000/data/types-of-health-insurance-coverage-by-age.csv"
output_base_path = "hdfs://namenode:9000/output"

# Étape 3 : Lire les fichiers dans des DataFrames
df1 = spark.read.csv(file1, header=True, inferSchema=True)
df2 = spark.read.csv(file2, header=True, inferSchema=True)
df3 = spark.read.csv(file3, header=True, inferSchema=True)

# Étape 4 : Appliquer des transformations
# Transformation 1 : Filtrer les revenus supérieurs à 50,000 dans df1
filtered_df1 = df1.filter(
    col("Estimate; Aggregate household income in the past 12 months (in 2015 Inflation-adjusted dollars)").cast("double") > 50000
)

# Transformation 2 : Sélectionner certaines colonnes dans df2
selected_df2 = df2.select(
    "Neighborhood",
    "Estimate; Total: - With self-employment income",
    "Estimate; Total: - No self-employment income"
)

# Transformation 3 : Ajouter une colonne calculée dans df3
df3_with_new_column = df3.withColumn(
    "new_column",
    (col("Estimate; Under 18 years:").cast("double") * 2)  # Exemple de colonne calculée
)

# Étape 5 : Sauvegarder les transformations avec gestion des versions
filtered_df1.write.csv(generate_versioned_path(output_base_path, "filtered_aggregate_income"), header=True, mode="overwrite")
selected_df2.write.csv(generate_versioned_path(output_base_path, "selected_self_employment_income"), header=True, mode="overwrite")
df3_with_new_column.write.csv(generate_versioned_path(output_base_path, "modified_health_insurance"), header=True, mode="overwrite")

# Métrique 1 : Revenus moyens par région
average_income_by_region = filtered_df1.groupBy("Neighborhood") \
    .avg("Estimate; Aggregate household income in the past 12 months (in 2015 Inflation-adjusted dollars)") \
    .withColumnRenamed("avg(Estimate; Aggregate household income in the past 12 months (in 2015 Inflation-adjusted dollars))", "Average_Income")
average_income_by_region.write.csv(generate_versioned_path(output_base_path, "average_income_by_region"), header=True, mode="overwrite")

# Métrique 2 : Proportion d'individus avec assurance par catégorie d'âge
health_coverage_by_age = df3.select(
    col("Estimate; Under 18 years:").cast("double"),
    col("Estimate; 18 to 34 years:").cast("double"),
    col("Estimate; 35 to 64 years:").cast("double"),
    col("Estimate; 65 years and over:").cast("double")
).agg(
    sum("Estimate; Under 18 years:").alias("Under_18_Total"),
    sum("Estimate; 18 to 34 years:").alias("18_to_34_Total"),
    sum("Estimate; 35 to 64 years:").alias("35_to_64_Total"),
    sum("Estimate; 65 years and over:").alias("65_and_over_Total")
)
health_coverage_by_age.write.csv(generate_versioned_path(output_base_path, "health_coverage_by_age"), header=True, mode="overwrite")

# Métrique 3 : Moyenne et écart type des revenus d'auto-entrepreneurs
self_employment_stats = df2.select(
    col("Estimate; Total: - With self-employment income").cast("double").alias("Self_Employment_Income")
).agg(
    mean("Self_Employment_Income").alias("Mean_Self_Employment_Income"),
    stddev("Self_Employment_Income").alias("StdDev_Self_Employment_Income")
)
self_employment_stats.write.csv(generate_versioned_path(output_base_path, "self_employment_stats"), header=True, mode="overwrite")

# Étape 6 : Arrêter Spark
spark.stop()
