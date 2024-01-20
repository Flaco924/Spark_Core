from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("LoadJob").master("local[*]").getOrCreate()

clients_df = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
villes_df = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)

def joinClientsVilles(clients_majeurs_df, villes_df):
    joined_df = clients_majeurs_df.join(villes_df, clients_majeurs_df["zip"] == villes_df["zip"], "left")
    villes_df = villes_df.withColumnRenamed("zip", "ville_zip")

    joined_df = clients_majeurs_df.join(villes_df, clients_majeurs_df["zip"] == villes_df["ville_zip"], "left")

    return joined_df

def departement4Caracteres(df):
    df = df.withColumn(
        "zip",
        when(length(df["zip"]) == 4, concat(lit("0"), df["zip"]))
        .otherwise(df["zip"])
    )

    return df

def specifiteCorse(df):    
    df = df.withColumn(
        "departement",
        when((df["departement"] == "20") & (df["zip"] <= 20190), "2A") \
        .otherwise(when((df["departement"] == "20"), "2B").otherwise(df["departement"]))
    )

    return df

if __name__ == "__main__":

    # Filtrer les clients majeurs
    clients_majeurs_df = clients_df.filter(col("age") >= 18)

    # Appeler la fonction pour joindre clients et villes
    joined_df = joinClientsVilles(clients_majeurs_df, villes_df)

    # Appeler la fonction pour ajouter un 0 avant les départements de 4 caractères
    df_with0 = departement4Caracteres(joined_df)

    # Ajouter une colonne "departement" avec les 2 premiers caractères de zip
    df_departement = df_with0.withColumn("departement", df_with0["zip"].substr(1, 2))

    # Ajout de chiffre pour les départements d'outre-mer
    df_outre_mer = df_departement.withColumn("departement", when(df_departement["departement"] == "97", substring(df_departement["zip"], 1, 3)) \
                              .otherwise(df_departement["departement"]))

    # Gérer le cas particulier du département 20
    df_finale = specifiteCorse(df_outre_mer)

    # Écriture avec partitionBy sur la colonne zip
    df_finale.select("name", "age", "zip", "city", "departement").write.partitionBy("zip").parquet("data/exo2/output", mode="overwrite")
