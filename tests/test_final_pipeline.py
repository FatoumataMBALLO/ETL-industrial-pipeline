import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

@pytest.fixture(scope="session")
def spark():
    # Création d'une session Spark locale pour les tests
    return SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

def test_spark_cleaning_logic(spark):
    """Vérifie que la logique de nettoyage Spark fonctionne."""
    # Création d'un mini DataFrame de test
    data = [("Livre\x00Sale", "£51.77", "Five")]
    df = spark.createDataFrame(data, ["title", "price", "rating"])
    
    # Test nettoyage titre (caractères invisibles)
    df_clean = df.withColumn("clean_title", regexp_replace(col("title"), r"[\x00]", ""))
    
    # Test nettoyage prix (symbole monétaire)
    df_clean = df_clean.withColumn("clean_price", 
                                    regexp_replace(col("price"), r"[^0-9.]", "").cast("double"))
    
    row = df_clean.collect()[0]
    assert row["clean_title"] == "LivreSale"
    assert row["clean_price"] == 51.77

def test_imports():
    """Vérifie que les modules critiques sont importables."""
    import src.utils.logger as logger
    import src.utils.config as config
    assert logger is not None
    assert config is not None