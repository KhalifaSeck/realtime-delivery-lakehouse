"""
Configuration Spark et helpers de session pour le job de streaming.

Reprend le "Windows fix" éprouvé (PYSPARK_PYTHON, HADOOP_HOME/winutils)
nécessaire pour faire tourner Spark sous Windows.

Ce module ne contient QUE la configuration et la fabrication de la session.
Les schémas (schemas.py) et la logique de traitement (jobs/) sont séparés.

Dual-write : support de l'écriture vers ADLS Gen2 (contrôlé par SPARK_WRITE_ADLS).
Auth ADLS : SharedKey (clé du Storage Account), compatible Hadoop 3.3.2.
Note : FixedSASTokenProvider (auth SAS) nécessite Hadoop >= 3.4.1,
incompatible avec Spark 3.3.4 qui embarque Hadoop 3.3.2.
"""
import os
import sys
import platform
from pathlib import Path
from dotenv import load_dotenv

# Racine du projet (deux niveaux au-dessus de streaming/config.py).
_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=True)

# ------------------------------------------------------------
# Windows fix : indispensable pour Spark sous Windows.
# Doit s'exécuter AVANT tout import de pyspark.
# ------------------------------------------------------------
if platform.system() == "Windows":
    import shutil
    _python = shutil.which("python") or sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", _python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _python)
    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = hadoop_home + r"\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

# ============================================================
# Répertoire de sortie du data lake local (Parquet).
# ============================================================
LAKE_OUTPUT_DIR = os.getenv("LAKE_OUTPUT_DIR", r"C:\delivery-lake")

# ============================================================
# Kafka : bootstrap servers et topics
# ============================================================
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_GPS = os.getenv("KAFKA_TOPIC_GPS", "gps_positions")
TOPIC_DELIVERY = os.getenv("KAFKA_TOPIC_DELIVERY", "delivery_events")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "orders")
TOPIC_DRIVERS = os.getenv("KAFKA_TOPIC_DRIVERS", "driver_events")

# Checkpoints : IMPÉRATIVEMENT hors OneDrive.
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", r"C:\spark-checkpoints")

# ============================================================
# ADLS Gen2 : configuration pour l'écriture cloud
# ============================================================
ADLS_ACCOUNT = os.getenv("ADLS_ACCOUNT_NAME", "deliverydevlakesa")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "raw-events")

# SharedKey : clé primaire du Storage Account (compatible Hadoop 3.3.2).
# Récupérée via : terraform output -raw adls_account_key
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY", "")

# URI de base ADLS Gen2 (abfss = Azure Blob File System Secure)
ADLS_BASE_URI = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"

# Flag pour activer/désactiver le sink ADLS.
SPARK_WRITE_ADLS = os.getenv("SPARK_WRITE_ADLS", "true").lower() == "true"

# ============================================================
# Packages Spark
# ============================================================
# Kafka via Maven.
_SPARK_MAVEN_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4"

# Les JARs Azure (hadoop-azure-3.3.6.jar, azure-storage-8.6.6.jar,
# wildfly-openssl-1.0.7.Final.jar) sont dans $SPARK_HOME/jars/
# (copiés manuellement). Pas besoin de spark.jars ici.


def get_spark(app_name: str = "DeliveryStreaming") -> SparkSession:
    """
    Fabrique (ou récupère) la session Spark configurée pour le streaming.

    - Charge le connecteur Kafka via spark.jars.packages (Maven).
    - Les JARs Hadoop-Azure sont dans $SPARK_HOME/jars/ (chargés auto).
    - Configure l'authentification SharedKey pour ADLS Gen2 APRÈS la
      création du SparkContext (pour éviter le bug Windows '&').
    - Arrêt gracieux activé pour ne pas corrompre les checkpoints.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", _SPARK_MAVEN_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate()
    )

    # ------------------------------------------------------------
    # Config ADLS Gen2 via SharedKey — APRÈS la création du SparkContext.
    #
    # SharedKey utilise la clé primaire du Storage Account.
    # Compatible avec Hadoop 3.3.2 embarqué dans Spark 3.3.4.
    #
    # Note : FixedSASTokenProvider (auth SAS) nécessite Hadoop >= 3.4.1.
    # On utilise SharedKey comme alternative fonctionnelle.
    #
    # En production, on utiliserait un Service Principal Azure AD
    # (OAuth2) pour une auth plus granulaire et auditable.
    # ------------------------------------------------------------
    if SPARK_WRITE_ADLS and ADLS_ACCOUNT_KEY:
        adls_host = f"{ADLS_ACCOUNT}.dfs.core.windows.net"
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set(
            f"fs.azure.account.auth.type.{adls_host}", "SharedKey"
        )
        hadoop_conf.set(
            f"fs.azure.account.key.{adls_host}", ADLS_ACCOUNT_KEY
        )
        print(f"[config] ADLS Gen2 configuré (SharedKey) pour {adls_host}")
    elif SPARK_WRITE_ADLS:
        print("[config] WARN: SPARK_WRITE_ADLS=true mais ADLS_ACCOUNT_KEY manquant. Écriture ADLS désactivée.")

    return spark


def checkpoint_path(name: str) -> str:
    """
    Construit un chemin de checkpoint dédié à un flux donné.
    Chaque requête streaming a son propre sous-dossier isolé.
    """
    return str(Path(CHECKPOINT_DIR) / name)


def adls_output_path(event_type: str) -> str:
    """
    Construit le chemin ADLS Gen2 pour un type d'événement donné.
    Ex : adls_output_path("gps")
         -> "abfss://raw-events@deliverydevlakesa.dfs.core.windows.net/gps"
    """
    return f"{ADLS_BASE_URI}/{event_type}"


def adls_checkpoint_path(name: str) -> str:
    """
    Checkpoint dédié pour un sink ADLS (séparé du checkpoint local).
    Reste sur le disque local (le checkpoint est du state Spark, pas de la sortie).
    """
    return str(Path(CHECKPOINT_DIR) / f"adls_{name}")