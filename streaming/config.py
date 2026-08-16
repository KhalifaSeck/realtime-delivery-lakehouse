"""
Configuration Spark et helpers de session pour le job de streaming.

Reprend le "Windows fix" éprouvé (PYSPARK_PYTHON, HADOOP_HOME/winutils)
nécessaire pour faire tourner Spark sous Windows.

Ce module ne contient QUE la configuration et la fabrication de la session.
Les schémas (schemas.py) et la logique de traitement (jobs/) sont séparés.

Dual-write : support de l'écriture vers ADLS Gen2 (contrôlé par SPARK_WRITE_ADLS).
Auth ADLS : SharedKey (clé du Storage Account), compatible Hadoop 3.3.2.

JARs :
  - Windows : Kafka téléchargé via Maven au démarrage, Azure dans $SPARK_HOME/jars/
  - Linux (Docker/K8s) : tous les JARs pré-installés dans $SPARK_HOME/jars/
    via le Dockerfile, classpath forcé via spark.driver.extraClassPath
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
# Détection de l'OS
# ============================================================
_IS_WINDOWS = platform.system() == "Windows"

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
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY", "")

# URI de base ADLS Gen2 (abfss = Azure Blob File System Secure)
ADLS_BASE_URI = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"

# Flag pour activer/désactiver le sink ADLS.
SPARK_WRITE_ADLS = os.getenv("SPARK_WRITE_ADLS", "true").lower() == "true"

# ============================================================
# Packages Spark (Windows uniquement)
# ============================================================
# Sous Windows, Kafka est téléchargé via Maven au démarrage.
# Sous Linux (Docker/K8s), tous les JARs sont pré-installés
# dans $SPARK_HOME/jars/ via le Dockerfile.
_SPARK_MAVEN_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4"


def get_spark(app_name: str = "DeliveryStreaming") -> SparkSession:
    """
    Fabrique (ou récupère) la session Spark configurée pour le streaming.

    - Sous Windows : charge le connecteur Kafka via Maven (spark.jars.packages).
    - Sous Linux (K8s) : tous les JARs sont dans $SPARK_HOME/jars/,
      classpath forcé via spark.driver.extraClassPath.
    - Configure l'authentification SharedKey pour ADLS Gen2 APRÈS la
      création du SparkContext (pour éviter le bug Windows '&').
    - Arrêt gracieux activé pour ne pas corrompre les checkpoints.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.scheduler.mode", "FAIR")
    )

    if _IS_WINDOWS:
        # Windows : télécharge Kafka via Maven au démarrage
        builder = builder.config("spark.jars.packages", _SPARK_MAVEN_PACKAGES)
    else:
        # Linux (Docker/K8s) : JARs pré-installés dans $SPARK_HOME/jars/
        # Forcer le classpath pour que Spark les trouve au runtime
        import pyspark
        jars_dir = os.path.join(pyspark.__path__[0], "jars", "*")
        builder = (builder
            .config("spark.driver.extraClassPath", jars_dir)
            .config("spark.executor.extraClassPath", jars_dir)
        )

    spark = builder.getOrCreate()

    # ------------------------------------------------------------
    # Config ADLS Gen2 via SharedKey — APRÈS la création du SparkContext.
    #
    # POURQUOI APRÈS : quand on passe des valeurs contenant des caractères
    # spéciaux via .config() du builder, PySpark les transmet au JVM via
    # la ligne de commande. Sous Windows, les '&' sont interprétés comme
    # des séparateurs de commande (cmd.exe).
    #
    # En configurant via hadoopConfiguration().set() APRÈS le getOrCreate(),
    # on injecte les valeurs directement dans la JVM en mémoire.
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
        print("[config] WARN: SPARK_WRITE_ADLS=true mais ADLS_ACCOUNT_KEY manquant.")

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