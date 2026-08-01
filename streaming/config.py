"""
Configuration Spark et helpers de session pour le job de streaming.

Reprend le "Windows fix" éprouvé (PYSPARK_PYTHON, HADOOP_HOME/winutils)
nécessaire pour faire tourner Spark sous Windows.

Ce module ne contient QUE la configuration et la fabrication de la session.
Les schémas (schemas.py) et la logique de traitement (jobs/) sont séparés.
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

    # Force PySpark à utiliser le même interpréteur Python que le driver.
    _python = shutil.which("python") or sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", _python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _python)

    # winutils.exe (Hadoop) : requis pour l'accès au système de fichiers local.
    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = hadoop_home + r"\bin;" + os.environ.get("PATH", "")


from pyspark.sql import SparkSession

# Répertoire de sortie du data lake (Parquet). Hors OneDrive en dev.
LAKE_OUTPUT_DIR = os.getenv("LAKE_OUTPUT_DIR", r"C:\delivery-lake")
# ------------------------------------------------------------
# Paramètres lus depuis .env (avec valeurs par défaut dev local)
# ------------------------------------------------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_GPS = os.getenv("KAFKA_TOPIC_GPS", "gps_positions")
TOPIC_DELIVERY = os.getenv("KAFKA_TOPIC_DELIVERY", "delivery_events")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "orders")
TOPIC_DRIVERS = os.getenv("KAFKA_TOPIC_DRIVERS", "driver_events")

# Checkpoints : IMPÉRATIVEMENT hors OneDrive (fichiers verrouillés/réécrits).
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", r"C:\spark-checkpoints")

# Versions des connecteurs Spark (doivent matcher la version de Spark : 3.3.4).
# _2.12 = version de Scala ; ne pas changer sans raison.
_SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4"


def get_spark(app_name: str = "DeliveryStreaming") -> SparkSession:
    """
    Fabrique (ou récupère) la session Spark configurée pour le streaming.

    - Charge le connecteur Kafka via spark.jars.packages (téléchargé au
      premier lancement, puis mis en cache dans ~/.ivy2).
    - Arrêt gracieux activé pour ne pas corrompre les checkpoints.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", _SPARK_KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def checkpoint_path(name: str) -> str:
    """
    Construit un chemin de checkpoint dédié à un flux donné.
    Chaque requête streaming a son propre sous-dossier isolé.
    """
    return str(Path(CHECKPOINT_DIR) / name)