"""
Fixtures partagées pour les tests streaming.
conftest.py est automatiquement découvert par pytest : les fixtures
définies ici sont disponibles dans tous les tests du dossier.
"""
import pytest

from streaming.config import get_spark


@pytest.fixture(scope="session")
def spark():
    """
    Session Spark partagée pour toute la session de test.
    scope='session' : une seule session créée pour tous les tests,
    plutôt qu'une par test (démarrer Spark coûte cher).
    """
    session = get_spark("PytestStreaming")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()