import logging

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def create_session_with_retries(
    retries: int = 3, backoff_factor: float = 1.0, status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
) -> Session:
    """Maakt een requests.Session met automatische retry-logica."""
    session = Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST", "PUT", "DELETE"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # logging.getLogger("urllib3.util.retry").setLevel(logging.DEBUG)
    # logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)
    logger.debug(
        "Retry strategy configured: total=%d, backoff_factor=%f, status_forcelist=%s",
        retries,
        backoff_factor,
        status_forcelist,
    )

    return session
