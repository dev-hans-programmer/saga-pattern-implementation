import logging
import sys


def configure_logging(service_name: str, level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format=(
            f"%(asctime)s level=%(levelname)s service={service_name} "
            "logger=%(name)s message=%(message)s"
        ),
        force=True,
    )
