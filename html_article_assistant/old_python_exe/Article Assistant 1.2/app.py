import platform
import sys

from PyQt5.QtWidgets import QApplication

from ui.main_window import App
from util.logger import get_logger

logger = get_logger(__name__)


def create_application() -> QApplication:
    logger.info(f"System: {platform.system()} {platform.version()}")
    logger.info(f"Machine: {platform.machine()}")
    logger.info(f"Python Version: {sys.version.split()[0]}")
    logger.info("Application initialization beginning")
    app = QApplication([])
    app.aboutToQuit.connect(lambda: logger.info("Quitting application"))
    App()
    return app


if __name__ == '__main__':
    try:
        create_application().exec()
    except Exception as err:
        logger.exception(err)
    finally:
        sys.exit()
