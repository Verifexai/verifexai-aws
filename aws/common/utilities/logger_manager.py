import logging
import sys


# Logger MODULE NAMES
ANALYZE_FILE = 'AnalyzeFilePipline'
IMAGE_ANALYSIS =  'ImageAnalysis'
METADATA = 'Metadata'
PARSING = "Parsing"

class LoggerManager:
    """
    Manager for configuring and handling application logging
    """

    @staticmethod
    def configure_app_logger(app, log_level=logging.INFO):
        """
        Configure the Flask application logger

        Args:
            app: Flask application instance
            log_level: Logging level (default: INFO)
        """
        # Remove default handlers if any
        if app.logger.handlers:
            app.logger.handlers.clear()

        # Configure the root logger as well to ensure messages are visible when
        # running under different servers (e.g. gunicorn, wsgi servers)
        logging.basicConfig(
            level=log_level,
            stream=sys.stdout,
            format='[%(asctime)s] [%(levelname)s] %(module)s:%(lineno)d - %(message)s',
            force=True,
        )

        # Create and configure handler specifically for the Flask app logger
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(module)s:%(lineno)d - %(message)s'
            )
        )

        app.logger.addHandler(handler)
        app.logger.setLevel(log_level)

        # Ensure propagation to root logger is disabled to avoid duplicate logs
        app.logger.propagate = False

        return app.logger

    @staticmethod
    def get_module_logger(module_name, log_level=logging.INFO):
        """
        Get a configured logger for a specific module

        Args:
            module_name: Name of the module
            log_level: Logging level (default: INFO)

        Returns:
            Logger instance
        """
        logger = logging.getLogger(module_name)

        # Configure only if no handlers exist
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(module)s:%(lineno)d - %(message)s'
            ))
            logger.addHandler(handler)
            logger.setLevel(log_level)
            logger.propagate = False

        return logger