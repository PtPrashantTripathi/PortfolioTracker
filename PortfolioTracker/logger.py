# Importing necessary files and packages
import logging

# Set up the logger
logging.basicConfig(
    level=logging.INFO,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
