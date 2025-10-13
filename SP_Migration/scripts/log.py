import logging
import os

# Ensure the logs directory exists
os.makedirs("logs", exist_ok=True)

# Default log file setup
logging.basicConfig(
    filename="SP_Migration/logs/Sp_convertion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Separate logger for detailed dq logs
# dq_logger = logging.getLogger("dq_logger")
# dq_logger.setLevel(logging.INFO)

# dq_handler = logging.FileHandler("logs/dq_logs.log")  # Log file for dq logs
# dq_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# dq_handler.setFormatter(dq_formatter)
# dq_logger.addHandler(dq_handler)

def log_info(message):
    """Logs general info messages to Sp_convertion.log."""
    logging.info(message)

def log_error(message):
    """Logs general error messages to Sp_convertion.log."""
    logging.error(message)

# def log_dq_info(prefix, dq_value=None):
#     """Logs dq details to dq_logs.log."""
#     if dq_value is not None:
#         dq_logger.info(f"{prefix}\n{dq_value}")
#     else:
#         dq_logger.info(prefix)

# def log_dq_error(message):
#     """Logs dq errors to dq_logs.log."""
#     dq_logger.error(message)
