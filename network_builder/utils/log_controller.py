import logging
from pathlib import Path

def setup_custom_logger(name, file_system, config):

    logging.basicConfig(
        filename=Path(file_system.log_dir/config.main_log_file),
        format="%(asctime)s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )
    handler = logging.StreamHandler()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger



