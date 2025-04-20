import logging

ORM_LOGGER_NAME = "pds_orm"

logger = logging.getLogger(ORM_LOGGER_NAME)

logger.addHandler(logging.NullHandler())
