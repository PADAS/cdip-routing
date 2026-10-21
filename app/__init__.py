from app.core import local_logging

local_logging.init()  # before anything else logs
from app.core import tracing  # Do not remove this import
from app.core import settings
