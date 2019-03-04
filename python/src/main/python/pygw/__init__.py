from .config import config
from .base_models import *
from .stores import *
from .indices import *
from .debug import *

# Init connection to Java Gateway (`config.gateway`)
# This should be called only once.
config.init()