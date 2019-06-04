from pygw.config import config
from pygw.base_models import *
from pygw.stores import *
from pygw.indices import *
from pygw.query import *
from pygw.geotools import *
from pygw.debug import *
from pygw.special_queries import *

from py4j.protocol import Py4JNetworkError

class PyGwJavaGatewayNotStartedError(Exception): pass

try: 
    # Init connection to Java Gateway (`config.GATEWAY`)
    # This should be called only once.
    config.init()
except Py4JNetworkError as exc:
    raise PyGwJavaGatewayNotStartedError("The JavaGateway must be running before you can import pygw.") from exc
