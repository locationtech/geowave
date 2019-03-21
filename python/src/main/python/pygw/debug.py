from pygw.config import config
<<<<<<< HEAD
from pygw.base_models import PyGwJavaWrapper
=======
from .base_models import PyGwJavaWrapper
>>>>>>> fe7f503f0786c09f20cc8e31882197b20812e5fc

def print_obj(to_print, verbose=False):
    """Print method to help with debugging"""
    if isinstance(to_print, PyGwJavaWrapper):
        to_print = to_print._java_ref
    print(config.GATEWAY.entry_point.getDebug().printObject(to_print, verbose))