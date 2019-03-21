from pygw.config import config
from pygw.base_models import PyGwJavaWrapper

def print_obj(to_print, verbose=False):
    """Print method to help with debugging"""
    if isinstance(to_print, PyGwJavaWrapper):
        to_print = to_print._java_ref
    print(config.GATEWAY.entry_point.debug.printObject(to_print, verbose))