try:
    from .randl_client import Randl
    from . import util
except ImportError:
    from .randl_client import Randl
    from . import util