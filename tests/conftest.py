import logging
import os
import os.path
import site

logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
site.addsitedir(HERE)
