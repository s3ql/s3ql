'''
storjs3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from . import s3c
from .s3c import get_S3Error
from .common import NoSuchObject, retry
from ..inherit_docstrings import copy_ancestor_docstring
from xml.sax.saxutils import escape as xml_escape
import re
import time
import urllib.parse
import hashlib
import hmac

log = logging.getLogger(__name__)

class Backend(s3c.Backend):
    """A backend to store data in Storj S3 gateway-st

    Uses some quirks for placing data, seq, and metadata objects in the storj bucket.
    This is needed for gateway-st/gateway-mt limited ListObjectsV2 S3-API to work correctly
    with buckets that contain more than 100K objects.
    """

    def __init__(self, options):
        super().__init__(options)

