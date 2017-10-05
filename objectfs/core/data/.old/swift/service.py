from __future__ import print_function, absolute_import
from swiftclient.service import SwiftService, SwiftError
from settings import Settings
settings = Settings()

class Service(object):

    def __init__(self):
        
        _auth = {
                    'auth_version': '1.0',
                    'auth': settings.AUTH_URL,
                    'user': settings.AUTH_USER,
                    'key': settings.AUTH_KEY
                }

        _opts = {
                    'retries': settings.NUM_THREADS,
                    'container_threads': settings.NUM_THREADS,
                    'object_dd_threads': settings.NUM_THREADS,
                    'segment_threads': settings.NUM_THREADS,
                    'no_download': False,
                    'shuffle': False
                }
        
        self._service = SwiftService(options=_opts)
