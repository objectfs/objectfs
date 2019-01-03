from __future__ import print_function, absolute_import
import json
from BaseHTTPServer import  HTTPServer, BaseHTTPRequestHandler
from objectfs.settings import Settings
settings = Settings()
import logging


class SnsHttpServer(HTTPServer):

    def set_fs(self, file_system):
        
        self.fs = file_system

class SnsHttpRequestHandler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        """Get is not supported"""
        print("GET")
        self.send_response(404)

    def do_HEAD(self):
        """Head is not supported"""
        print("HEAD")
        self.send_response(404)

    def do_POST(self):
        """A notification is posted"""

        print("POST")
        import pdb; pdb.set_trace()
        content_length = self.headers.getheader('content-length')
        post_body = self.rfile.read(content_length)
        message_content = json.loads(post_body)
	message_type = self.headers.getheader('x-amz-sns-message-type')
	if message_type == 'SubscriptionConfirmation':
	    token = message_content['Token']
        # logic to update local memory
        # self.server.fs

class SnsHttpFactory(object):

    @staticmethod
    def run_server(file_system, server_class=SnsHttpServer, handler_class=SnsHttpRequestHandler, server_host=settings.SNS_HOST, server_port=settings.SNS_PORT):
        http_daemon = server_class((server_host, server_port), handler_class)
        http_daemon.set_fs(file_system)
        http_daemon.serve_forever()
