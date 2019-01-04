# Copyright 2017 IBM Corporation
# Copyright 2017 The Johns Hopkins University
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, absolute_import
import json
import boto3
from BaseHTTPServer import  HTTPServer, BaseHTTPRequestHandler
from objectfs.core.common.snsqueue import SnsTopic
from objectfs.settings import Settings
settings = Settings()
import logging

# use the 0.0.0.0 address since on EC2 we cannot attach to the public IP address
HOST = '0.0.0.0'

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
        content_length = self.headers.getheader('content-length')
        message_body = self.rfile.read(int(content_length))
        message_content = json.loads(message_body)
        message_type = self.headers.getheader('x-amz-sns-message-type')
        if message_type == 'SubscriptionConfirmation':
            # send confirmation token to SNS
            token = message_content['Token']
            sns_topic = SnsTopic()
            sns_topic.confirm_subscription(token)
        elif message_type == 'Notification':
            # import pdb; pdb.set_trace()
            message = json.loads(message_content['Message'])
            bucket_name = message['Records'][0]['s3']['bucket']['name']
            object_key = message['Records'][0]['s3']['object']['key']
            # process message
            self.server.fs.process_notification()
            # send 200 to notification received
            self.send_response(200)
        else:
            # send 400 back if we cannot parse the message
            self.send_response(400)
            
        # logic to update local memory
        # self.server.fs

class SnsHttpFactory(object):

    @staticmethod
    def run_server(file_system, server_class=SnsHttpServer, handler_class=SnsHttpRequestHandler, server_host=HOST, server_port=settings.SNS_PORT):
        http_daemon = server_class((server_host, server_port), handler_class)
        http_daemon.set_fs(file_system)
        print("Running server")
        http_daemon.serve_forever()
