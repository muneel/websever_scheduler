#!/usr/bin/env python
"""
Very simple HTTP server in python.

Usage::
    ./dummy-web-server.py [<port>]

Send a GET request::
    curl http://localhost

Send a HEAD request::
    curl -I http://localhost

Send a POST request::
    curl -d "foo=bar&bin=baz" http://localhost

"""
OK = 200
BAD_REQUEST = 400

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer
import json
import scheduler
from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO, ERROR, FileHandler, NullHandler

# HTTP Codes
OK = 200
BAD_REQUEST = 400
INTERNAL_ERROR = 500

class S(BaseHTTPRequestHandler):

    # Global Scheduler
    sch = scheduler.Scheduler()
    # load Work from Json
    sch.load_work()
    # Start Work Thread
    sch.start_work()
    # Global Logger
    log = scheduler.MLOGGER('RESTServer', level=DEBUG, logtype='CONSOLE', filename = 'rest_log.log')

    def _set_headers(self, resp_code = OK):
        self.send_response(resp_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        self.log.info('GET Recieved : %s' %self.path)
        if self.path == "/queue":
            self._set_headers()
            result = self.sch.get_work()
            self.log.info('Posting Result : %s' %result)
            self.wfile.write(json.dumps(result))
        else:
            self._set_headers(BAD_REQUEST)
            self.wfile.write({"Error : Bad request for GET Method, check url"})

    def do_HEAD(self):
        self._set_headers()

    def __read_content(self):
        # Read Contents from POST and Load Json
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data)
        return data

    def do_POST(self):
        self.log.info('POST Recieved : %s' %self.path)
        try:
            if self.path == "/results":
                data = self.__read_content()
                self.log.info('/results - data = %s' %data)
                result = self.sch.get_result(data)
                self.log.info('Posting Result : %s' %result)
                self._set_headers()
                self.wfile.write(json.dumps(result))
            elif self.path == "/monitor":
                data = self.__read_content()
                self.log.info('/monitor - data = %s' %data)
                result = self.sch.add_work(data)
                self.log.info('Posting Result : %s' %result)
                self._set_headers()
                self.wfile.write(json.dumps(result))
                #data
            else:
                self._set_headers(BAD_REQUEST)
                self.log.debug('Bad request Recieved for POST')
                self.wfile.write({"Error : Bad request for POST Method, check url"})
        except Exception as e:
            self.log.info('Exception : %s' %e)
            self._set_headers(INTERNAL_ERROR)
            self.wfile.write({"Error : Bad request for POST Method, check url"})
        # Send POST Message
        #self.wfile.write("<html><body><h1>POST!</h1></body></html>")

def run(server_class=HTTPServer, handler_class=S, port=80):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print 'Starting WebServer...'
    httpd.serve_forever()

if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
