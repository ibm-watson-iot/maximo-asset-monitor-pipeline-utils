# *****************************************************************************
# © Copyright IBM Corp. 2018, 2024  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import argparse
import logging.config
import os
import sys
import json
import datetime as dt
import gzip
import inspect
import logging
import os
import re

import traceback
import warnings
from collections import defaultdict
from functools import partial
from pathlib import Path
import numpy as np
import pandas as pd
import pandas.tseries.offsets

from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from socketserver import ThreadingMixIn
import threading
import time

logger = logging.getLogger(__name__)


hostName = "localhost"
serverPort = 8080

class MyServer(ThreadingHTTPServer):
    def __init__(self, address, handler, renderCmd, entityType, location):
        self.renderCmd = renderCmd
        self.entityType = entityType
        self.location = location
        super().__init__(address, handler)

class MyHandlerDot(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
        self.wfile.write(bytes('<script src="https://d3js.org/d3.v7.js"></script>', "utf-8"))
        self.wfile.write(bytes('<script src="https://unpkg.com/@hpcc-js/wasm@2/dist/graphviz.umd.js"></script>', "utf-8"))
        self.wfile.write(bytes('<script src="https://unpkg.com/d3-graphviz@5/build/d3-graphviz.js"></script>', "utf-8"))
        my_d3_div = '<script src="https://unpkg.com/d3-graphviz@5/build/d3-graphviz.js"></script> \
            <div id="graph" style="text-align: center;"></div> \
            <script> d3.select("#graph").graphviz({useWorker: false}).renderDot('
        my_d3_div += "'" + self.server.renderCmd + "'); </script>"
        self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
        self.wfile.write(bytes('<body>', "utf-8"))
        self.wfile.write(bytes(my_d3_div, "utf-8"))
        self.wfile.write(bytes("<p>This is an example web server.</p>", "utf-8"))
        self.wfile.write(bytes('</body></html>', "utf-8"))

class MyHandlerMermaid(BaseHTTPRequestHandler):
    def do_GET(self):
        #print("CMD", self.server.renderCmd)
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes("<html><head><title>Pipeline Graph</title></head>", "utf-8"))
        merm = "<script type=\"module\"> \
            import mermaid from \'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs\'; \
            mermaid.initialize({ startOnLoad: true }); \
            </script>"
        self.wfile.write(bytes(merm, "utf-8"))
        loc = self.server.location
        self.wfile.write(bytes("<p>Pipeline: %s, %s</p>" % (self.server.entityType, loc['alias'] if loc is not None else "-"),"utf-8"))
        self.wfile.write(bytes('<body>', "utf-8"))
        self.wfile.write(bytes('<pre class="mermaid">', "utf-8"))
        self.wfile.write(bytes(self.server.renderCmd, "utf-8"))
        self.wfile.write(bytes('</pre></body></html>', "utf-8"))

