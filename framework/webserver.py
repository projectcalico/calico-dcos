#!/usr/bin/env python

# Copyright 2015 Metaswitch Networks
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
import json
import threading
from flask import Flask

from config import config
from zookeeper import ZkDatastore

# Instantiate the Flask app and a ZkDatastore: we use our own datastore
# from the framework since we are running in a different thread.
app = Flask(__name__, static_url_path='')
zk = ZkDatastore()


def launch_webserver():
    """
    Launch the webserver.  Return a Thread object.  Caller may join the Thread
    if they need to be aware of its termination.
    :return:
    """
    kwargs = {
        "host": config.webserver_bind_ip,
        "port": config.webserver_bind_port
    }
    t = threading.Thread(target=app.run, kwargs=kwargs)
    t.daemon = True
    t.start()
    return t


@app.route("/")
def html_server():
    """
    Provide access to Calico status page.
    """
    return app.send_static_file("calico-status.html")


@app.route('/json')
def agent_json():
    """
    Get dictionary of agents with status for each task.
    """
    return json.dumps(zk.load_agents_raw_data())


@app.route("/health")
def check_health():
    """
    Provide access to Calico status page.
    """
    return '{"health": "OK"}'


