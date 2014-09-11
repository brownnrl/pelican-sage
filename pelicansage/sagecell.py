"""
A small client illustrating how to interact with the Sage Cell Server, version 2

Requires the websocket-client package: http://pypi.python.org/pypi/websocket-client
"""

import websocket
import threading
import json
from uuid import uuid4

from .pelicansageio import pelicansageio

from .managefiles import ResultTypes

from collections import namedtuple as NT
from itertools import groupby

import pprint

from .util import CellResult, SageError, combine_results

CR = CellResult

class BaseClient(object):
    def __init__(self, url, timeout=10, io=None):

        self.io = pelicansageio if io is None else io

        if not url.endswith('/'):
            url+='/'
        self.url = url
        self.req_ses = None
        self.timeout = timeout
        
        self._json_session_info = None
        self.kernel_id = None

        self.reset()
    
    def reset(self):

        self._running = False

    def _code_keepalive(self, code):
        return code

    def _get_terminate_command(self):
        return "exit"

    def _create_new_session(self):
        raise NotImplemented()

    def _send_first_message(self):
        raise NotImplemented()

    def cleanup(self):
        self.close()

    def execute_request(self, code, store_history=False):
        # zero out our list of messages, in case this is not the first request
        if not self._running:
            self._create_new_session()

            # RESPONSE: {"id": "ce20fada-f757-45e5-92fa-05e952dd9c87", "ws_url": "ws://localhost:8888/"}
            # construct the iopub and shell websocket channel urls from that

            websocket.setdefaulttimeout(self.timeout)
            self._shell = websocket.create_connection(self.kernel_url+'shell')
            self._iopub = websocket.create_connection(self.kernel_url+'iopub')
            self._send_first_message()

            self._running = True

            code = self._code_keepalive(code)

        self.shell_messages = []
        self.iopub_messages = []

        # Send the JSON execute_request message string down the shell channel

        msg = self._make_execute_request(code, store_history)
        self._shell.send(msg)
            
        # We use threads so that we can simultaneously get the messages on both channels.
        self.threads = [threading.Thread(target=self._get_iopub_messages), 
                        threading.Thread(target=self._get_shell_messages)]
        for t in self.threads:
            t.start()

        # Wait until we get both a kernel status idle message and an execute_reply message
        for t in self.threads:
            t.join()

        return {'kernel_url': self.kernel_url, 'shell': self.shell_messages, 'iopub': self.iopub_messages}

    def _get_shell_messages(self):
        while True and self._running:
            msg = json.loads(self._shell.recv())
            self.shell_messages.append(msg)
            # an execute_reply message signifies the computation is done
            if msg['header']['msg_type'] == 'execute_reply':
                break

    def _get_iopub_messages(self):
        while True and self._running:
            msg = json.loads(self._iopub.recv())
            self.iopub_messages.append(msg)
            # the kernel status idle message signifies the kernel is done
            if msg['header']['msg_type'] == 'status' and msg['content']['execution_state'] == 'idle':
                break

    def _make_request(self, msg_type, content):
        message = str(uuid4())

        # Here is the general form for an execute_request message
        request = {'header': {'msg_type': msg_type, 'msg_id': message, 'username': '', 'session': self.session},
                            'parent_header':{},
                            'metadata': {},
                            'content': content}

        return json.dumps(request)

    def kernel_info(self):
        return self._make_kernel_info_request()

    def _make_kernel_info_request(self):
        return self._make_request('kernel_info_request', {})

    def _make_execute_request(self, code, store_history=False):
        return self._make_request('execute_request', {})

    def close(self):
        # If we define this, we can use the closing() context manager to automatically close the channels
        self._shell.close()
        self._iopub.close()

    def get_results_from_response(self, response):
        results = self.get_streams_from_response(response)

        combined_result = combine_results(results)

        return combined_result 

    def _get_from_response(self, response, action):

        kernel_url = response['kernel_url']
        iopub = response['iopub']

        results = []

        # This will give us an ordering index to interleave results / images if we want.
        message_index = 0

        for message in iopub:
            message_index += 1

            if 'msg_type' not in message:
                continue

            result = action(message, message_index)
                
            if result is not None:
                results.append(result)

        return results

    def _check_and_grab(self, message, msg_type, traversal, creation):
        if message['msg_type'] == msg_type:
            node = traverse_down(*traversal)

            if node is not None:
                return creation(node)

    def get_streams_from_response(self, response):

        def action(message, message_index):
            cag = lambda *args: self._check_and_grab(message, *args)
            cr  = lambda indx, node, mimetype, result_type=ResultTypes.Stream: CR(result_type, indx, node, mimetype)

            def cr_err(indx, node):
                node['traceback'] = '\n'.join(node['traceback'])
                return CR(ResultTypes.Error, message_index, SageError(**node), 'text/x-python-traceback')

            msg_content = (message, 'content', 'data')

            def filter_display(node):
                filter_msgs = ('application/sage-interact', 'application/sage-clear',)
                for filter_msg in filter_msgs:
                    if traverse_down(*(msg_content + (filter_msg,))) is not None:
                        return None

                return cr(message_index, node, 'text/plain')

            checks = [cag('stream',
                          msg_content,
                          lambda node: cr(message_index, node, 'text/plain')),
                      cag('display_data',
                          msg_content + ('image/png',),
                          lambda node: cr(message_index, node, 'image/png')),
                      cag('display_data',
                          msg_content + ('text/html',),
                          lambda node: cr(message_index, node, 'text/html')),
                      cag('display_data',
                          msg_content + ('text/image-filename',),
                          lambda node: cr(message_index, self.kernel_url.replace('ws:','http:')+'files/'+node, 'text/image-filename', ResultTypes.Image)),
                      cag('display_data',
                          msg_content + ('text/plain',),
                          filter_display),
                      cag('pyerr',
                          (message, 'content'),
                          lambda node: cr_err(message_index, node)),
                      ] 

            # We are only going to do the first item.
            result = tuple(filter(lambda x: x is not None, checks))
            return result[0] if result else None

        return self._get_from_response(response, action)

    def __filter_on_mime(self, mimetype, streams):
        return tuple(filter(lambda x: x.mimetype == mimetype, streams))

    def get_errors_from_response(self, response):
        streams = self.get_streams_from_response(response)
        return self.__filter_on_mime('text/x-python-traceback', streams)

    def get_image_urls_from_response(self, response):
        streams = self.get_streams_from_response(response)
        return self.__filter_on_mime('text/image-filename', streams)

class SageCell(BaseClient):

    def _create_new_session(self):
        self.req_ses = self.io.requests.Session()
        s = self.req_ses
        self.session = str(uuid4())
        resp = s.post('http://sagecell.sagemath.org/kernel',
                     data={'accepted_tos':'true'},
                     headers={'Accept': 'application/json'})

        resp_json = resp.json()
        self.kernel_id = resp_json['id']
        self.session = resp_json['id']
        self.kernel_url = resp_json['ws_url'] + 'kernel/' + self.kernel_id + '/'

    def _send_first_message(self):
        return

    def _make_execute_request(self, code, store_history=False):
        content = {'code': code, 
                   'silent': False, 
                   'store_history' : store_history,
                   'user_variables': [], 
                   'user_expressions': {'_sagecell_files': 'sys._sage_.new_files()'}, 
                   'allow_stdin': False}
        return self._make_request('execute_request', content)

    def _code_keepalive(self, code):
        # we also require an interact to keep the kernel alive
        return "interact(lambda : None)\n" + code

    def _get_kernel_url(self, response):
        return response['ws_url']+'kernel/'+response['id']+'/'


class IPythonNotebookClient(BaseClient):

    def _create_new_session(self):
        self.req_ses = self.io.requests.Session()
        s = self.req_ses
        resp_login = s.get(url=self.url+'login',
                           headers={'Accept': 'application/json'})

        resp_new_notebook = s.post(url=self.url+'api/notebooks',
                                   headers={'Accept': 'application/json'})

        resp = resp_new_notebook.json()
        self.notebook_name = resp['name']
        self.notebook_path =  resp['path']

        self._json_session_info ={ 
                                    'notebook':
                                    { 
                                      'name': self.notebook_name,
                                      'path': self.notebook_path if self.notebook_path else ''
                                    }
                                 }
        self._json_session_info = json.dumps(self._json_session_info)
        resp = s.post(url=self.url+'api/sessions',
                                   data=self._json_session_info,
                                   headers={'Accept': 'application/json'})
        
        resp_json = resp.json()
        self.kernel_id = resp_json['kernel']['id']
        self.session = resp_json['id']
        self.kernel_url = self.url.replace('http:','ws:') + 'api/kernels/' + self.kernel_id + '/'

    def _send_first_message(self):
        self._shell.send(self.session + ":")
        self._iopub.send(self.session + ":")

    def cleanup(self):
        if self._running and self._shell.connected:
            self._shell.send(self._make_request('shutdown_request', {'restart': False}))
            self.req_ses.delete(url=self.url+'api/sessions/%s' % (self.session,),
                                headers={'Accept': 'application/json'})
            self.req_ses.delete(url=self.url+'api/notebooks/%s' %(self.notebook_name,),
                          data=self._json_session_info,
                          headers={'Accept': 'application/json'})

    def _make_execute_request(self, code, store_history=False):
        content = {'code': code, 
                   'silent': False, 
                   'store_history' : store_history,
                   'user_variables': [], 
                   'user_expressions': {}, 
                   'allow_stdin': False}
        return self._make_request('execute_request', content)

def traverse_down(collection, *args):
    node = collection
    for arg in args:
        if arg in node:
            node = node[arg]
        else:
            return None

    return node

def test_haskell():
    import pprint

    a=IHaskellNotebookClient('http://localhost:8899', timeout=None)
    result = a.execute_request("""
print "Hello1."
""", True)
    pprint.pprint(result)
    result = a.execute_request("""
-- We can draw diagrams, right in the notebook.
:extension NoMonomorphismRestriction
import Diagrams.Prelude

-- By Brent Yorgey
-- Draw a Sierpinski triangle!
sierpinski 1 = eqTriangle 1
sierpinski n =     s
                  ===
               (s ||| s) # centerX
  where s = sierpinski (n-1)

-- The `diagram` function is used to display them in the notebook.
diagram $ sierpinski 4
            # centerXY
            # fc black
          `atop` square 10
                   # fc white
""", True)
    pprint.pprint(result)
    a.cleanup()

def test_ipython_notebook():

    import pprint
    import sys
    c = IPythonNotebookClient('http://localhost:8888', timeout=None)
    resp = c.execute_request("""
%matplotlib inline
print("Hello.")
from math import pi
from numpy import array, arange, sin
import pylab as P

for knockoff in range(100):
    print(knockoff)

fig = P.figure()
x = arange(10.0)
y = sin(arange(10.0)/20.0*pi)

P.errorbar(x,y,yerr=0.1,capsize=3)

y = sin(arange(10.0)/20.0*pi) + 1
P.errorbar(x,y,yerr=0.1, uplims=True)

y = sin(arange(10.0)/20.0*pi) + 2
upperlimits = array([1,0]*5)
lowerlimits = array([0,1]*5)
P.errorbar(x, y, yerr=0.1, uplims=upperlimits, lolims=lowerlimits)

P.xlim(-1,10)

fig = P.figure()
x = arange(10.0)/10.0
y = (x+0.1)**2

P.errorbar(x, y, xerr=0.1, xlolims=True)
y = (x+0.1)**3

P.errorbar(x+0.6, y, xerr=0.1, xuplims=upperlimits, xlolims=lowerlimits)

y = (x+0.1)**4
P.errorbar(x+1.2, y, xerr=0.1, xuplims=True)

P.xlim(-0.2,2.4)
P.ylim(-0.1,1.3)

print("other item?")
P.show()

print("hello.")
raise Exception()
""")
    results = c.get_results_from_response(resp)
    pprint.pprint(resp)
    for result in results:
        print(str(result)[0:80])
    c.cleanup()

def main():
   
    print("-"*70)
    b = SageCell('http://sagecell.sagemath.org/')
    resp = b.execute_request("""
print 'Hi.'
H=Graph({0 : [1,2,3], 4 : [0, 2], 6 : [1,2,3,4,5]})
a = plot(H)
a.show()
import numpy

difference = 3

print "hi."

import pylab as plt

t = plt.arange(0.0, 2.0, 0.01)
s = plt.sin(2*pi*t)
plt.plot(t, s)
plt.xlabel('time (s)')
plt.ylabel('voltage (mV)')
plt.title('About as simple as it gets, folks')
plt.grid(True)
#savefig("test.png")
plt.show()
#factorial(2012)
""")
    pprint.pprint(resp)
    results = b.get_results_from_response(resp)
    pprint.pprint(results)


    return results

if __name__ == "__main__":
    main()
