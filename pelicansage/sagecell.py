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

CellResult = NT('CellResult', 'type order data')
SageError = NT('SageError', 'ename evalue traceback')
SageImage = NT('SageImage', 'file_name url')

CR = CellResult

class BaseClient(object):
    def __init__(self, url, timeout=10, io=None):

        self.io = pelicansageio if io is None else io

        if not url.endswith('/'):
            url+='/'
        self.url = url
        self.timeout = timeout
        
        self.req = None

        self.reset()
    
    def reset(self):

        self._running = False

    def _code_keepalive(self, code):
        return code

    def _get_kernel_url(self, response):
        raise NotImplemented()

    def execute_request(self, code, store_history=False):
        # zero out our list of messages, in case this is not the first request
        if not self._running:
            resp = self.io.get_response(self.req)
            response = json.loads(resp)
            self.session = str(uuid4())

            # RESPONSE: {"id": "ce20fada-f757-45e5-92fa-05e952dd9c87", "ws_url": "ws://localhost:8888/"}
            # construct the iopub and shell websocket channel urls from that

            self.kernel_url = self._get_kernel_url(response)
            websocket.setdefaulttimeout(self.timeout)
            self._shell = websocket.create_connection(self.kernel_url+'shell')
            self._iopub = websocket.create_connection(self.kernel_url+'iopub')

            self._running = True

            code = self._code_keepalive(code)

        self.shell_messages = []
        self.iopub_messages = []

        self._shell.send(self._make_kernel_info_request())

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
        results.extend(self.get_image_urls_from_response(response))
        results.extend(self.get_errors_from_response(response))
        results.sort(key=lambda x: x.order)
        results = [list(items) for order, items in groupby(results, key=lambda x: x.type)]

        for indx in range(len(results)):
            if results[indx][0].type == ResultTypes.Stream:
                results[indx] = [CR(ResultTypes.Stream, 
                                    results[indx][0].order, 
                                    ''.join([x.data for x in results[indx]]))]
        
        return [item for sublist in results for item in sublist]

    def get_streams_from_response(self, response):

        kernel_url = response['kernel_url']
        iopub = response['iopub']

        results = []

        # This will give us an ordering index to interleave results / images if we want.
        message_index = 0

        for message in iopub:
            message_index += 1
            if 'msg_type' in message and message['msg_type'] == "stream":
                node = traverse_down(message, 'content', 'data')

                if node is not None:
                    results.append(CR(ResultTypes.Stream, message_index, node))

        return results

    def get_errors_from_response(self, response):

        kernel_url = response['kernel_url']
        iopub = response['iopub']

        results = []

        # This will give us an ordering index to interleave results / images if we want.
        message_index = 0

        for message in iopub:
            message_index += 1
            if 'msg_type' in message and message['msg_type'] == "pyerr":
                node = traverse_down(message, 'content')

                if node is not None:
                    node['traceback'] = '\n'.join(node['traceback'])
                    results.append(CR(ResultTypes.Error, message_index, SageError(**node)))

        return results

    def get_image_urls_from_response(self, response):

        kernel_url = response['kernel_url']
        iopub = response['iopub']
        file_url_base = kernel_url.replace('ws:','http:') + 'files/' 
        file_urls = []

        # This will give us an ordering index to interleave results / images if we want.
        message_index = 0

        for message in iopub:
            message_index += 1
            if 'msg_type' in message and message['msg_type'] == "display_data":
                node = traverse_down(message, 'content', 'data', 'text/image-filename')

                if node is not None:
                    file_url = file_url_base + node
                    file_urls.append(CR(ResultTypes.Image, message_index, SageImage(node, file_url_base + node)))

        return file_urls

class SageCell(BaseClient):

    def __init__(self, *args, **kwargs):
        super(SageCell,self).__init__(*args, **kwargs)

        # POST or GET <url>/kernel
        # if there is a terms of service agreement, you need to
        # indicate acceptance in the data parameter below (see the API docs)
        self.req = self.io.Request(url=self.url+'kernel',
                       data=self.io.to_bytes('accepted_tos=true'),
                       headers={'Accept': 'application/json'})

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

class IHaskellClient(BaseClient):

    def __init__(self, *args, **kwargs):
        super(IHaskellClient, self).__init__(*args, **kwargs)

        self.req = self.io.Request(method="POST",url=self.url+'api/kernels',
                                   headers={'Accept': 'application/json'})


    def _make_execute_request(self, code, store_history=False):
        content = {'code': code, 
                   'silent': False, 
                   'store_history' : store_history,
                   'user_variables': [], 
                   'user_expressions': {}, 
                   'allow_stdin': False}
        return self._make_request('execute_request', content)

    def _get_kernel_url(self, response):
        url = self.url.replace('http','ws') + 'api/kernels/' + response['id'] + '/'

        return url


def traverse_down(collection, *args):
    node = collection
    for arg in args:
        if arg in node:
            node = node[arg]
        else:
            return None

    return node

def main():
    import sys
    # argv[1] is the web address
    a=IHaskellClient('http://localhost:8080', timeout=5)
    import pprint
    result = a.execute_request("""
putStrLn "Hello."
""")
    return result 

if __name__ == "__main__":
    main()
