#!/usr/bin/env python
# coding: utf-8
# @author          ozhang@
# @namespace       amazon

import json
import datetime
import uuid
import argparse

# Urgly resolve on dependency modules
import subprocess
import sys
try:
    import requests
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'requests'])
finally:
    import requests
    
try:
    from websocket import create_connection
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'websocket', 'websocket-client'])
finally:
    from websocket import create_connection



def send_execute_request(code):
    msg_type = 'execute_request';
    content = { 'code' : code, 'silent':False }
    hdr = { 'msg_id' : uuid.uuid1().hex, 
        'username': 'test', 
        'session': uuid.uuid1().hex, 
        'data': datetime.datetime.now().isoformat(),
        'msg_type': msg_type,
        'version' : '5.3' }
    msg = { 'header': hdr, 'parent_header': hdr, 
        'metadata': {},
        'content': content }
    return msg

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("kernel_name", type=str)
    parser.add_argument("-p","--port", type=str)
    parser.add_argument("-a","--ip_address", type=str)    
    parser.add_argument("-f","--notebook_file_path", type=str)
    parser.add_argument("-c","--command", type=str)
    parser.add_argument("-i","--kernel_id", type=str)
    parser.add_argument("-d","--delete")        
    args = parser.parse_args()
        
    if (not args.notebook_file_path and not args.command) or (args.notebook_file_path and args.command):
        raise ValueError('One and only one of --notebook_file_path or --command should be specified.')
    
    if not args.port:
        jkg_port = '8888'
    else:
        jkg_port = args.port
    

    base = 'http://' + args.ip_address + ':' + jkg_port  
    
    
    if not args.kernel_id:
    # Start a new kernel
        data = {"name": args.kernel_name, "env":{"KERNEL_LAUNCH_TIMEOUT":"40","KERNEL_WORKING_PATH": ""},"path":""}

        url = base + '/api/kernels'
        response = requests.post(url, data=json.dumps(data))
        if response.ok:
            kernel = json.loads(response.text)
            print(f"Kernel Started:\n{kernel}")
        else:
            print(response.text)    
    
    
    # Load the notebook and get the code of each cell
    if args.notebook_file_path:
        notebook_path = args.notebook_file_path
        file = json.load(open(notebook_path, 'rt'))
        code = [ ''.join(c['source']) for c in file['cells'] if len(c['source'])>0 ]
    elif args.command:
        code = [args.command.replace('\\n','\n')]
    print(f"code to be executed: {code}")
    
    if not args.kernel_id:
        kid = kernel["id"]
    else:
        kid = args.kernel_id

    # Execution request/reply is done on websockets channels
    ws = create_connection("ws://" + args.ip_address + ':' + jkg_port + "/api/kernels/" + kid + "/channels")
    for c in code:
        ws.send(json.dumps(send_execute_request(c)))

    # retrieve response to each cell
    print('------------------------------')
    for i in range(0, len(code)):
        msg_type = '';
        while msg_type != "execute_reply":
            rsp = json.loads(ws.recv())
            msg_type = rsp["msg_type"]
            print(f'message type: <{rsp["msg_type"]}>, message content:\n')
            for d in rsp["content"].values():
                print(d)
        print('------------------------------')

    if args.delete is not None:
        url = base + '/api/kernels/' + kid
        response = requests.delete(url)
        print(f'Delete kernel {kernel["id"]}: {response.status_code}')
