import os
import time
import socket
import dash
import visdcc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from threading import Thread, Lock


BROKER_SERVER_HOST = '127.0.0.1'
BROKER_SERVER_PORT = 8090

PUBLISHER_SERVER_HOST = '127.0.0.1'
PUBLISHER_SERVER_PORT = 8091

SUBSCRIBER_SERVER_HOST = '127.0.0.1'
SUBSCRIBER_SERVER_PORT = 8092

REGISTER_BROKER = 'REGISTER_BROKER'
REGISTER_SUBSCRIBER = 'REGISTER_SUBSCRIBER'
REGISTER_PUBLISHER = 'REGISTER_PUBLISHER'
SUBSCRIPTION_IN = 'SUBSCRIPTION_IN'
PUBLICATION_IN = 'PUBLICATION_IN'
PUBLICATION_OUT = 'PUBLICATION_OUT'
NO_PARENT_PEER = 'NO_PARENT_PEER'

BROKER_PEERS_LIMIT = 2


root_selected = False
root_selected_lock = Lock()

brokers_network_table = {}
brokers_network_table_lock = Lock()

publishers_round_robin_index = 0
publishers_round_robin_index_lock = Lock()

subscribers_round_robin_index = 0
subscribers_round_robin_index_lock = Lock()

log_file = open('log.txt', 'w')
log_file_lock = Lock()


def run_broker_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((BROKER_SERVER_HOST, BROKER_SERVER_PORT))
        server_socket.listen()
        while True:
            conn, addr = server_socket.accept()
            handle_broker_thread = Thread(target=handle_broker, args=(conn, addr,))
            handle_broker_thread.start()
            
def run_publisher_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((PUBLISHER_SERVER_HOST, PUBLISHER_SERVER_PORT))
        server_socket.listen()
        while True:
            conn, addr = server_socket.accept()
            handle_publisher_thread = Thread(target=handle_publisher, args=(conn, addr,))
            handle_publisher_thread.start()
           
def run_subscriber_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((SUBSCRIBER_SERVER_HOST, SUBSCRIBER_SERVER_PORT))
        server_socket.listen()
        while True:
            conn, addr = server_socket.accept()
            handle_subscriber_thread = Thread(target=handle_subscriber, args=(conn, addr,))
            handle_subscriber_thread.start()
        
def handle_broker(conn, addr):
    global root_selected, root_selected_lock
    global brokers_network_table, brokers_network_table_lock

    conn_file = conn.makefile(mode='rw', encoding='utf-8')
    
    request_data = []
    for request in conn_file:
        request = request.strip()
        if len(request_data) < 3:
            request_data.append(request)
        if len(request_data) == 3:
            break
            
    broker_address = addr[0]
    broker_broker_server_port = request_data[0]
    broker_publishers_server_port = request_data[1]
    broker_subscribers_server_port = request_data[2]
            
    root_selected_lock.acquire()
    brokers_network_table_lock.acquire()
    if not root_selected:
        conn_file.write('{}\n'.format(NO_PARENT_PEER))
        conn_file.flush()
        root_selected = True
    else:
        peer_data = select_optimal_peer(brokers_network_table)
        conn_file.write('{}\n'.format(peer_data[0]))
        conn_file.write('{}\n'.format(peer_data[1]))
        conn_file.flush()
    brokers_network_table[(broker_address, broker_broker_server_port)] = {
        'ports': (broker_broker_server_port, broker_publishers_server_port, broker_subscribers_server_port),
        'peers': [],
        'publishers': 0,
        'subscribers': 0
    }
    brokers_network_table_lock.release()
    root_selected_lock.release()
    
    for request in conn_file:
        request = request.strip()
        request_parts = request.split()
        brokers_network_table_lock.acquire()
        if request_parts[0] == REGISTER_BROKER:
            if len(request_parts) < 3:
                brokers_network_table_lock.release()
                continue
            peer_host = request_parts[1]
            peer_port = request_parts[2]
            brokers_network_table[(broker_address, broker_broker_server_port)]['peers'].append((peer_host, peer_port))
        elif request_parts[0] == REGISTER_PUBLISHER:
            brokers_network_table[(broker_address, broker_broker_server_port)]['publishers'] += 1
        elif request_parts[0] == REGISTER_SUBSCRIBER:
            brokers_network_table[(broker_address, broker_broker_server_port)]['subscribers'] += 1
        elif request_parts[0] == SUBSCRIPTION_IN:
            log_file_lock.acquire()
            print('{} SUBSCRIPTION_IN {}'.format(time.time(), request_parts[1]), file=log_file)
            log_file_lock.release()
        elif request_parts[0] == PUBLICATION_IN:
            log_file_lock.acquire()
            print('{} PUBLICATION_IN {}'.format(time.time(), request_parts[1]), file=log_file)
            log_file_lock.release()
        elif request_parts[0] == PUBLICATION_OUT:
            log_file_lock.acquire()
            print('{} PUBLICATION_OUT {}'.format(time.time(), request_parts[1]), file=log_file)
            log_file_lock.release()
        brokers_network_table_lock.release()
        
def handle_publisher(conn, addr):
    global publishers_round_robin_index, publishers_round_robin_index_lock
    global brokers_network_table, brokers_network_table_lock
    
    brokers_network_table_lock.acquire()
    if len(brokers_network_table) == 0:
        conn.close()
        brokers_network_table_lock.release()
        return
   
    publishers_round_robin_index_lock.acquire()
    broker_key = list(brokers_network_table.keys())[publishers_round_robin_index]
    broker_host = broker_key[0]
    broker_port = brokers_network_table[broker_key]['ports'][1]
    publishers_round_robin_index = (publishers_round_robin_index + 1) % len(brokers_network_table)
    publishers_round_robin_index_lock.release()
    brokers_network_table_lock.release()
    
    conn_file = conn.makefile(mode='rw', encoding='utf-8')
    conn_file.write('{}\n'.format(broker_host))
    conn_file.write('{}\n'.format(broker_port))
    conn_file.flush()
    
    conn.close()

def handle_subscriber(conn, addr):
    global subscribers_round_robin_index, subscribers_round_robin_index_lock
    global brokers_network_table, brokers_network_table_lock
    
    brokers_network_table_lock.acquire()
    if len(brokers_network_table) == 0:
        conn.close()
        brokers_network_table_lock.release()
        return
    
    subscribers_round_robin_index_lock.acquire()
    broker_key = list(brokers_network_table.keys())[subscribers_round_robin_index]
    broker_host = broker_key[0]
    broker_port = brokers_network_table[broker_key]['ports'][2]
    subscribers_round_robin_index = (subscribers_round_robin_index + 1) % len(brokers_network_table)
    subscribers_round_robin_index_lock.release()
    brokers_network_table_lock.release()
    
    conn_file = conn.makefile(mode='rw', encoding='utf-8')
    conn_file.write('{}\n'.format(broker_host))
    conn_file.write('{}\n'.format(broker_port))
    conn_file.flush()
    
    conn.close()
    
def select_optimal_peer(brokers_network_table):
    for node, data in brokers_network_table.items():
        if len(data['peers']) < BROKER_PEERS_LIMIT:
            return node
    return None


app = dash.Dash()
app.layout = html.Div(
    style={
        'backgroundColor': '#1c1c1c'
    },
    children=[
        visdcc.Network(
            id='network',
            data={
                'nodes': [],
                'edges': []
            },
            options={
                'width': '1000px',
                'height': '1000px'
            },
            style={
                'background-color': 'white',
                'margin': '0px'
            }
        ),
        dcc.Interval(id='update_network_data', interval=5 * 1000, n_intervals=0)
    ]
)

@app.callback(
    Output('network', 'data'),
    [Input('update_network_data', 'n_intervals')]
)
def update_network_data(n_intervals):
    global brokers_network_table_lock
    global brokers_network_table
    
    brokers_network_table_lock.acquire()
    
    nodes = [{
            'id': str(key),
            'label': 'B',
            'color': 'red',
            'labelHighlightBold': True,
            'shape': 'dot',
            'size': 15,
            'physics': True
        } for key, data in brokers_network_table.items()
    ]
    publisher_nodes = [{
            'id': '{}pub{}'.format(str(key), str(i)),
            'label': 'P',
            'color': 'green',
            'labelHighlightBold': True,
            'shape': 'dot',
            'size': 15,
            'physics': True
        } for key, data in brokers_network_table.items() for i in range(data['publishers'])
    ]
    subscriber_nodes = [{
            'id': '{}sub{}'.format(str(key), str(i)),
            'label': 'S',
            'color': 'blue',
            'labelHighlightBold': True,
            'shape': 'dot',
            'size': 15,
            'physics': True
        } for key, data in brokers_network_table.items() for i in range(data['subscribers'])
    ]
    nodes.extend(publisher_nodes)
    nodes.extend(subscriber_nodes)
    
    edges = [{
            'id': '{}-{}'.format(str(key), str(peer_key)),
            'from': str(key),
            'to': str(peer_key),
            'width': 1,
            'color': {
                'color': 'black',
                'inherit': False
            },
            'physics': True
        } for key, data in brokers_network_table.items() for peer_key in data['peers'] 
    ]
    publisher_edges = [{
            'id': '{}-{}'.format(str(key), '{}pub{}'.format(str(key), str(i))),
            'from': str(key),
            'to': '{}pub{}'.format(str(key), str(i)),
            'width': 1,
            'color': {
                'color': 'black',
                'inherit': False
            },
            'physics': True
        } for key, data in brokers_network_table.items() for i in range(data['publishers'])
    ]
    subscriber_edges = [{
            'id': '{}-{}'.format(str(key), '{}sub{}'.format(str(key), str(i))),
            'from': str(key),
            'to': '{}sub{}'.format(str(key), str(i)),
            'width': 1,
            'color': {
                'color': 'black',
                'inherit': False
            },
            'physics': True
        } for key, data in brokers_network_table.items() for i in range(data['subscribers'])
    ]
    edges.extend(publisher_edges)
    edges.extend(subscriber_edges)
    
    brokers_network_table_lock.release()
    return {
        'nodes': nodes,
        'edges': edges
    }


if __name__ == '__main__':
    run_broker_server_thread = Thread(target=run_broker_server)
    run_broker_server_thread.start()
    
    run_publisher_server_thread = Thread(target=run_publisher_server)
    run_publisher_server_thread.start()
    
    run_subscriber_server_thread = Thread(target=run_subscriber_server)
    run_subscriber_server_thread.start()
    
    app.run_server(host='0.0.0.0')
