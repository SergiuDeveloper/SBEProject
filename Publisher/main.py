import socket
import random
import json
from datetime import date, timedelta


MASTER_HOST = '127.0.0.1'
MASTER_PORT = 8091


def get_broker_address():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((MASTER_HOST, MASTER_PORT))
        
        conn_file = sock.makefile(mode='r', encoding='utf-8')
        response_data = []
        for response in conn_file:
            response = response.strip()
            if len(response_data) < 2:
                response_data.append(response)
            if len(response_data) == 2:
                break
        broker_host = response_data[0]
        broker_port = int(response_data[1])
        
        sock.close()
        
        return broker_host, broker_port
        
def connect_to_broker(broker_host, broker_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((broker_host, broker_port))
        conn_file = sock.makefile(mode='w', encoding='utf-8')
        
        publications_generator = generate_publications(
            ['Google', 'Microsoft', 'Facebook', 'Twitter', 'Amazon', 'Uber', 'Glovo'],
            list(generate_dates_between(date(2010, 4, 12), date(2018, 1, 1))),
            -30., 30.,
            -10., 10.,
            0.1, 0.8
        )
        for publication in publications_generator:
            conn_file.write('{}\n'.format(json.dumps(publication)))
            conn_file.flush()
        
def generate_publications(companies, dates, value_min, value_max, drop_min, drop_max, variation_min, variation_max):
    while True:
        company = random.choice(companies)
        date = random.choice(dates)
        value = random.uniform(value_min, value_max)
        drop = random.uniform(drop_min, drop_max)
        variation = random.uniform(variation_min, variation_max)

        publication = {
            'company': company,
            'date': date,
            'value': value,
            'drop': drop,
            'variation': variation
        }
        yield publication
        
def generate_dates_between(start_date, end_date):
    delta = end_date - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        yield str(day)

if __name__ == '__main__':
    broker_host, broker_port = get_broker_address()
    connect_to_broker(broker_host, broker_port)
