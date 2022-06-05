import socket
import random
import json
import math
from datetime import date, timedelta


MASTER_HOST = '127.0.0.1'
MASTER_PORT = 8092


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
        conn_file = sock.makefile(mode='rw', encoding='utf-8')
        
        subscriptions_generator = generate_subscriptions(
            3333,
            {
                'company': 0.2,
                'value': 0.2,
                'variation': 0.2,
                'drop': 0.2,
                'date': 0.2
            },
            {
                'company': 0.9,
                'value': 0.7,
                'variation': 0.2,
                'drop': 0.2,
                'date': 0.2
            },
            ['>', '<', '=', '>=', '<=', '!='],
            ['Google', 'Microsoft', 'Facebook', 'Twitter', 'Amazon', 'SpaceX', 'Tesla'],
            list(generate_dates_between(date(2015, 6, 22), date(2022, 8, 27))),
            -50., 15.5,
            -40., 3.,
            0.55, 0.67
        )
        for subscription in subscriptions_generator:
            conn_file.write('{}\n'.format(json.dumps(subscription)))
            conn_file.flush()
            
        for response in conn_file:
            response = response.strip()
            print(response)
        
def generate_subscriptions(subscriptions_count, fields_frequency, field_equals_frequency, operators, companies, dates, value_min, value_max, drop_min, drop_max, variation_min, variation_max):
    max_subscriptions = subscriptions_count
    # Convert from percentages to values
    for field in fields_frequency.keys():
        fields_frequency[field] = min(1., fields_frequency[field])
        fields_frequency[field] = int(math.floor(subscriptions_count * fields_frequency[field]))
        if field in field_equals_frequency.keys():
            field_equals_frequency[field] = min(1., field_equals_frequency[field])
            field_equals_frequency[field] = int(math.floor(fields_frequency[field] * field_equals_frequency[field]))

    remaining_fields = ['company', 'date', 'value', 'drop', 'variation']

    # Generate subscriptions in order to satisfy constraints
    generated_subscriptions = []
    index_sub = 0
    while subscriptions_count > 0:
        subscription = []
        finished_adding = True
        for field in fields_frequency.keys():
            value = None
            if field == 'company':
                value = random.choice(companies)
            elif field == 'date':
                value = random.choice(dates)
            elif field == 'value':
                value = random.uniform(value_min, value_max)
            elif field == 'drop':
                value = random.uniform(drop_min, drop_max)
            elif field == 'variation':
                value = random.uniform(variation_min, variation_max)
            if value is None:
                continue

            if fields_frequency[field] > 0:
                finished_adding = False
                if field in field_equals_frequency.keys() and field_equals_frequency[field] > 0:
                    tmp_field = (field, '=', value)
                    field_equals_frequency[field] -= 1
                else:
                    operator = random.choice(operators)
                    tmp_field = (field, operator, value)

                # Insert new fields until we reach max subscription number then append to existing ones
                # checking if fields aren't already present in that subscription
                next_free_index = index_sub
                if len(generated_subscriptions) < max_subscriptions:
                    generated_subscriptions.insert(index_sub, [tmp_field])
                else:
                    collision = True
                    while collision:
                        if any(field in i for i in generated_subscriptions[next_free_index]):
                            next_free_index += 1
                        else:
                            generated_subscriptions[next_free_index].append(tmp_field)
                            collision = False
                # don't skip the subscriptions that got collision
                if next_free_index == index_sub:
                    index_sub += 1
                    index_sub %= max_subscriptions
                fields_frequency[field] -= 1
                if fields_frequency[field] == 0:
                    remaining_fields.remove(field)

        if finished_adding:
            break

        subscriptions_count -= 1
    
    generated_subscriptions = [{
        'conditions': [{
            'field': str(condition[0]),
            'operator': str(condition[1]),
            'value': str(condition[2])
        } for condition in subscription]
    } for subscription in generated_subscriptions]

    return generated_subscriptions
        
def generate_dates_between(start_date, end_date):
    delta = end_date - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        yield str(day)
        
        
if __name__ == '__main__':
    broker_host, broker_port = get_broker_address()
    connect_to_broker(broker_host, broker_port)
