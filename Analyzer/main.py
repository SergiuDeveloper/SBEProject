EQUALS_25_FILE = 'data/log_equals_25.txt'
EQUALS_100_FILE = 'data/log_equals_100.txt'

SUBSCRIPTION_IN = 'SUBSCRIPTION_IN'
PUBLICATION_IN = 'PUBLICATION_IN'
PUBLICATION_OUT = 'PUBLICATION_OUT'

SUBSCRIBERS = 3



def read_data(data_file_path):
    data = {}
    
    with open(data_file_path, 'r') as data_file:
        lines = data_file.readlines()
        lines = [line.split() for line in lines]
        subscription_in_lines = [line for line in lines if line[1] == SUBSCRIPTION_IN]
        publication_in_lines = [line for line in lines if line[1] == PUBLICATION_IN]
        publication_out_lines = [line for line in lines if line[1] == PUBLICATION_OUT]
        lines = subscription_in_lines
        lines.extend(publication_in_lines)
        lines.extend(publication_out_lines)
        
        for line in lines:
            timestamp = line[0]
            log_type = line[1]
            log_data = line[2]
            
            if log_data not in data:
                data[log_data] = []
                
            if log_type == PUBLICATION_IN:
                data[log_data].append({
                    'emission': timestamp,
                    'arrivals': []
                })
            elif log_type == PUBLICATION_OUT:
                if len(data[log_data]) == 0:
                    continue
                data[log_data][-1]['arrivals'].append(timestamp)
                
    return data
    
def analyze_data(data):
    emitted_publications = sum([len(data[log_data]) for log_data in data])
    arrived_publications = sum([len(emission_batch['arrivals']) for log_data in data for emission_batch in data[log_data]])
    matching_rate = arrived_publications / (emitted_publications * SUBSCRIBERS)
    
    latencies = []
    for log_data in data:
        for emission_batch in data[log_data]:
            emission = float(emission_batch['emission'])
            arrivals = [float(arrival) for arrival in emission_batch['arrivals']]
            if len(arrivals) == 0:
                continue
            arrivals_mean = sum(arrivals) / len(arrivals)
            latency = arrivals_mean - emission
            latencies.append(latency)
    mean_latency = sum(latencies) / len(latencies)
    
    return {
        'emitted_publications': emitted_publications,
        'arrived_publications': arrived_publications,
        'matching_rate': matching_rate,
        'mean_latency': mean_latency
    }


if __name__ == '__main__':
    data_25 = read_data(EQUALS_25_FILE)
    data_100 = read_data(EQUALS_100_FILE)
    
    analysis_25 = analyze_data(data_25)
    analysis_100 = analyze_data(data_100)
    
    print()
    print('===== Statistics for 25% equals operator frequency =====')
    print('Emitted publications: {:,}'.format(analysis_25['emitted_publications']))
    print('Arrived publications: {:,}'.format(analysis_25['arrived_publications']))
    print('Matching rate: {:.2f}%'.format(analysis_25['matching_rate'] * 100))
    print('Mean latency: {:.2f} seconds'.format(analysis_25['mean_latency']))
    
    print()
    print('===== Statistics for 100% equals operator frequency =====')
    print('Emitted publications: {:,}'.format(analysis_100['emitted_publications']))
    print('Arrived publications: {:,}'.format(analysis_100['arrived_publications']))
    print('Matching rate: {:.2f}%'.format(analysis_100['matching_rate'] * 100))
    print('Mean latency: {:.2f} seconds'.format(analysis_100['mean_latency']))
