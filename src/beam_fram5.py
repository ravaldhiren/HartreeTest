
import apache_beam as beam
import logging

class ProcessDataset1(beam.DoFn):
    def process(self, element):
        lines = element.split('\r')

        if lines[0].startswith('invoice_id,legal_entity,counter_party,rating,status,value'):
            lines = lines[1:]

        for line in lines:
            elements = line.split(',')
            if len(elements) == 6:
                invoice_id, legal_entity, counter_party, rating, status, value = elements
                yield {
                    'legal_entity': legal_entity,
                    'counter_party': counter_party,
                    'rating': int(rating),
                    'status': status,
                    'value': float(value)
                }
            else:
                logging.warning(f"Issue with splitting in ProcessDataset1: {elements}")


class ProcessDataset2(beam.DoFn):
    def process(self, element):
        lines = element.split('\r')

        if lines[0].startswith('counter_party,tier'):
            lines = lines[1:]

        for line in lines:
            elements = line.split(',')
            if len(elements) == 2:
                counter_party, tier = elements
                yield {
                    'counter_party': counter_party,
                    'tier': float(tier)
                }
            else:
                logging.warning(f"Issue with splitting in ProcessDataset2: {elements}")


def calculate_stats(element):
    #print(element)
    counter_party = element[0]
    dataset1_info = list(element[1]['dataset1'])
    dataset2_info = list(element[1]['dataset2'])

    legal_entity = dataset1_info[0]['legal_entity']

    max_rating = max(info['rating'] for info in dataset1_info)

    sum_arap = sum(info['value'] for info in dataset1_info if info['status'] == 'ARAP')
    sum_accr = sum(info['value'] for info in dataset1_info if info['status'] == 'ACCR')

    tier = next((info['tier'] for info in dataset2_info if info['counter_party'] == counter_party), None)

    output_data = {
        'legal_entity': legal_entity,
        'counter_party': counter_party,
        'tier': tier,
        'max_rating_by_counter_party': max_rating,
        'sum_value_ARAP': sum_arap,
        'sum_value_ACCR': sum_accr
    }

    return [
        ((legal_entity, counter_party, 'Total'), output_data),
        ((legal_entity, 'Total', 'Total'), output_data),
        (('Total', counter_party, 'Total'), output_data),
        (('Total', 'Total', tier), output_data)
    ]


# def generate_output_data(stats):
#    # print(stats[1][0])
#     output_data = {
#         'legal_entity': stats[0][0],
#         'counter_party': stats[0][1],
#         'tier': stats[0][2],
#         'max_rating_by_counter_party': stats[1][0]['max_rating_by_counter_party'],
#         'sum_value_ARAP': stats[1][0]['sum_value_ARAP'],
#         'sum_value_ACCR': stats[1][0]['sum_value_ACCR']
#     }
#     return output_data

# def generate_output_data(stats):
#     #stats_tuple, stats_dict_list = stats
#     unique_data = set()
#     for state in stats[1]:
#         unique_key  =(
#                 state['legal_entity'],
#                 state['counter_party'],
#                 state['tier'],
#                 state['max_rating_by_counter_party'],
#                 state['sum_value_ARAP'],
#                 state['sum_value_ACCR']
#             )
#         unique_data.add(unique_key)
#
#     for entry in unique_data:
#         yield ','.join(map(str,entry))

def generate_output_data(stats):
    # Grouping the stats by the key (legal_entity, counter_party, tier)
    grouped_stats = {}
    #print(stats[1])
    for v in stats[1]:
        key = (v['legal_entity'], v['counter_party'], v['tier'])
        if key not in grouped_stats:
            grouped_stats[key] = {
                'max_rating_by_counter_party': v['max_rating_by_counter_party'],
                'sum_value_ARAP': 0,
                'sum_value_ACCR': 0
            }
        grouped_stats[key]['sum_value_ARAP'] += v['sum_value_ARAP']
        grouped_stats[key]['sum_value_ACCR'] += v['sum_value_ACCR']

    # Output the grouped data
    for key, value in grouped_stats.items():
        output_data = [
            key[0],  # legal_entity
            key[1],  # counter_party
            key[2],  # tier
            value['max_rating_by_counter_party'],
            value['sum_value_ARAP'],
            value['sum_value_ACCR']
        ]
        yield ','.join(map(str, output_data))

def add_total_rows(stats):
    legal_entity, counter_party, tier = stats[0]
    stats_data = stats[1]

    # Calculate the max rating for the counter party
    max_rating_total = max(data['max_rating_by_counter_party'] for data in stats_data)

    # Calculate the sum of ARAP and ACCR values for the counter party
    sum_arap_total = sum(data['sum_value_ARAP'] for data in stats_data)
    sum_accr_total = sum(data['sum_value_ACCR'] for data in stats_data)

    # Add the total rows
    total_legal_entity = 'Total'
    total_counter_party = 'Total'
    total_tier = 'Total'

    total_rows = [
        (total_legal_entity, total_counter_party, total_tier, max_rating_total, sum_arap_total, sum_accr_total),
        (legal_entity, total_counter_party, total_tier, max_rating_total, sum_arap_total, sum_accr_total),
        (total_legal_entity, counter_party, total_tier, max_rating_total, sum_arap_total, sum_accr_total)
    ]

    # Append the total rows to the existing data
    stats_data.extend(total_rows)

    return ((legal_entity, counter_party, tier), stats_data)


with beam.Pipeline() as p:
    input_collection1 = (p | 'ReadDataset1' >> beam.io.ReadFromText('..\\data\\input\\dataset1.csv'))
    input_collection2 = (p | 'ReadDataset2' >> beam.io.ReadFromText('..\\data\\input\\dataset2.csv'))

    processed_dataset1 = (input_collection1 | 'ProcessDataset1' >> beam.ParDo(ProcessDataset1()))
    processed_dataset2 = (input_collection2 | 'ProcessDataset2' >> beam.ParDo(ProcessDataset2()))

    keyed_dataset1 = processed_dataset1 | 'AddKey1' >> beam.Map(lambda x: (x['counter_party'], x))
    keyed_dataset2 = processed_dataset2 | 'AddKey2' >> beam.Map(lambda x: (x['counter_party'], x))

    merged_data = {'dataset1': keyed_dataset1, 'dataset2': keyed_dataset2} | beam.CoGroupByKey()

    calculated_stats = merged_data | beam.FlatMap(calculate_stats)

    grouped_stats = calculated_stats | 'GroupStats' >> beam.GroupByKey()

    output_data = grouped_stats | 'PrepareOutput' >> beam.Map(add_total_rows)

    #output_data = grouped_stats | 'PrepareOutput' >> beam.FlatMap(generate_output_data)

    file_path  = '..\\data\\output\\output_totalsfrm1.csv'

    output_data | 'WriteTotals' >> beam.io.WriteToText(file_path, file_name_suffix=".csv", header='legal_entity, counter_party, tier, max_rating_by_counter_party, sum_value_ARAP, sum_value_ACCR', num_shards=1)


