import pandas as pd

# Load the datasets
dataset1 = pd.read_csv('../data/input/dataset1.csv')  # Update with your file path
dataset2 = pd.read_csv('../data/input/dataset2.csv')  # Update with your file path

# Merge dataset1 with dataset2 on 'counter_party' to get 'tier'
merged_data = pd.merge(dataset1, dataset2, on='counter_party', how='left')

# Perform the required aggregations
grouped = merged_data.groupby(['legal_entity', 'counter_party', 'tier'])

aggregated_data = grouped.agg(
   # tier=('tier', 'max'),
    max_rating=('rating', 'max'),
    sum_value_ARAP=('value', lambda x: x[merged_data['status'] == 'ARAP'].sum()),
    sum_value_ACCR=('value', lambda x: x[merged_data['status'] == 'ACCR'].sum())
).reset_index()

# Calculate totals for each category - legal_entity, counter_party, tier
total_legal_entity = aggregated_data.groupby('legal_entity').agg(
    #tier=('tier', 'max'),
    max_rating=('max_rating', 'max'),
    sum_value_ARAP=('sum_value_ARAP', 'sum'),
    sum_value_ACCR=('sum_value_ACCR', 'sum')
).reset_index()
total_legal_entity['counter_party'] = 'Total'
total_legal_entity['tier'] = 'Total'

total_counter_party = aggregated_data.groupby('counter_party').agg(
    #tier=('tier', 'max'),
    max_rating=('max_rating', 'max'),
    sum_value_ARAP=('sum_value_ARAP', 'sum'),
    sum_value_ACCR=('sum_value_ACCR', 'sum')
).reset_index()
total_counter_party['legal_entity'] = 'Total'
total_counter_party['tier'] = 'Total'

total_tier = aggregated_data.groupby('tier').agg(
    max_rating=('max_rating', 'max'),
    sum_value_ARAP=('sum_value_ARAP', 'sum'),
    sum_value_ACCR=('sum_value_ACCR', 'sum')
).reset_index()
total_tier['legal_entity'] = 'Total'
total_tier['counter_party'] = 'Total'

# Concatenate all results and totals
result = pd.concat([aggregated_data, total_legal_entity, total_counter_party, total_tier], ignore_index=True)
result = result.sort_values(by=['legal_entity', 'counter_party', 'tier']).reset_index(drop=True)

    #result.replace('calculated_value', 999, inplace=True)

# Save the result to an output file
result.to_csv('../data/output/output_panda2.csv', index=False)
