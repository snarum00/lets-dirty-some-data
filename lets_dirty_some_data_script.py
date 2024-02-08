import random
import pandas as pd

FILE_NAME = 'BankCustomers.csv'
FIELDS_TO_ADD_NULLS = ['credit_limit','customer_age','income_category']
FIELDS_TO_CREATE_DUPS = ['clientnum']

MAX_NUM_OF_NULL_ROWS = 20
MAX_NUM_OF_DUP_ROWS = 5

df = pd.read_csv(FILE_NAME, dtype=object)
RANGE_OF_INDEXES = range(len(df))

### ADD NULLS ###
dict_of_null_indexes = {}
for field_name in FIELDS_TO_ADD_NULLS:
	dict_of_null_indexes[field_name]=random.sample(RANGE_OF_INDEXES, random.randint(2, MAX_NUM_OF_NULL_ROWS))

for key in dict_of_null_indexes.keys():
	for num in dict_of_null_indexes[key]:
		df.at[num, key] = None

## CREATE DUPLICATES ###
dict_of_dup_indexes = {}

for field_name in FIELDS_TO_CREATE_DUPS:
	dict_of_dup_indexes[field_name] = {}
	dict_of_dup_indexes[field_name]['row_list'] = random.sample(RANGE_OF_INDEXES, random.randint(2, MAX_NUM_OF_DUP_ROWS))
	dict_of_dup_indexes[field_name]['value_for_duplicates'] = df.at[dict_of_dup_indexes[field_name]['row_list'][0],field_name]

for key in dict_of_dup_indexes.keys():
	for num in dict_of_dup_indexes[key]['row_list'][1:]:
		df.at[num, key] = dict_of_dup_indexes[key]['value_for_duplicates']

### CREATE NEW FILE ###
df.to_csv(f'new_{FILE_NAME}', index = False)



