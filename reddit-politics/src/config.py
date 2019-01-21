import os.path
from pathlib import Path

DIR_PATH = Path(__file__).parents[1]

DICTIONARY_PATH = os.path.join(DIR_PATH, 'resources', 'dictionary.dict')
CORPUS_PATH = os.path.join(DIR_PATH, 'resources', 'corpus.mm')
HDP_MODEL_PATH = os.path.join(DIR_PATH, 'resources', 'model.hdp')
LSI_MODEL_PATH = os.path.join(DIR_PATH, 'resources', 'model.lsi')

# Replace with actual values
mysql = {'host': HOST,
         'port': PORT,
         'user': USER_NAME,
         'password': PASSWORD,
         'db': DATABASE_NAME }

submission_table_name = 'submission'

hdp_model = {'max_chunks': None,
             'max_time': None,
             'tau': 64.0 }
