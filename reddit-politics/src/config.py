import os
import os.path

DIR_PATH = os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir))

# Replace with actual values
mysql = {'host': HOST,
         'port': PORT,
         'user': USER_NAME,
         'passwd': PASSWORD,
         'db': DATABASE_NAME }
praw =  {'client_id': REDDIT_API_CLIENT_ID,
         'client_secret': REDDIT_API_CLIENT_SECRET,
         'user_agent': USER_AGENT }
