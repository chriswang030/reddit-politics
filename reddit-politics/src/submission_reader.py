import datetime as dt
import praw
from psaw import PushshiftAPI
import pymysql
import submission_writer as sw
import yaml

PATH = '/Users/Chris/Documents/workspace-py' # path to working directory

with open(PATH + '/reddit-politics/reddit-politics/resources/config.yml', 'r') as file:
    config = yaml.load(file)


class SubmissionReader(object):
    def __init__(self, submission_table_name='submission', timestamp_column_name='created_utc'):
        self.submission_table_name = submission_table_name
        self.timestamp_column_name = timestamp_column_name
        self.default_start_epoch = int(dt.datetime(2016,11,8).replace(tzinfo=dt.timezone.utc).timestamp()) # set default to Nov 8, 2016

    def get(self, subreddit='politics', limit=16):
        """
        """

        db = pymysql.connect(**config['mysql'])
        cursor = db.cursor()

        cursor.execute('SELECT EXISTS (SELECT * FROM {})'.format(self.submission_table_name))
        if cursor.fetchone()[0] == 0:
            after = self.default_start_epoch
        else:
            cursor.execute('SELECT MAX({}) FROM {}'.format(self.timestamp_column_name, self.submission_table_name))
            after = int(cursor.fetchone()[0])
        return self._get(after=after, subreddit=subreddit, limit=limit)

    def _get(self, after, subreddit, limit):
        """
        """

        reddit = praw.Reddit(**config['praw'])
        pushshift = PushshiftAPI(reddit)

        swriter = sw.SubmissionWriter()
        submissions = list(pushshift.search_submissions(after=after, subreddit=subreddit, limit=limit))

        for submission in submissions:
            swriter.push(submission)
            print('Pushing submission {} to writer'.format(submission.id))
        swriter.flush()
        return True
