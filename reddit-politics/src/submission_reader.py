import datetime as dt
from datetime import datetime, timedelta, timezone

import pymysql
import requests

from . import config
from . import submission_writer as sw


class SubmissionReader(object):
    def __init__(self, max_size=16, submission_table_name='submission', timestamp_column_name='created_utc'):
        self.submission_table_name = submission_table_name
        self.timestamp_column_name = timestamp_column_name
        self.default_start_epoch = int(dt.datetime(2015,6,26).replace(tzinfo=timezone.utc).timestamp()) # set default to Jun 26, 2015
        self.max_size = max_size

    def get(self, subreddit='politics', limit=16):
        """
        """

        db = pymysql.connect(**config.mysql)
        cursor = db.cursor()

        cursor.execute('SELECT EXISTS (SELECT * FROM {})'.format(self.submission_table_name))
        if cursor.fetchone()[0] == 0:
            after = self.default_start_epoch
        else:
            cursor.execute('SELECT {} FROM {} ORDER BY {} DESC LIMIT 1'.format(self.timestamp_column_name,
                                                                               self.submission_table_name,
                                                                               self.timestamp_column_name))
            after = int(cursor.fetchone()[0])

        before = int((datetime.now()-timedelta(days=14)).replace(tzinfo=timezone.utc).timestamp())

        return self._get(q='', before=before, after=after, subreddit=subreddit, limit=limit)

    def get_all(self, subreddit='politics'):
        """
        """

        while self.get(limit=1000):
            continue

    def _get(self, **kwargs):
        """
        """

        swriter = sw.SubmissionWriter(max_size=self.max_size)
        submissions = self._submission_search(**kwargs)

        removed_posts = set([comment['parent_id'][3:] for comment in self._comment_search(author='PoliticsModeratorBot')])

        for submission in submissions:
            if 'www.reddit.com' in submission['url']:
                print('Not pushing submission {}: no article linked'.format(submission['id']))
            elif 'www.youtube.com' in submission['url']:
                print('Not pushing submission {}: Youtube video'.format(submission['id']))
            elif submission['id'] in removed_posts:
                print('Not pushing submission {}: removed by moderators'.format(submission['id']))
            else:
                swriter.push(submission)
                print('Pushing submission {}'.format(submission['id']))
        swriter.flush()

        return submissions

    def _submission_search(self, **kwargs):
        url_constructor = ['{}={}'.format(arg, kwargs[arg]) for arg in kwargs.keys()]
        url = 'https://api.pushshift.io/reddit/submission/search/?' + '&'.join(url_constructor)
        return requests.get(url).json()['data']

    def _comment_search(self, **kwargs):
        url_constructor = ['{}={}'.format(arg, kwargs[arg]) for arg in kwargs.keys()]
        url = 'https://api.pushshift.io/reddit/search/comment/?' + '&'.join(url_constructor)
        return requests.get(url).json()['data']
