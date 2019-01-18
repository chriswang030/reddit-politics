import newspaper
import pymysql

import config

# Config article parsing
nconfig = newspaper.Config()
nconfig.MIN_WORD_COUNT = 150
nconfig.MAX_KEYWORDS = 35
nconfig.memoize_articles = False
nconfig.fetch_images = False


class SubmissionWriter(object):
    def __init__(self, submission_table_name='submission', keywords_table_name='keywords', max_size=16):
        self.submission_queue = []
        self.queue_size = 0
        self.max_size = max_size
        self.submission_table_name = submission_table_name
        self.keywords_table_name = keywords_table_name

    def push(self, submission):
        """ Pushes a Submission instance onto submission_queue. Unimplemented: thread-safe push.

        Args
            submission (praw.Submission): submission to be written onto SQL database

        Returns
            bool: True
        """

        self.submission_queue.append(submission)
        self.queue_size += 1

        if self.queue_size >= self.max_size:
            self._write_to_sql()
        return True

    def flush(self):
        """ Calls _write_to_sql. Unimplemented: thread-safe flush.

        Returns
            bool: True if _write_to_sql is successfully called. False if otherwise.
        """

        return self._write_to_sql()

    def _write_to_sql(self):
        """ Writes all submissions in submission_queue to appropriate table in SQL database. Column names
            in database must match attributes of praw.Submission instances.

        Returns
            success (bool): True if all data is successfully written to database. False if any data could not
            be written.
        """

        success = True

        db = pymysql.connect(**config.mysql)
        cursor = db.cursor()

        get_columns_query = 'SHOW columns FROM {}'
        insert_query = 'INSERT IGNORE INTO {} {} VALUES {}'

        cursor.execute(get_columns_query.format(self.submission_table_name))
        attributes = tuple(column[0] for column in cursor.fetchall())
        values = str([tuple(getattr(submission, attribute) for attribute in attributes) for submission in self.submission_queue])[1:-1]
        columns = str(attributes).replace("'", '')

        query = insert_query.format(self.submission_table_name, columns, values)
        try:
            cursor.execute(query)
            db.commit()
            print('Executed query: ' + query)
        except:
            print('Error executing query: ' + query)
            db.rollback()
            success = False

        db.close()
        queue_size = 0
        self.submission_queue.clear()
        return success
