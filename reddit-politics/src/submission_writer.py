import pymysql

from . import config


class SubmissionWriter(object):
    def __init__(self, submission_table_name=config.submission_table_name, max_size=16):
        self.submission_queue = []
        self.queue_size = 0
        self.max_size = max_size
        self.submission_table_name = submission_table_name

    def push(self, submission):
        self.submission_queue.append(submission)
        self.queue_size += 1

        if self.queue_size >= self.max_size:
            self._write_to_sql()
        return True

    def flush(self):
        """ Flushes submission_queue by writing all to SQL. TODO: thread-safe flush. """

        if self.submission_queue:
            self._write_to_sql()

    def _write_to_sql(self):
        """ Writes all submissions in submission_queue to appropriate table in SQL database. """

        db = pymysql.connect(**config.mysql)
        cursor = db.cursor()

        get_columns_query = 'SHOW columns FROM {}'
        insert_query = 'INSERT IGNORE INTO {} {} VALUES {}'

        cursor.execute(get_columns_query.format(self.submission_table_name))
        attributes = tuple(column[0] for column in cursor.fetchall())
        values = str([tuple(submission[attribute] for attribute in attributes) for submission in self.submission_queue])[1:-1]
        columns = str(attributes).replace("'", '')

        query = insert_query.format(self.submission_table_name, columns, values)
        cursor.execute(query)
        db.commit()
        print('Executed query: {}'.format(query))

        db.close()
        queue_size = 0
        self.submission_queue.clear()
