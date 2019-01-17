import newspaper
import pymysql
import yaml

PATH = '/Users/Chris/Documents/workspace-py' # path to working directory

with open(PATH + '/reddit-politics/reddit-politics/resources/config.yml', 'r') as file:
    config = yaml.load(file)


class SubmissionWriter:
    def __init__(self, submission_table_name='submission', keywords_table_name='keywords', max_size=16):
        self.submission_queue = []
        self.queue_size = 0
        self.max_size = max_size
        self.submission_table_name = submission_table_name
        self.keywords_table_name = keywords_table_name

    def push(self, submission):
        """ Pushes a Submission instance onto submission_queue. Unimplemented: thread-safe push.

        Args
            submission: praw.Submission instance

        Ret
            True
        """

        self.submission_queue.append(submission)
        self.queue_size += 1

        if self.queue_size >= self.max_size:
            self._write_to_sql()
        return True

    def flush(self):
        """ Calls _write_to_sql. Unimplemented: thread-safe flush.

        Ret
            True if _write_to_sql is successfully called. False if otherwise.
        """

        return self._write_to_sql()

    def _write_to_sql(self):
        """ Writes all submissions in submission_queue to appropriate table in SQL database. Column names
            in database must match attributes of praw.Submission instances.

        Ret
            success: True if all data is successfully written to database. False if any data could not
            be written.
        """

        success = True

        db = pymysql.connect(**config['mysql'])
        cursor = db.cursor()

        insert_query = 'INSERT IGNORE INTO {} {} VALUES {}'

        cursor.execute('SHOW columns FROM ' + self.submission_table_name)
        attributes = tuple(column[0] for column in cursor.fetchall())
        sub_values = str([tuple(getattr(submission, attribute) for attribute in attributes)
                          for submission in self.submission_queue])[1:-1]
        sub_columns = str(attributes).replace("'", '')

        sub_query = insert_query.format(self.submission_table_name, sub_columns, sub_values)
        try:
            cursor.execute(sub_query)
            db.commit()
            print('Executed query: ' + sub_query)
        except:
            print('Error executing query: ' + sub_query)
            db.rollback()
            success = False

        cursor.execute('SHOW columns FROM ' + self.keywords_table_name)
        key_columns = str(tuple(column[0] for column in cursor.fetchall())).replace("'", '')

        key_values_list = []
        for submission in self.submission_queue:
            article = newspaper.Article(url=submission.url)
            article.download()
            article.parse()
            article.nlp()
            for keyword in article.keywords:
                key_values_list.append((submission.id, keyword))
        key_values = str(key_values_list)[1:-1]

        key_query = insert_query.format(self.keywords_table_name, key_columns, key_values)
        try:
            cursor.execute(key_query)
            db.commit()
            print('Executed query: ' + key_query)
        except:
            print('Error executing query: ' + key_query)
            db.rollback()
            success = False

        db.close()
        queue_size = 0
        self.submission_queue.clear()
        return success
