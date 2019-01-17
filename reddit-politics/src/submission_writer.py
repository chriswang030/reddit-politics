import newspaper
import pymysql
import yaml

with open('/Users/Chris/Documents/workspace-py/reddit-politics/reddit-politics/resources/config.yml', 'r') as file:
    config = yaml.load(file)


class SubmissionWriter:
    def __init__(self, submission_table_name='submission', keywords_table_name='keywords', max_size=16):
        self.submission_queue = []
        self.queue_size = 0
        self.max_size = max_size
        self.submission_table_name = submission_table_name
        self.keywords_table_name = keywords_table_name

    def push(self, submission):
        self.submission_queue.append(submission)
        self.queue_size += 1

        if self.queue_size >= self.max_size:
            self._write_to_sql()
        return True

    def flush(self):
        return self._write_to_sql()

    def _write_to_sql(self):
        # Connect to MySQL database
        db = pymysql.connect(**config['mysql'])
        cursor = db.cursor()

        # Insert submission metadata into submission table
        cursor.execute('SHOW columns FROM ' + self.submission_table_name)
        attributes = tuple(column[0] for column in cursor.fetchall())
        sub_values = str([tuple(getattr(submission, attribute) for attribute in attributes)
                          for submission in self.submission_queue])[1:-1]
        sub_columns = str(attributes).replace("'", '')

        sub_query = 'INSERT IGNORE INTO {} {} VALUES {}'.format(self.submission_table_name, sub_columns, sub_values)
        try:
            cursor.execute(sub_query)
            db.commit()
            print('Executed query: ' + sub_query)
        except:
            print('Error executing query: ' + sub_query)
            db.rollback()
            return False

        # Insert keyword extraction into keywords table
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

        key_query = 'INSERT IGNORE INTO {} {} VALUES {}'.format(
            self.keywords_table_name, key_columns, key_values)
        try:
            cursor.execute(key_query)
            db.commit()
            print('Executed query: ' + key_query)
        except:
            print('Error executing query: ' + key_query)
            db.rollback()
            return False

        # Close database and update queue size
        db.close()
        queue_size = 0
        return True
