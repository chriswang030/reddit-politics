import newspaper as news
import praw
import pymysql as sql
import queue
import submission_writer as sw

reddit = praw.Reddit(client_id='42j3JDwf9A_EIA',
                     client_secret='ZnQoCDBxVwu1vRTmkap_VJ75SRk',
                     user_agent='cwangtest')

submission = reddit.submission(id='agk55o')

writer = sw.SubmissionWriter()
writer.push(submission)
writer._write_to_sql()


#
#
#
# article = news.Article(url='https://thinkprogress.org/the-shutdown-just-cost-trump-his-state-of-the-union-address-41cb69480706/')
# article.download()
# article.parse()
# article.nlp()
# print(article.keywords)
