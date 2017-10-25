import requests
import pprint
import simplejson as json
import http.client
import pandas as pd
from time import time, mktime
from datetime import datetime, date, timedelta, time
import arrow
from pandas.io import sql
from sqlalchemy import create_engine

data = {'grant_type': 'password',
        'client_id': '[ENTER CLIENT ID HERE]',
        'client_secret': '[ENTER CLIENT SECRET HERE]',
        'username': '[ENTER USERNAME HERE]',
        'password': '[ENTER PASSWORD HERE]'}
result = requests.post('https://api.socialstudio.radian6.com/oauth/token', data=data)

load = json.loads(result.content)
key = load['access_token']

#create a midnight timestamp based on today's date:
midnight = datetime.combine(date.today(), time.min)
#create a time stamp value for yesterday at 00:00:00 hours:
yesterday_midnight = midnight - timedelta(days=1)
#create a time stamp value for yesterday at 23:59:00 hours, before midnight hits:
yesterday_beforemidnight = midnight - timedelta(minutes=1)

#below variable converts 'yesterday_midnight' into unix time
sec_since_epochy = mktime(yesterday_midnight.timetuple()) + yesterday_midnight.microsecond/1000000.0
#below code converts the 'sec_since_epochy' into epoch milliseconds, since it's the only timestamp social studio accepts
startDate = int(sec_since_epochy * 1000)

#below variable converts 'yesterday_beforemidnight' into unix time
sec_since_epochb = mktime(yesterday_beforemidnight.timetuple()) + yesterday_beforemidnight.microsecond/1000000.0
#below code converts the 'sec_since_epochb' into epoch milliseconds, since it's the only timestamp social studio accepts
endDate = int(sec_since_epochb * 1000)

#startDate and endDate are converted into strings wich is the only datatype the query string accepts
startDate = str(startDate)
endDate = str(endDate)

headers = {"access_token": key}
#we integrated the python variable strings startDate and endDate into the http query string below to query data
#from the previous dat in an automated fashion
url = 'https://api.socialstudio.radian6.com/v3/posts?topics=1135449&\
startDate=' + startDate + '&endDate=' + endDate + '&limit=1000'
#as you can see, the http query string below has epoch milliseconds as a startDate and endDate
#url = 'https://api.socialstudio.radian6.com/v3/posts?topics=1135449&startDate=1506927600000&endDate=1507013940000&limit=1000'
resp = requests.get(url, headers=headers)
Json = json.loads(resp.content)

TOPICS, ASSIGNED_USER, ARTICLE_ID, EXTERNAL_ID, HEADLINE, AUTHOR, \
CONTENT, ARTICLE_URL, MEDIA_PROVIDER, REGION, LANGUAGE, POST_STATUS, \
PUBLISH_DATE, HARVESTED_DATE, SENTIMENT, CLASSIFICATION, TAGS = \
    [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []

for data in Json['data']:
    TOPICS.append(data['topics']),
    ASSIGNED_USER.append(data['assignedUser']),
    ARTICLE_ID.append(data['id']),
    EXTERNAL_ID.append(data['externalId'])
    HEADLINE.append(data['title']),
    AUTHOR.append(data['author']['title']),
    CONTENT.append(data['content']),
    ARTICLE_URL.append(data['externalLink']),
    MEDIA_PROVIDER.append(data['mediaProvider']['title']),
    REGION.append(data['source']['region']),
    LANGUAGE.append(data['source']['language']),
    POST_STATUS.append(data['postStatus']),
    PUBLISH_DATE.append(data['publishedDate']),
    HARVESTED_DATE.append(data['harvestDate']),
    SENTIMENT.append(data['sentiment'][0]['value']),
    CLASSIFICATION.append(data['classification']),
    TAGS.append(data['tags'])

df = pd.DataFrame([TOPICS, ASSIGNED_USER, ARTICLE_ID, EXTERNAL_ID, HEADLINE, AUTHOR, CONTENT, ARTICLE_URL,
                   MEDIA_PROVIDER, REGION, LANGUAGE, POST_STATUS, PUBLISH_DATE, HARVESTED_DATE, SENTIMENT,
                   CLASSIFICATION, TAGS]).T

# Below code will produce a timestamp of when the API data was requested
utc = arrow.utcnow()
df['PullTime'] = utc.to('US/Pacific')

# Below renames the columns
df1 = df.rename(columns={0: 'TOPICS', 1: 'ASSIGNED_USER', 2: 'ARTICLE_ID', 3: 'EXTERNAL_ID', 4: 'HEADLINE',
                         5: 'AUTHOR', 6: 'CONTENT', 7: 'ARTICLE_URL', 8: 'MEDIA_PROVIDER', 9: 'REGION',
                         10: 'LANGUAGE', 11: 'POST_STATUS', 12: 'PUBLISH_DATE', 13: 'HARVESTED_DATE',
                         14: 'SENTIMENT', 15: 'CLASSIFICATION', 16: 'TAGS'})

# Below transforms the TAGS feature into a string
df1['TAGS'] = df1['TAGS'].astype(str)

postDynamics = []
for data in Json['data']:
    postDynamics.append(data['postDynamics'])
df2 = pd.DataFrame([postDynamics]).T
df3 = df2.rename(columns={0: 'List'})

# The postDynamics feature was tricky, it had nested keys and values so I looped through the postDynamics key
# and converted each nested list to a pandas.Series object with the label as the index This will result in a data frame
# with the label as the column headers, and then you can concat with the remaining columns of the data frame to get what you need
df4 = pd.concat([
    df3.drop('List', 1),
    df3.List.apply(lambda lst: pd.Series({
        d['label']: d['value'] for d in lst
    }))
], axis=1)

df4 = df4.fillna(0)

# Based on the transformation above, I concatenated df4 data frame with df1
result = pd.concat([df1, df4], axis=1)

# Insert pandas data frame, called 'result' into a mysql database

db = create_engine('mysql+mysqldb://username:password@host:port/database?charset=utf8',
                   echo=False, encoding='utf-8')
cnx = db.connect()
result.to_sql(name='ss_retrieve_posts', con=cnx, if_exists='append', index=False)
cnx.close()
db.dispose()