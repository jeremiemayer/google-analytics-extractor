import copy
import calendar
from datetime import datetime, timedelta, date

# Production SQL server credentials
MSSQL_HOST='localhost'
MSSQL_DB='db'
MSSQL_USER='user'
MSSQL_PASS='pwd'
MSSQL_PORT='1433'
MSSQL_DRIVER='mssql+pyodbc'

# Mapping of custom dimensions so their actual names
CUSTOM_DIMENSIONS = {}

views=['200513957','10490692'] 

# Each request will run with all date ranges
dateRanges=[]

      
""" add these to refresh old data if required
dateRanges.append({'name':'October','season':2021,'range':{'startDate': '2020-10-01', 'endDate':'2020-10-31'}})
dateRanges.append({'name':'All','season':2020,'range':{'startDate': '2019-10-01', 'endDate':'2020-09-30'}})
dateRanges.append({'name':'Plan','season':2020,'range':{'startDate': '2019-11-01', 'endDate':'2020-04-30'}})
dateRanges.append({'name':'Grow','season':2020,'range':{'startDate': '2020-05-01', 'endDate':'2020-08-15'}})
dateRanges.append({'name':'Harvest','season':2020,'range':{'startDate': '2020-08-16', 'endDate':'2020-10-31'}})
dateRanges.append({'name':'October','season':2020,'range':{'startDate': '2019-10-01', 'endDate':'2019-10-31'}})
dateRanges.append({'name':'November','season':2020,'range':{'startDate': '2019-11-01', 'endDate':'2019-11-30'}})
dateRanges.append({'name':'December','season':2020,'range':{'startDate': '2019-12-01', 'endDate':'2019-12-31'}})
dateRanges.append({'name':'January','season':2020,'range':{'startDate': '2020-01-01', 'endDate':'2020-01-31'}})
dateRanges.append({'name':'February','season':2020,'range':{'startDate': '2020-02-01', 'endDate':'2020-02-28'}})
dateRanges.append({'name':'March','season':2020,'range':{'startDate': '2020-03-01', 'endDate':'2020-03-31'}})
dateRanges.append({'name':'April','season':2020,'range':{'startDate': '2020-04-01', 'endDate':'2020-04-30'}})
dateRanges.append({'name':'May','season':2020,'range':{'startDate': '2020-05-01', 'endDate':'2020-05-31'}})
dateRanges.append({'name':'June','season':2020,'range':{'startDate': '2020-06-01', 'endDate':'2020-06-30'}})
dateRanges.append({'name':'July','season':2020,'range':{'startDate': '2020-07-01', 'endDate':'2020-07-31'}})
dateRanges.append({'name':'August','season':2020,'range':{'startDate': '2020-08-01', 'endDate':'2020-08-31'}})
dateRanges.append({'name':'September','season':2020,'range':{'startDate': '2020-09-01', 'endDate':'2020-09-30'}})
"""

# use yesterdays date since we cannot retrieve data from current day
now = datetime.now() - timedelta(days=1) 
day = now.day
yr = now.year
month = now.month

last_day = calendar.monthrange(yr,month)[1]
season = yr+1 if month>=10 else yr
bpSeason = yr+1 if month>=8 else yr

#  create decision cycle record
#  A: Nov 1 - April 30
#  B: May 1 - August 15
#  C: August 16 - October 31
cycle_season = yr+1 if month>=11 else yr
if month>=11 or month<=4:
    cycle_name = 'A'
    cycle_range = {'startDate':"{}-{}-01".format(cycle_season-1,11), 'endDate':"{}-{}-{}".format(cycle_season,'04',30)}
elif ((month>=5 and month<=7) or (month==8 and day<=15)):
    cycle_name = 'B'
    cycle_range = {'startDate':"{}-{}-01".format(yr,'05'), 'endDate':"{}-{}-{}".format(yr,'08',15)}
elif ((month==8 and day>=16) or (month>=9 and month<=10)):
    cycle_name = 'C'
    cycle_range = {'startDate':"{}-{}-16".format(yr,'08'), 'endDate':"{}-{}-{}".format(yr,10,31)}

# Always export the current season, decision cycle and month
# zfill used to zero-pad months as google analytics only access dates with the format 2019-01-01
dateRanges.append({'name':'All','season':season,'range':{'startDate':"{}-{}-01".format(season-1,10), 'endDate':"{}-09-{}".format(season,30)}})
dateRanges.append({'name':cycle_name,'season':cycle_season,'range':cycle_range})
dateRanges.append({'name':date(1900, month, 1).strftime('%B'),'season':season,'range':{'startDate':"{}-{}-01".format(yr,str(month).zfill(2)), 'endDate':"{}-{}-{}".format(yr,str(month).zfill(2),last_day)}})
                          
# request templates
location={'reportRequests':[  
                            {'viewId': '',
                            'dateRanges': {},
                            'metrics': [
                                {'expression': 'ga:users'},
                                {'expression': 'ga:sessions'}],
                            'dimensions': [
                                {'name': 'ga:city'},
                                {'name': 'ga:region'},
                                {'name': 'ga:country'}
                            ],
                            'dimensionFilterClauses':[
                                    {
                                        'operator':'AND',
                                        'filters':[
                                            {
                                                'dimensionName':'ga:hostname',
                                                'not':True,
                                                'operator':'REGEXP',
                                                'expressions':['dev|test|staging|localhost']
                                            },
                                        ]
                                    }
                            ],
                            'pageToken' : None,
                            'pageSize' : 2000}
                            ]} 

audience={'reportRequests':[  
                            {'viewId': '',
                            'dateRanges': {},
                            'metrics': [
                                {'expression': 'ga:users'},
                                {'expression': 'ga:sessions'}],
                            'dimensions': [
                                {'name': 'ga:userGender'},
                                {'name': 'ga:userAgeBracket'}
                            ],
                            'dimensionFilterClauses':[
                                    {
                                        'operator':'AND',
                                        'filters':[
                                            {
                                                'dimensionName':'ga:hostname',
                                                'not':True,
                                                'operator':'REGEXP',
                                                'expressions':['dev|test|staging|localhost']
                                            }
                                        ]
                                    }
                            ],
                            'pageToken' : None,
                            'pageSize' : 2000}
                            ]}

# pair all requests with their matching table
all_requests = [
    {"var":location, "schema":"imports.ga_location"},
    {"var":audience, "schema":"imports.ga_audience"}
]

# Make an array of requests, one for each request + date range
# deepcopy ensures that none of the elements are shareds
requests,schemas,cuts,seasons = [],[],[],[]
total_length = 0

for view in views:
    for req in all_requests:

        # only export faq for viewId=10490692
        if (req['schema'] in ['imports.ga_faq','imports.ga_faq_events'] and view in ['200513957','133824653']):
            continue

        # only export for viewId=200513957 // Farm Post/Content Hub
        if (req['schema'] in ['imports.ga_fp_page_history','imports.ga_fp_location'] and view in ['10490692','133824653']):
            continue

        for i, rng in enumerate(dateRanges):
            requests.append(copy.deepcopy(req['var']))
            requests[i+total_length]['reportRequests'][0]['viewId'] = view # set view id
            requests[i+total_length]['reportRequests'][0]['dateRanges'] = rng['range'] # set date range
            schemas.append(req['schema'])
            cuts.append(rng['name'])
            seasons.append(rng['season'])
        
        total_length = len(requests)       

print("Processing {} GA requests...".format(total_length))

#Combination of requests and schema - ENSURE THEY ARE BOTH ORDERED CONSISTENTLY 
GA_REQUEST_PAYLOAD = zip(requests,schemas,cuts,seasons)

################################################################################################
## Combined variables passed to main.py
################################################################################################
MSSQL_SCHEMA='imports'
MSSQL_CONN_STRING='{}://{}:{}@{}:{}/{}?driver=SQL+Server'.format(MSSQL_DRIVER,MSSQL_USER,MSSQL_PASS,MSSQL_HOST,MSSQL_PORT,MSSQL_DB)
#engine = sa.create_engine('mssql+pyodbc://user:password@server/database')

GA_CONFIG = {
        'SCOPES':['https://www.googleapis.com/auth/analytics.readonly'],
        'KEY_FILE_LOCATION':'api-key.json',
        'REQUEST_PAYLOAD':GA_REQUEST_PAYLOAD
    }