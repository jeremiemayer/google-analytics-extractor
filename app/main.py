from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
#from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client.client import flow_from_clientsecrets, Credentials
from oauth2client import tools
from sqlalchemy import create_engine, MetaData, Table
from datetime import datetime
from config import MSSQL_CONN_STRING, MSSQL_SCHEMA, GA_CONFIG, CUSTOM_DIMENSIONS
import urllib
import pyodbc
import time
import random
import socket

socket.setdefaulttimeout(600)

SCRIPT_RUN_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Script start: {}".format(SCRIPT_RUN_TIME))

def get_authenticated_service(keyfile_location,scopes):
    # Returns an authenticated analytics reporting service object
    credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile_location,scopes)
    ga_api = build('analyticsreporting', 'v4', credentials=credentials)
    return ga_api

def makeRequest(analytics,request):
    return analytics.reports().batchGet(body=request).execute()['reports'][0]

def makeRequestWithExponentialBackoff(analytics,request):
    """Wrapper to request Google Analytics data with exponential backoff.

    The makeRequest method accepts the analytics service object, makes API
    requests and returns the response. If any error occurs, the makeRequest
    method is retried using exponential backoff.

    Args:
        analytics: The analytics service object
        request: Our request JSON object, from config.py

    Returns:
        The API response from the makeRequest method.
    """
    for n in range(0, 5):
        try:
            return makeRequest(analytics,request)

        except HttpError as error:
            if error.resp.reason in ['userRateLimitExceeded','quoteExceeded','internalServerError','backendError']:
                time.sleep((2 ** n) + random.random())
            else:
                break

    print("There has been an error, the request never succeeded.")

def get_ga_metrics(ga_api,request_payload):   
    # Paginates and returns a GA report page with the dimensions and metrics specified in config.py
    # Taken almost verbatim from the Google Analytics API page
    for request,schema,cut,season in request_payload:
        records = []
        viewId = request['reportRequests'][0]['viewId']
        # time.sleep(1) # google has a limit of 10 requests per second, 100 requests per 100 seconds
        response = makeRequestWithExponentialBackoff(ga_api,request)
        if response is not None:
            if 'rows' in response['data']:
                report_page = response['data']['rows']
                #paginate request
                while "nextPageToken" in response:
                    request['reportRequests'][0]['pageToken'] = response['nextPageToken']
                    response = ga_api.reports().batchGet(body=request).execute()['reports'][0]
                    report_page.extend(response['data']['rows'])
                # Extract headers from API response
                column_header = response.get('columnHeader', {})
                dimension_headers = column_header.get('dimensions', [])
                metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])
                # Transform the headers and data into records format
                for row in report_page:
                    record = {}
                    dimensions = row.get('dimensions', [])
                    dateRangeValues = row.get('metrics', [])
                    for header, dimension in zip(dimension_headers, dimensions):
                        record[header] = dimension
                    for i, values in enumerate(dateRangeValues):
                        for metric_header, value in zip(metric_headers, values.get('values')):
                            record[metric_header.get('name')] = value
                    # Custom fields
                    record['cut']=cut
                    record['viewId']=viewId
                    record['season']=season
                    records.append(record)
                #response_payload.append((schema,records,cut,viewId,season))
                export_ga_metrics(MSSQL_CONN_STRING,MSSQL_SCHEMA,[(schema,records,cut,viewId,season)])
    print("[{}]: Returned response from Google Analytics".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    #return response_payload
    return

# Given a column type and an entry, adds quotes and does some validation:
# For dates, change Google's string result to a valid DATE entry
def validate(type, data):
    # TODO account for other forms of data?
    if type == 'DATE':
        return "'{}-{}-{}'".format(data[:4], data[4:6], data[6:8])
    else:
        return "'{}'".format(data)

def export_ga_metrics(conn_string,schema,response_payload):
    # Establish connection 
    #engine = create_engine(conn_string)
    engine = create_engine(
    'mssql+pyodbc:///?odbc_connect=%s?charset=utf8' % (
        urllib.parse.quote_plus(
            'DRIVER={FreeTDS};SERVER=localhost;'
            'DATABASE=db;UID=user;PWD=pwd;port=1433;'
            'TDS_Version=4.2;')),encoding="utf-8")

    meta = MetaData()
    meta.reflect(bind=engine,schema=schema)
    conn = engine.connect()

    table = ''
    # Resfresh results for each import resquest
    for schema, response, cut, viewId, season in response_payload:
        table = meta.tables[schema]

        #if table != prev_table:
        conn.execute("DELETE FROM {} WHERE Cut = '{}' and viewId='{}' and season={}".format(table,cut,viewId,season))

        idx = 0 # Should not change (always start from the 0th entry)
        batch_size = 1000 # SQL Server does not allow inserts of >1000 rows
        log_output_frequency = 2 # Number of batches before we should print a message
        total_to_insert = len(response)

        print("[{}]: Preparing to insert {} entries"
              .format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total_to_insert))

        while idx < len(response):
            query = 'INSERT INTO {table_name} ({column_names}) VALUES {all_values}'
            # Sort to impose an ordering
            keys_sorted = sorted(response[0].keys())
            # For each row, construct the VALUES entry
            values = []
            for row in response[idx:idx + batch_size]:  # Only process the current batch
                value_params = []
                for response_key in keys_sorted:
                    # Replace the key with its mapped one (only for custom dimensions)
                    table_key = CUSTOM_DIMENSIONS.get(response_key) if response_key in CUSTOM_DIMENSIONS else response_key
                    column_type = str(table.c[table_key].type)
                    column_data = str(row.get(response_key)).replace("'", "''")
                    entry = validate(column_type, column_data)
                    value_params.append(entry)
                values.append('({})'.format(','.join(value_params)))
            all_values = ','.join(values)
            # Construct column names off the mapped keys
            keys_mapped = [CUSTOM_DIMENSIONS.get(response_key) if response_key in CUSTOM_DIMENSIONS else response_key
                           for response_key in keys_sorted]
            column_names = ','.join(['[{}]'.format(col) for col in keys_mapped])
            # Insert constructed strings into the query and execute
            query = query.format(table_name = str(table), column_names = column_names, all_values = all_values)
            engine.execute(query)
            idx = idx + len(response[idx:idx + batch_size])
            # TODO this update message doesn't actually work that well... low priority
            if (idx/batch_size) % log_output_frequency == 0:
                print("[{}]: {} batches inserted ({}/{})"
                      # Technically we should query to see how many were inserted, but performance will take a hit
                      .format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), idx/batch_size, idx, total_to_insert))
        #results = conn.execute(table.select()).fetchall()
        #print("[{}]: Exported {} records to {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),table,len(results)))

#   if __name__ == "__main__":
#Connect to the Google Analytics v4 API
ga_api = get_authenticated_service(GA_CONFIG['KEY_FILE_LOCATION'],GA_CONFIG['SCOPES'])
# Return and parse the GA metrics defined in the config table into accessible records
get_ga_metrics(ga_api,GA_CONFIG['REQUEST_PAYLOAD'])
# Export the GA data in the pre-existing [google_analytics_metrics] table on the production MSSQL server
#export_ga_metrics(MSSQL_CONN_STRING,MSSQL_SCHEMA,response_payload)