import http.client
import json
from io import StringIO
import csv
import pandas as pd
import xlrd
import xlsxwriter
import xlwings as xw
import openpyxl as pxl
import datetime
from ast import literal_eval
import logging

import azure.functions as func

        
        
def Bloomfire(api_key, email):
        d= datetime.datetime.today()
        Mon_offset = (d.weekday() - 0) % 7
        Monday_same_week = d - datetime.timedelta(days=Mon_offset)
        Monday_same_weeks = Monday_same_week.strftime("%Y-%m-%d")
        td = datetime.timedelta(days=7)
        Monday_previous_week = (Monday_same_week - td).strftime("%Y-%m-%d")
        Urlendpoint = "https://reports-api.bloomfire.com/" + "member_engagement/full.csv?" + "date_range=" + Monday_previous_week + "%20to%20" + Monday_same_weeks



   
     

#open http connection to client bloomfire
        conn = http.client.HTTPSConnection("asea-global.bloomfire.com")

# convert payload to json
        payload = json.dumps({
        "api_key": api_key,
        "email": email
        })

#Set headers
        headers =  {
        'Content-Type': 'application/json',
        'Bloomfire-Requested-Fields': 'reports_api_token'
        }
#request connection to api
        conn.request("POST", "/api/v2/login", payload, headers)
#read response
        res = conn.getresponse()
        data = res.read()
#convert from bytes to string
        data = data.decode("utf-8")
        data = json.loads(data)
#close connections that were opened when getting token
        res.close()
        conn.close()

#assign token to bloomfire_token variable
        bloomfire_token = data["reports_api_token"]["token"]

#open another http connection to client bloomfire
        conn1 = http.client.HTTPSConnection("reports-api.bloomfire.com")
        payload1 = ''
        headers1 = {
        'Content-Type': 'application/json',
                'Authorization': 'Bearer' + ' ' + bloomfire_token
            }

#request connection to api
        conn1.request("GET", Urlendpoint, payload1, headers1)

#read response
        res1 = conn1.getresponse()
        data = res1.read()
#conevrt data from bytes to string
        data = data.decode("utf-8")
        res.close()
        conn.close()
#insert the string data into a dataframe
        # print(type(data))
    
        StringData = StringIO(data)
        df = pd.read_csv(StringData, sep =",")
#print(df)
# print(type(df['Date']))
    
        df['Date']= pd.to_datetime(df['Date'])
    # df['Dateint'] = int(df['Date'].strftime("%Y%m%d%H%M%S"))
        print(df["Date"])
    # df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    # df['Dateint'] = df['Date'].apply(lambda x:x.toordinal())
        df['Dateint'] = df['Date'].apply(lambda x: x.value)

        # print(df['Dateint'])
        df['natural_key'] =  str(df['Email']) + str(df['Dateint']) + str(df['Action'])
        df['natural_key'] =  df['Email'].map(str) + df['Dateint'].map(str) + df['Action'].map(str)
    #remove any space
        df['natural_key']= df['natural_key'].str.replace(' ', '')
        df.drop(df.columns[5], axis=1, inplace = True)
    # print(df)
        # print(df.info())
    # print(type(df))
    
    
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    ##Convert the dataframe to json format

        result = df.to_json(orient="records", date_format='iso')
        parsed = json.loads(result)
        result1 =json.dumps(parsed, indent=4)
        return parsed
     
    # print(result1)
#     print(type(result1))
   
            
       
def main(req):
    logging.info('Python HTTP trigger function processed a request.')
    request = req  # Handler Method For Object to Json (used locally )
    # request = req.get.json() #used when deploying the code

    # Set State for API Call
    if 'cursor' in request['state']:
        time = request['state']['cursor']
        print(time)
    else:
        time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        print(time)

    # Get Authentication Details Out of API
    api_key = request['secrets']['api_key']
    email = request['secrets']['email']
    
    bloomfire_result = Bloomfire(api_key, email)
#     print(bloomfire_result)

    response = {
        "schema": {
            "Boomfire": {
                "primary_key": [
                    "natural_key"
                ]
            }
        },
        "state": {
            "cursor": time
        }
    }

#     data = []
#     for page in result:
#         data.append(page)

    response['insert'] = {"Bloomfire": bloomfire_result}

    headers = {"Content-Type": "application/json"}
    with open('Z:\Shared\Public\BI_Test\BloomfireAPIResults.txt', 'w') as outfile:
            json.dump(response, outfile, sort_keys = True, indent = 4,
            ensure_ascii = False)



    #required azure format
    return func.HttpResponse(
        json.dumps(response), 
        status_code=200, 
        headers=headers
    )

req = {
    'state': {},
    'secrets': {
        'api_key': '0d2168dd2079e8ef1e4e63fc13e8b53d801840ca',
        'email': 'ITdev@aseaglobal.com'
    }

}
out = main(req)   
print(out)






