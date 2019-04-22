# import requests
# #
# # cookies = {
# # }
# #
# # data = open('auth')
# # response = requests.post('https://api.appnexus.com/auth', cookies=cookies, data=data)
# # print(response.content)
# #
# # print("#########")
# # # headers = {
# # #     'Authorization': 'authn:220244:aeaaf25a92313:lax1',
# # # }
# # # cookies={
# # #     "token" : "authn:220244:b1ad63dfb43d3:ams1"
# # # }
# #
# # token='authn:220244:aeaaf25a92313:lax1'
# # headers = {'Authorization': 'Bearer ' + token, "Content-Type": "application/json"}
# # data1 = open('report_request')
# # response1 = requests.get('https://api.appnexus.com/report', headers=headers, data=data1)
# # print(response1.content)
# #
# # print(cookies)
# #
# # '''
# # curl -b cookies -c cookies -X POST -d @auth 'https://api.appnexus.com/auth'
# # curl -b cookies -c cookies -X POST -d @network_analysis_report 'https://api.appnexus.com/report'
# #
# # curl -b cookies -c cookies 'https://api.appnexus.com/report-download?id=report_id' > file_name.csv
# #
# # '''

# curl -H "Authorization: authn:220244:aeaaf25a92313:lax1" @report_request 'https://api.appnexus.com/report'


import re
import requests
import json
import pandas as pd
session = requests.Session()

url = 'https://api.appnexus.com/auth'
print(session.cookies.get_dict())

data=open('auth')
response1 = session.post(url,data=data)
print(response1)
print(session.cookies.get_dict())
#print(response1.cookies)

cookies = session.cookies
report = {
    "report": {
        "report_type": "seller_brand_review",
        "start_date": "2019-04-01 00:00:00",
        "end_date" : "2019-04-01 00:00:00",
        "columns": ["month","day","buyer_member_id","placement_id","placement_name","placement","publisher_id","geo_country","geo_country_name","imp_type","imp_type_id","creative_id","site_id","site_name","brand_id","brand_name","size","imps","clicks","cost","revenue","booked_revenue","reseller_revenue","profit","cpm","ctr","rpm","ppm","imps_viewed","view_measured_imps","view_rate","view_measurable_rate"],
        "format": "csv"
    }
}
data_d = open('report_request')
print(data_d)
response3 = requests.get('https://api.appnexus.com/report', cookies=session.cookies.get_dict(),data=report)
# print(response2)
#print(response3.status_code)
print(response3.content)
