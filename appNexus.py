
# # '''
# # curl -b cookies -c cookies -X POST -d @auth 'https://api.appnexus.com/auth'
# # curl -b cookies -c cookies -X POST -d @network_analysis_report 'https://api.appnexus.com/report'
# #
# # curl -b cookies -c cookies 'https://api.appnexus.com/report-download?id=report_id'
# #
# # '''

# curl -H "Authorization: authn:220244:aeaaf25a92313:lax1" @report_request 'https://api.appnexus.com/report'



# import requests
# import json
#
# session = requests.Session()
#
# url = 'https://api.appnexus.com/auth'
# data=open('auth')
# response1 = session.post(url,data=data)
# print(response1)
#
# cookies = session.cookies
# data_d = open('report_request')
# response3 = requests.post('https://api.appnexus.com/report', cookies=cookies,data=data_d)
#
# json_data = json.loads(response3.content)
# # cols=['id','created_on']
# print(json_data)
#
#
# report_id = json_data['response']['report_id']
# print(report_id)
#
# params = (
#     ('id', report_id),
# )
# #print(data_extraction.report_id)
#
# print(params)
# extract_data = requests.get('https://api.appnexus.com/report-download',params=params, cookies=session.cookies.get_dict())
#
# #requests.Session.close()
#
# with open("network_data_extarct_uat.csv","wb") as out:
#
#     out.write(extract_data.content)
#
#

from datetime import  date,timedelta
import requests
import json
import airflow

class AppNexusAPIOperator():

    def __init__(self,
                 baseUrl,
                 authFile,
                 idURL,
                 downloadURL,
                 outputCsvFile,
                 report_name,
                 columns,
                 start_date=None,
                 end_date=None):
        self.baseUrl = baseUrl
        self.authFile = authFile
        self.idURL = idURL
        self.downloadURL = downloadURL
        self.outputCsvFile = outputCsvFile
        self.report_name= report_name
        self.columns = columns
        self.start_date=start_date
        self.end_date=end_date

    def execute(self):

        auth = open(self.authFile)
        session = requests.Session()
        response = session.post(self.baseUrl, data=auth)
        print(response)
        cookies = session.cookies
        print("##############")
        print(cookies)
        reports = {
                     "report":
                       {
                         "report_type":self.report_name,
                         "columns":self.columns,
                         "start_date": self.start_date,
                          "end_date" : self.end_date,
                           "format":"csv"
                       }
                  }
        reports_1 = {
            "report":
                {
                    "report_type": self.report_name,
                    "columns": self.columns,
                    "report_interval" : "yesterday",
                    "format": "csv"
                }
        }
        print(reports)
        response_id1 = requests.post(self.idURL, cookies=cookies, data=json.dumps(reports))
        json_data = json.loads(response_id1.content)
        report_id = json_data['response']['report_id']
        print (report_id)
        params = (
             ('id', report_id),
        )
        extract_data = requests.get(self.downloadURL, params=params,
                                    cookies=session.cookies.get_dict())
        print(extract_data.content)
        with open(self.outputCsvFile,"wb") as output_file:
            output_file.write(extract_data.content)


url = 'https://api.appnexus.com/auth'
idURL='https://api.appnexus.com/report'
downloadURL='https://api.appnexus.com/report-download'
authfile = './auth'
# td = date.today()
# yes = td-timedelta(days=1)
# start_date=yes
# end_date=td
#print(type(start_date))
outputCsvFile='./appnexus_report_20190508.csv'
report='network_analytics'
columns = ["adjustment_day","adjustment_hour","buyer_member_id","buyer_member_name","buyer_type","buying_currency","adjustment_id","advertiser_code","advertiser_currency","advertiser_id","advertiser_name","advertiser_type","bid_type","brand_id","brand_name","campaign_code","campaign_name","campaign_priority","creative_code","creative_id","creative_name","day","inventory_class","imps_filtered_reason","imps_filtered_reason_id","imp_type","imp_type_id", "deal_code","deal_id","deal_name","entity_member_id","geo_country","geo_country_name","marketplace_clearing_event","marketplace_clearing_event_id","marketplace_clearing_event_name","mediatype_id","media_type","month","payment_type","pixel_id","placement_code","placement_id","placement_name","publisher_code","publisher_currency","publisher_id","publisher_name","pub_rule_code","pub_rule_name","pub_rule_id","revenue_type","revenue_type_id", "seller_member_id","seller_member_name","seller_type","selling_currency","site_code","site_id","site_name","size","supply_type","imps","imp_requests","imps_blank","imps_psa","imps_psa_error","imps_default_error","imps_default_bidder","imps_kept","imps_resold","imps_rtb","external_impression","clicks","click_thru_pct","external_click","cost","cost_including_fees","revenue","revenue_including_fees","booked_revenue","booked_revenue_adv_curr","reseller_revenue","profit","profit_including_fees","commissions","cpm","cpm_including_fees","post_click_convs","post_click_revenue","post_view_convs","post_view_revenue","total_convs","convs_per_mm","convs_rate","ctr","rpm","rpm_including_fees","total_network_rpm","total_publisher_rpm","sold_network_rpm","sold_publisher_rpm","media_cost_pub_curr","ppm","ppm_including_fees","serving_fees","imps_viewed","view_measured_imps","view_rate","view_measurement_rate","cpvm","imps_master_creative","data_costs","imps_filtered","avg_bid_reduction","revenue_buying_currency","revenue_selling_currency","booked_revenue_buying_currency","booked_revenue_selling_currency","reseller_revenue_buying_currency","reseller_revenue_selling_currency","cost_buying_currency","cost_selling_currency","profit_buying_currency","profit_selling_currency","total_network_rpm_buying_currency","total_network_rpm_selling_currency","cpm_buying_currency","cpm_selling_currency","rpm_buying_currency","rpm_selling_currency","ppm_buying_currency","ppm_selling_currency","sold_network_rpm_buying_currency","sold_network_rpm_selling_currency","commissions_buying_currency","commissions_selling_currency","serving_fees_buying_currency","serving_fees_selling_currency","data_costs_buying_currency","data_costs_selling_currency","marketplace_clearing_event_units","marketplace_clearing_events_booked_revenue_cpm","marketplace_clearing_event_buyer_media_cost_ecpm"]
start_date ='2019-05-08' + " "+ "00:00:00"
end_date ='2019-05-09' + " " + "00:00:00"
print(start_date,end_date)

ptr = AppNexusAPIOperator(url,authfile,idURL,downloadURL,outputCsvFile,report,columns,start_date,end_date)
ptr.execute()












