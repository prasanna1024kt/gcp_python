import requests

base_url ='http://www.squadup.com/api/v3/'


events_url= 'events'

events_url = "%s%s"%(base_url, events_url)

print(events_url)

events_param_dict = {
                "access_token": access_token,
                "page_number": 1,
                "page_size": 100,
            }

data = requests.get(events_url, params=events_param_dict)

print(data.status_code)

#print(data.content)

ATTENDEES_URL= 'events/%s/attendees'

attendences = "%s%s"%(base_url,ATTENDEES_URL)
print(attendences)
attendees_param_dict = {
                        "access_token": access_token,
                        "page_number": 1,
                        "refund": True,
                        "page_size": 100,
                    }
id='28367'
attendences_link = attendences %id
print(attendences_link)


attendences_data = requests.get(attendences_link, params=attendees_param_dict)
print(attendences_data.status_code)



