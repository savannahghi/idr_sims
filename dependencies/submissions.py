import requests
import pandas as pd
import json
import xmltodict
from airflow import models

email = models.Variable.get("odk_email")
password = models.Variable.get("odk_password")
url = models.Variable.get("odk_url")

# Start The Session
payload = {'email': email, 'password': password}
r = requests.post(url, json=payload)
a = json.loads(r.text)
Bearer = a['token']
session = requests.Session()
session.headers.update({'Authorization': f'Bearer {Bearer}'})

def start_session(formID):
    response = session.get(f'https://odk.fahariyajamii.org/v1/projects/3/forms/{formID}/submissions')
    return response

def fullSubmissions(formID):
    # Submission Instances
    def submissionInstances():
        response = start_session(formID)
        a = json.loads(response.text)
        instances = pd.DataFrame(a)
        return instances

    # Retrieving Submissions in xml format
    def get_xml(formID):
        formID = formID
        a = submissionInstances()
        xml = []
        for x in a["instanceId"]:
            r = session.get(f'https://odk.fahariyajamii.org/v1/projects/3/forms/{formID}/submissions/{x}.xml')
            xml.append(r.text)
        return xml
    
    # Converting xml to json format
    def convert_to_json():
        a = get_xml(formID)
        list_of_Dicts = []
        for i in a:
            Dict = xmltodict.parse(i)
            list_of_Dicts.append(Dict)

        submissions_json = json.dumps(list_of_Dicts)
        submissions_json_obj = json.loads(submissions_json)

        submissions_df = pd.json_normalize(submissions_json_obj)
        return submissions_df

    def relevant():
        a = convert_to_json()
        a = a.iloc[:,8:]
        a[['Facility','meta_details']] = a["data.meta.instanceName"].str.split("-",1, expand=True)
        return a
        
    return relevant()
