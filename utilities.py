
# Helper function for SG lead ads processing
import logging
from datetime import datetime
import json
import requests

from google.cloud import pubsub_v1
# from config import API_KEY_STG, CLIENT_ID, URL_STG
from config import API_KEY_SG, CLIENT_ID_SG, URL_PROD_SG
from config import URL_STG_ID, X_CLIENT_TYPE_STG, URL_PROD_ID, X_CLIENT_TYPE_PROD


# noinspection DuplicatedCode
def process_sg_leads(form_id, adgroup_id, leadgen_id, payload_time):

    name = ''
    email = ''
    phone_number = ''
    response_form_name = ''
    created_time = ''
    payload_timestamp = ''

    try:
        payload_timestamp = datetime.fromtimestamp(int(str(payload_time))).strftime('%Y-%m-%d %H:%M:%S')

        # Graph API endpoint
        api_version = 'v5.0'
        graph_api_url = 'https://graph.facebook.com/' + api_version

        # System user access token file path.
        # Note: System user needs to be an admin of the subscribed page.
        # access_token = os.environ.get('ACCESS_TOKEN')
        access_token = "EAAJqp82nCOQBAHTHRspqZAZAkODnMyhFS6WmNPdepQtKfQlyRrQD0lBZC1RTkjCkRqlC2Un5JrDZAebBCjAKGROI5GhWzZCpAYQBrbklBN2u4XUogCv8hLxHMmDhhxTUN00usLDhABzg8xPye96X7ZBasSlt7VwmUgEVuaK4BXUbNWbP2GjlKdiFqdRDig5wNQJBA7P9ZAgfAZDZD"

        # Call graph API to request lead info with the lead ID
        # and access token.
        leadgen_uri = (graph_api_url + '/' + leadgen_id +
                       '?access_token=' + access_token)
        response = json.loads(requests.get(leadgen_uri).text)

        # created_time = str(response.get('created_time')).split('T')[0]
        created_time = str(response.get('created_time'))
        field_data = response.get('field_data')

        # Processing lead response (parsing, getting ad & campaign data and insert data to BQ etc.)

        for field in field_data:
            field_name = field.get('name')
            field_value = field.get('values')[0]
            if field_name == 'full_name':
                name = str(field_value)
            elif field_name == 'email':
                email = str(field_value)
            elif field_name == 'phone_number':
                phone_number = str(field_value)

        if len(name) > 200:
            name = name[0:200]

        # To call the form_id edges to get the form name
        form_url = graph_api_url + '/' + form_id + '?access_token=' + access_token

        response_form_name = requests.get(form_url)

        ad_uri = (graph_api_url + '/' + adgroup_id + '/insights' + '?fields=campaign_id%2Ccampaign_name'
                                                                   '%2Cad_id%2Cad_name%2Cadset_id'
                                                                   '%2Cadset_name' + '&access_token='
                  + access_token)

        response_ad_data = requests.get(ad_uri)

        ad_data = response_ad_data.json().get('data')[0]

        # Final payload
        data = {'date': str(created_time),
                'market': 'SG',
                'payload_date': str(payload_timestamp),
                'form_name': str(response_form_name.json().get('name')),
                'name': name,
                'email': email,
                'phone_number': phone_number,
                'campaign_id': str(ad_data['campaign_id']),
                'campaign_name': str(ad_data['campaign_name']),
                'ad_id': str(ad_data['ad_id']),
                'ad_name': str(ad_data['ad_name']),
                'adset_id': str(ad_data['adset_id']),
                'adset_name': str(ad_data['adset_name'])}

        # Payload for iDeveloper API
        payload_idev = {
            'CountryCode': 'SG',
            'DevelopmentID': int(str(ad_data['campaign_name']).split("_")[-1]),
            'Name': name,
            'Email': email,
            'Phone': phone_number,
            'EnquiryType': 4
        }

        headers = {
            'Content-Type': 'application/json',
            'Apikey': str(API_KEY_SG),
            'ClientId': CLIENT_ID_SG
        }

        logging.info(json.dumps(data))

        response_idev = requests.post(URL_PROD_SG, data=json.dumps(payload_idev), headers=headers)
        response_idev.raise_for_status()
        logging.info("Response from API is {}".format(str(response_idev.json())))

    except IndexError as err:
        logging.warning("Following error happened while processing: {}".format(err))

        data_rec = {'date': str(created_time),
                    'market': 'SG',
                    'payload_date': str(payload_timestamp),
                    'form_name': str(response_form_name.json().get('name')),
                    'name': name,
                    'email': email,
                    'phone_number': phone_number,
                    'campaign_id': '',
                    'campaign_name': '',
                    'ad_id': '',
                    'ad_name': '',
                    'adset_id': '',
                    'adset_name': '',
                    'leadgen_id': str(leadgen_id),
                    'form_id': str(form_id),
                    'adgroup_id': str(adgroup_id)
                    }

        logging.info(json.dumps(data_rec))

        try:
            idev_payload = {
                'CountryCode': 'SG',
                'DevelopmentID': int(response_form_name.json().get('name').split("_")[-1]),
                'Name': name,
                'Email': email,
                'Phone': phone_number,
                'EnquiryType': 4
            }

            headers = {
                'Content-Type': 'application/json',
                'Apikey': str(API_KEY_SG),
                'ClientId': CLIENT_ID_SG
            }

            retry_results = requests.post(URL_PROD_SG, data=json.dumps(idev_payload), headers=headers)
            retry_results.raise_for_status()
            logging.info("Response from API is {}".format(str(retry_results.json())))

        except ValueError as error:
            logging.warning("Following error happened while processing: {}".format(error))

    except Exception as error:
        logging.warning("Following error happened while processing: {}. "
                        "Sending unprocessed record to queue".format(error))

        publisher_new = pubsub_v1.PublisherClient()
        topic_name_new = 'projects/{project_id}/topics/{topic}'.format(project_id='data-services-asia-dev',
                                                                       topic='fblead_unprocessed_sg')

        data = '{{"form_id": {}, "adgroup_id": {}, "leadgen_id": {}, "payload_timestamp": {}}}'. \
            format(form_id, adgroup_id, leadgen_id, payload_time)

        future = publisher_new.publish(topic_name_new, data=data.encode('utf-8'))
        logging.info('Message published with id {}'.format(future.result()))


def process_id_leads(form_id, adgroup_id, leadgen_id, payload_time):

    name = ''
    email = ''
    phone_number = ''
    response_form_name = ''
    created_time = ''
    payload_timestamp = ''
    final_meta_data = ''

    try:
        payload_timestamp = datetime.fromtimestamp(int(str(payload_time))).strftime('%Y-%m-%d %H:%M:%S')

        # Graph API endpoint
        api_version = 'v5.0'
        graph_api_url = 'https://graph.facebook.com/' + api_version

        # System user access token file path.
        # Note: System user needs to be an admin of the subscribed page.
        # access_token = os.environ.get('ACCESS_TOKEN')
        access_token = "EAAIPdExqRd8BAJ7BPgWSTQm3wNSobXq3Dc8gRgZAAaV5SaqzSOuQPWYUuW8e8J0NxRCEMyk9LVPvIqXMq7IGoLFbAiLAl3BiluDO7OEYMlAnZCHOSHcX0Nm290OVUMv5A0NmcSSad3melyA9J9k7IfTTxEGkZBqdHXVToFZC3Bw07vZAflsw0ZCqIKoqraUfmqASitsBqeugZDZD"

        # Call graph API to request lead info with the lead ID
        # and access token.
        leadgen_uri = (graph_api_url + '/' + leadgen_id +
                       '?access_token=' + access_token)
        response = json.loads(requests.get(leadgen_uri).text)

        # created_time = str(response.get('created_time')).split('T')[0]
        created_time = str(response.get('created_time'))
        field_data = response.get('field_data')

        # Processing lead response (parsing, getting ad & campaign data and insert data to BQ etc.)
        meta_data = []

        for field in field_data:
            field_name = field.get('name')
            field_value = field.get('values')[0]
            if field_name == 'full_name':
                name = str(field_value)
            elif field_name == 'email':
                email = str(field_value)
            elif field_name == 'phone_number':
                phone_number = str(field_value)
            else:
                meta_data.append(field_name + ':' + field_value)

        final_meta_data = '; '.join(meta_data)

        if len(name) > 200:
            name = name[0:200]

        # To call the form_id edges to get the form name
        form_url = graph_api_url + '/' + form_id + '?access_token=' + access_token

        response_form_name = requests.get(form_url)

        ad_uri = (graph_api_url + '/' + adgroup_id + '/insights' + '?fields=campaign_id%2Ccampaign_name'
                                                                   '%2Cad_id%2Cad_name%2Cadset_id'
                                                                   '%2Cadset_name' + '&access_token='
                  + access_token)

        response_ad_data = requests.get(ad_uri)

        ad_data = response_ad_data.json().get('data')[0]

        # Final payload
        data = {'date': str(created_time),
                'market': 'ID',
                'payload_date': str(payload_timestamp),
                'form_name': str(response_form_name.json().get('name')),
                'name': name,
                'email': email,
                'phone_number': phone_number,
                'campaign_id': str(ad_data['campaign_id']),
                'campaign_name': str(ad_data['campaign_name']),
                'ad_id': str(ad_data['ad_id']),
                'ad_name': str(ad_data['ad_name']),
                'adset_id': str(ad_data['adset_id']),
                'adset_name': str(ad_data['adset_name']),
                'meta_data': final_meta_data
                }

        # Payload for API
        payload_final = {
                        "grab_email": str(email),
                        "grab_name": str(name),
                        "grab_phone": str(phone_number),
                        "grab_message": 'Hai,\nSaya tertarik dengan informasi lebih lanjut tentang project ini. '
                                        'Tolong hubungi saya.\nTerima kasih.',
                        "ads_id": [str(ad_data['campaign_name'].split("_")[-1])],
                        "page_label": "fb_lead",
                        "subscribe": 0
                        }

        headers = {'X-Client-Type': X_CLIENT_TYPE_PROD,
                   'Content-Type': 'application/json'
                   }

        logging.info(json.dumps(data))

        response_api = requests.post(URL_PROD_ID, data=json.dumps(payload_final), headers=headers)
        response_api.raise_for_status()
        logging.info("Response from API is : {}".format(str(response_api.content, 'utf-8')))

    except IndexError as err:
        logging.warning("Following error happened while processing: {}".format(err))

        data_rec = {'date': str(created_time),
                    'market': 'ID',
                    'payload_date': str(payload_timestamp),
                    'form_name': str(response_form_name.json().get('name')),
                    'name': name,
                    'email': email,
                    'phone_number': phone_number,
                    'campaign_id': '',
                    'campaign_name': '',
                    'ad_id': '',
                    'ad_name': '',
                    'adset_id': '',
                    'adset_name': '',
                    'leadgen_id': str(leadgen_id),
                    'form_id': str(form_id),
                    'adgroup_id': str(adgroup_id)
                    }

        logging.info(json.dumps(data_rec))

        try:
            payload_retry = {
                            "grab_email": str(email),
                            "grab_name": str(name),
                            "grab_phone": str(phone_number),
                            "grab_message": 'Hai,\nSaya tertarik dengan informasi lebih lanjut tentang project ini. '
                                            'Tolong hubungi saya.\nTerima kasih.',
                            "ads_id": [str(response_form_name.json().get('name').split("_")[-1])],
                            "page_label": "fb_lead",
                            "subscribe": 0
                            }

            headers_retry = {
                             'X-Client-Type': X_CLIENT_TYPE_PROD,
                             'Content-Type': 'application/json'
                            }

            response_api_retry = requests.post(URL_PROD_ID, data=json.dumps(payload_retry), headers=headers_retry)
            response_api_retry.raise_for_status()
            logging.info("Response from API is : {}".format(str(response_api_retry.content, 'utf-8')))

        except ValueError as error:
            logging.warning("Following error happened while processing: {}".format(error))

    except Exception as error:
        logging.warning("Following error happened while processing: {}. "
                        "Sending unprocessed record to queue".format(error))

        publisher_new = pubsub_v1.PublisherClient()
        topic_name_new = 'projects/{project_id}/topics/{topic}'.format(project_id='data-services-asia-dev',
                                                                       topic='fblead_unprocessed_ID')

        data = '{{"form_id": {}, "adgroup_id": {}, "leadgen_id": {}, "payload_timestamp": {}}}'. \
            format(form_id, adgroup_id, leadgen_id, payload_time)

        future = publisher_new.publish(topic_name_new, data=data.encode('utf-8'))
        logging.info('Message published with id {}'.format(future.result()))
