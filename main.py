""" This cloud function will receive the real time lead ads update
from facebook. It will also handle the page subscription."""


def fb_webhook(request):

    """" Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
        Returns:
        The response text, or any set of values that can be turned into a
         Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>."""

    from flask import abort, make_response, jsonify
    import os
    # import json
    # import requests
    # from datetime import datetime
    # import base64

    from google.cloud import pubsub_v1
    import logging

    if request.method == 'GET':
        # Part 1: Subscribe a leadgen endpoint to webhook
        # A token that Facebook will echo back to you as part of
        # callback URL verification.
        # Extract a verify token we set in the webhook subscription
        # and a challenge to echo back.
        verify_token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')

        if not verify_token or not challenge:
            return 'Missing hub.verify_token and hub.challenge params'

        if verify_token != os.environ.get('VERIFY_TOKEN'):
            return 'Verify token does not match'
        # We echo the received challenge back to Facebook to finish
        # the verification process.
        return challenge
    elif request.method == 'POST':
        # Part 2: Retrieving realtime leads
        # Facebook will post realtime leads to this endpoint
        # if we've already subscribed to the webhook in part 1.

        # Get value from POST request body
        body = request.get_json()

        print("Data from facebook: ", body)
        page = body.get('entry')

        # We get page, form, and lead IDs from the change here.
        # We need the lead gen ID to get the lead data.
        # The form ID and page ID are optional.
        # To record them into BQ.

        if len(page) != 0:
            try:
                value = page[0]['changes'][0]['value']
                page_id = value['page_id']
                form_id = value['form_id']
                adgroup_id = value['adgroup_id']
                leadgen_id = value['leadgen_id']
                payload_timestamp = value['created_time']

                logging.info('Page ID {}, Form ID {}, Lead ID {}, AdGroup ID {}'.format(
                    str(page_id), str(form_id), str(leadgen_id), str(adgroup_id)
                ))

                publisher = pubsub_v1.PublisherClient()
                topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id='data-services-asia-dev',
                                                                           topic='fblead-data')

                data = '{{"page_id": {}, "form_id": {}, "adgroup_id": {}, "leadgen_id": {}, "payload_timestamp": {}}}'.\
                    format(page_id, form_id, adgroup_id, leadgen_id, payload_timestamp)

                try:
                    future = publisher.publish(topic_name, data=data.encode('utf-8'))
                    logging.info('Message published with id {}'.format(future.result()))
                except Exception as err:
                    print(err)

                # Send HTTP 200 OK status to indicate we've received the update.
                return make_response(jsonify(success=True), 200)
            except Exception as err:
                logging.warning("Following error happened {}. Not sending to topic".format(err))
                return make_response(jsonify(success=True), 200)

    else:
        return abort(405)


# noinspection DuplicatedCode
def fb_process(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    import base64
    import json
    import logging
    import os

    import requests
    from datetime import datetime
    from google.cloud import pubsub_v1

    from config import API_KEY_PROD, CLIENT_ID_PROD, URL_PROD
    from utilities import process_sg_leads
    from utilities import process_id_leads

    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))

    if 'data' in event:
        event_data = base64.b64decode(event['data']).decode('utf-8')
        converted_data = json.loads(event_data)
        logging.info("Received data: {}".format(converted_data))

        page_id = str(converted_data['page_id'])
        form_id = str(converted_data['form_id'])
        adgroup_id = str(converted_data['adgroup_id'])
        leadgen_id = str(converted_data['leadgen_id'])
        payload_time = str(converted_data['payload_timestamp'])

        if page_id == '119882084714527':
            process_sg_leads(form_id, adgroup_id, leadgen_id, payload_time)

        elif page_id == '143907332287044':
            process_id_leads(form_id, adgroup_id, leadgen_id, payload_time)

        else:
            # response_ad_data = ''
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
                access_token = os.environ.get('ACCESS_TOKEN')
          
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

                if response_ad_data.json().get('data') is None:
                    ad_uri_retry = (
                                graph_api_url + '/' + adgroup_id + '/insights' + '?fields=campaign_id%2Ccampaign_name'
                                                                                 '%2Cad_id%2Cad_name%2Cadset_id'
                                                                                 '%2Cadset_name' + '&access_token='
                                + access_token_new)
                    response_ad_data_retry = requests.get(ad_uri_retry)
                    ad_data = response_ad_data_retry.json().get('data')[0]
                else:
                    ad_data = response_ad_data.json().get('data')[0]

                # Final payload
                data = {'date': str(created_time),
                        'market': 'MY',
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

                # Parsing listing_id (Remove the try-except after campaign name convention fixed)
                try:
                    listing_id = int(str(ad_data['campaign_name']).split("_")[-1])
                    print(f"Listing Id => {listing_id}")
                except Exception as err:
                    print(f"Error while parsing lising id => {err}")
                    listing_id = int(str(ad_data['campaign_name']).split("_")[4].split("-")[1])
                    print(f"Listing Id => {listing_id}")

                # Payload for product API
                payload_idev = {
                                 'CountryCode': 'MY',
                                 'DevelopmentID': int(listing_id),
                                 'Name': name,
                                 'Email': email,
                                 'Phone': phone_number,
                                 'EnquiryType': 4
                                }

                headers = {
                            'Content-Type': 'application/json',
                            'Apikey': str(API_KEY_PROD),
                            'ClientId': CLIENT_ID_PROD
                          }

                logging.info(json.dumps(data))

                response_idev = requests.post(URL_PROD, data=json.dumps(payload_idev), headers=headers)
                response_idev.raise_for_status()
                logging.info("Response from API is {}".format(str(response_idev.json())))

            except IndexError as err:
                logging.warning("Following error happened while processing: {}".format(err))

                data_rec = {'date': str(created_time),
                            'market': 'MY',
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
                                 'CountryCode': 'MY',
                                 'DevelopmentID': int(response_form_name.json().get('name').split("_")[-1]),
                                 'Name': name,
                                 'Email': email,
                                 'Phone': phone_number,
                                 'EnquiryType': 4
                                 }

                    headers = {
                        'Content-Type': 'application/json',
                        'Apikey': str(API_KEY_PROD),
                        'ClientId': CLIENT_ID_PROD
                        }

                    retry_results = requests.post(URL_PROD, data=json.dumps(idev_payload), headers=headers)
                    retry_results.raise_for_status()
                    logging.info("Response from API is {}".format(str(retry_results.json())))

                except ValueError as error:
                    logging.warning("Following error happened while processing: {}".format(error))

            except Exception as error:
                logging.warning("Following error happened while processing: {}. "
                                "Sending unprocessed record to queue".format(error))

                publisher_new = pubsub_v1.PublisherClient()
                topic_name_new = 'projects/{project_id}/topics/{topic}'.format(project_id='data-services-asia-dev',
                                                                               topic='fblead_unprocessed')

                data = '{{"form_id": {}, "adgroup_id": {}, "leadgen_id": {}, "payload_timestamp": {}}}'. \
                    format(form_id, adgroup_id, leadgen_id, payload_time)

                future = publisher_new.publish(topic_name_new, data=data.encode('utf-8'))
                logging.info('Message published with id {}'.format(future.result()))
    else:
        logging.info("No data to process for message ID {}.".format(context.event_id))




