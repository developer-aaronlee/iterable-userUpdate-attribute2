import pandas as pd
import numpy as np
import requests
import json

email_channel_id = "39601"
email_type_id = "47276"
sms_channel_id = "40240"
sms_type_id = "79715"
channel_name = "messageChannel"
type_name = "messageType"

iterable_update = "https://api.iterable.com/api/users/update"
iterable_subscribe = "https://api.iterable.com/api/subscriptions"

api_headers = {
    "Content-Type": "application/json",
    "Api-Key": "f435988be541463fb59da3e9d16d0925"
}

df = pd.read_csv("iterable_backfill_test.csv")
# print(df.values)

nan_values = df.isna()
# print(nan_values)

df.fillna("", inplace=True)
# print(df.values)

for i, r in df.iterrows():
    if r[2] != "":
        df.loc[i, "phone"] = "+" + str(df.loc[i, "phone"]).split(".")[0]

all_data = df.to_numpy()
# print(all_data)


def update_email_sms(email, sms):
    payload = {
        "email": email,
        "dataFields":
            {
                "phoneNumber": sms
            }
    }

    return payload


def update_email(email):
    payload = {
        "email": email
    }

    return payload


def subscription_url(link, kind, kind_id, email):
    url = f"{link}/{kind}/{kind_id}/user/{email}"

    return url


n = 0
for x in all_data:
    n += 1
    if x[2] != "":
        update_user = json.dumps(update_email_sms(x[0], x[2]))
    else:
        update_user = json.dumps(update_email(x[0]))

    # print(f"Row {n}: Update Profile - {update_user}")

    update_response = requests.post(url=iterable_update, data=update_user, headers=api_headers)
    print(f"Row {n}: email: {x[0]} phone: {x[2]} Response: ", update_response.json())

    if x[1].lower() == "subscribed":
        email_channel_url = subscription_url(iterable_subscribe, channel_name, email_channel_id, x[0])
        email_channel_response = requests.patch(url=email_channel_url, headers=api_headers)
        print(f"Row {n}: email channel: {x[1]} Response: ", email_channel_response.json())

        email_type_url = subscription_url(iterable_subscribe, type_name, email_type_id, x[0])
        email_type_response = requests.patch(url=email_type_url, headers=api_headers)
        print(f"Row {n}: email type: {x[1]} Response: ", email_type_response.json())

        # print(f"Row {n}: Email Subscribe - {email_channel_url}; {email_type_url}")

    else:
        email_type_url = subscription_url(iterable_subscribe, type_name, email_type_id, x[0])
        email_unsub_response = requests.delete(url=email_type_url, headers=api_headers)
        print(f"Row {n}: email type: {x[1]} Response: ", email_unsub_response.json())

        # print(f"Row {n}: Email Unsubscribe - {email_type_url}")

    if x[3] != "":
        if x[3].lower() == "subscribed":
            sms_channel_url = subscription_url(iterable_subscribe, channel_name, sms_channel_id, x[0])
            sms_channel_response = requests.patch(url=sms_channel_url, headers=api_headers)
            print(f"Row {n}: sms channel: {x[3]} Response: ", sms_channel_response.json())

            sms_type_url = subscription_url(iterable_subscribe, type_name, sms_type_id, x[0])
            sms_type_response = requests.patch(url=sms_type_url, headers=api_headers)
            print(f"Row {n}: sms type: {x[3]} Response: ", sms_type_response.json())

            # print(f"Row {n}: SMS Subscribe - {sms_channel_url}; {sms_type_url}")

        elif x[3].lower() == "pending":
            sms_channel_url = subscription_url(iterable_subscribe, channel_name, sms_channel_id, x[0])
            sms_channel_response = requests.patch(url=sms_channel_url, headers=api_headers)
            print(f"Row {n}: sms channel: {x[3]} Response: ", sms_channel_response.json())

            # print(f"Row {n}: SMS Pending - {sms_channel_url}")

        else:
            sms_type_url = subscription_url(iterable_subscribe, type_name, sms_type_id, x[0])
            sms_unsub_response = requests.delete(url=sms_type_url, headers=api_headers)
            print(f"Row {n}: sms type: {x[3]} Response: ", sms_unsub_response.json())

            # print(f"Row {n}: SMS Unsubscribe - {sms_type_url}")
    else:
        continue

