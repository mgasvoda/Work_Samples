"""Pulls information tables from Call Tracking Metrics service."""


import MySqlInsert
import requests
from base64 import standard_b64encode

access_key = 'CTM Access_Key'
secret_key = 'CTM Secret_Key'
account_id = 0000  # add account ID

basic_auth = '{}:{}'.format(access_key, secret_key)

user = 'db_user'
password = 'db_pass'
host = 'db_IP'
database = 'db_name'

mysql = MySqlInsert.sql_monster(user, password, host, database)
auth_endcoded = standard_b64encode(basic_auth)
headers = {
    'authorization': 'Basic {}'.format(auth_endcoded),
    'content-type': "application/json"
}


def get_call_list(page):
    """Retrieve desired information from Call Tracking Metrics."""
    url = "https://api.calltrackingmetrics.com/api/v1/accounts/{}/calls"\
        .format(account_id)
    querystring = {"page": page}

    response = requests.request("GET", url,
                                headers=headers,
                                params=querystring)

    return response.text


if __name__ == '__main__':
    count = 0
    page = 0
    x = get_call_list(0)
    x = get_call_list(page)
    x = x.replace('null', 'None')
    x = x.replace('true', 'True')
    x = x.replace('false', 'False')
    x = eval(x)
    keys = x['calls'][0].keys()
    while True:
        x = get_call_list(page)
        x = x.replace('null', 'None')
        x = x.replace('true', 'True')
        x = x.replace('false', 'False')
        x = eval(x)
        y = x['calls']

        # format converion to avoid errors on SQL insert
        for l in y:
            for key in keys:
                if key not in l.keys():
                    l[key] = ''
            for q in l:
                if type(l[q]) is list:
                    l[q] = str(l[q])
                elif type(l[q]) is dict:
                    l[q] = str(l[q])

        count += len(y)
        mysql.insert_sql('rcs_ctm_calls', y)

        if len(y) < 10:
            break
        else:
            page += 1
    mysql.close_cnx()

    print '{} records updated'.format(count)
