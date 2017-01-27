import MySqlInsert
import requests
from os import urandom
from base64 import standard_b64encode, b64encode

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


def send():
    # method and params outlined at:
    # https://YOURAPP.marketingautomation.services/settings/pubapireference#apimethods

    # Sets session ID using urandom for enhanced security
    request = urandom(24)
    requestID = b64encode(request).decode('utf-8')

    querystring = {"names": "1", "all": "1"}

    # The Sharpspring reference will show encoding the URL with
    # http_build_query, but this is the output
    url = "https://api.calltrackingmetrics.com/api/v1/accounts"
    headers = {
        'authorization': 'Basic {}'.format(auth_endcoded),
        'content-type': "application/json"
    }

    # The requests library replaces cURL used in PHP - much simpler.
    # Note that all SharpSpring API calls use the POST method
    response = requests.request("GET", url, headers=headers, params=querystring)

    print(response.text)


def get_call_list(page):
    url = "https://api.calltrackingmetrics.com/api/v1/accounts/{}/calls".format(account_id)
    querystring = {"page": page}

    headers = {
        'authorization': 'Basic {}'.format(test),
        'content-type': "application/json"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

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
