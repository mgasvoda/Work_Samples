"""Handles primary functions for SharpSpring throughout the project."""

import requests
import json
from os import urandom
from base64 import b64encode
import MySqlInsert

account_id = 'Account_ID_GOES HERE'
secret_key = 'Secret_Key_GOES HERE'

db_user = 'Database username '
db_pass = 'Database Password'
db_host = 'Database IP'
database = 'Database Name'

mysql = MySqlInsert.sql_monster(db_user, db_pass, db_host, database)


def send(m, p):
    """Transfer requests to SharpSpring.

    SharpSpring method and params outlined at:
    https://YOURAPP.marketingautomation.services/settings/pubapireference#apimethods

    """
    request = urandom(24)
    requestID = b64encode(request).decode('utf-8')

    data = {
        'method': m,
        'params': p,
        'id': requestID
    }

    # The Sharpspring reference will show encoding the URL with
    # http_build_query, but this is the output
    url = "http://api.sharpspring.com/pubapi/v1/?accountID={}&secretKey={}"\
        .format(account_id, secret_key)
    # Important - all requests must be sent in JSON format
    dataj = json.dumps(data)

    # The requests library replaces cURL used in PHP - much simpler.
    # Note that all SharpSpring API calls use the POST method
    r = requests.post(
        url,
        data=dataj
    )
    response = r.json()
    return response


def sync(table, db_table, record):
    """Send information through to database."""
    count = 0
    page = 0

    while True:
        p = {'where': {}, 'offset': page}
        contacts = send(table, p)
        contacts = contacts['result'][record]
        mysql.insert_sql(db_table, contacts)
        for contact in contacts:
            count += 1

        if len(contacts) < 500:
            break
        elif page >= 8500:
            break
        else:
            page += 500

    print('{} Records processed for {}'.format(count, db_table))
    return count


if __name__ == '__main__':
    sync('getLeads', 'TABLENAME', 'lead')
    sync('getOpportunities', 'TABLENAME', 'opportunity')
    sync('getAccounts', 'TABLENAME', 'account')
    sync('getOpportunityLeads', 'TABLENAME', 'getWhereopportunityLeads')
    mysql.close_cnx()
