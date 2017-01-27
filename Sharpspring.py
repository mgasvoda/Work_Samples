"""Handles primary functions for SharpSpring throughout the project."""

import requests
import json
from os import urandom
from base64 import b64encode

account_id = 'Account_ID_GOES HERE'
secret_key = 'Secret_Key_GOES HERE'


# Returns all opportunities in the specified instance without any filtering
def getOpportunities():
    """Basic common function - returning all Opportunities."""
    method = 'getOpportunities'
    params = {'where': {}}

    response = send(method, params)
    accounts = response['result']['opportunity']

    ops = []
    for account in accounts:
        ops.append(account)

    return ops


def send(m, p):
    """Core method for sending requests to SharpSpring."""
    # method and params outlined at:
    # https://YOURAPP.marketingautomation.services/settings/pubapireference#apimethods

    # Sets session ID using urandom for enhanced security
    request = urandom(24)
    requestID = b64encode(request).decode('utf-8')

    data = {
        'method': m,
        'params': p,
        'id': requestID
    }

    # The Sharpspring reference will show encoding the URL with
    # http_build_querybut this is the output
    url = ("http://api.sharpspring.com/pubapi/v1/?accountID={}&secretKey={}"
           .format(account_id, secret_key))
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


def update_accounts(accounts):
    """Commonly used function for updating a group of accounts."""
    method = 'updateAccounts'
    holder = {}
    for account in accounts:
        x = accounts[account]
        # Because list indices start at 0 and length starts at 1, subtract 1
        # from length to get the right index for the donor_totals dictionary.
        # This should always work regardless of the number of opportunities
        y = x[len(x) - 1]
        holder[account] = y
    params = holder
    send(method, params)


if __name__ == '__main__':
    holder = getOpportunities()
    print(holder)
    print(holder[0].keys())
