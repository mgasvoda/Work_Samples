"""Retrieves information from Infusionsoft service for import."""

import xmlrpclib
import datetime
import MySqlInsert

key = 'Infusionsoft API Key'
server = xmlrpclib.ServerProxy("https://YOURAPP.infusionsoft.com:443/api/xmlrpc")

user = 'DB_User'
password = 'DB_Password'
host = 'DB_Host'
database = 'DB_Name'

mysql = MySqlInsert.sql_monster(user, password, host, database)
mysql.x = 'Id'


def get_payments():
    """Retrieve ecommerce information."""
    fields = ['ChargeId', 'ContactId', 'Id', 'PayAmt', 'PayDate', 'PayType']
    query = {'Id': "%"}

    total_retrieved = 0
    retrieved_ids = []

    limit = 1000  # Limit the number of rows that will be returned
    page = 0  # Start with the first page

    while True:
        results = server.DataService.query(key, 'Payment', limit, page, query,
                                           fields, 'Id', False)
        for result in results:
            a = datetime.datetime.strptime(str(result['PayDate']),
                                           '%Y%m%dT%H:%M:%S')

            result['PayDate'] = a.strftime('%Y-%m-%d %H:%M:%S')
            total_retrieved += 1
            retrieved_ids.append(result['Id'])
            for field in fields:
                if field not in result.keys():
                    result['field'] = ''

        mysql.insert_sql('infu_payments', results)
        if len(results) < 1000:
            break
        elif page >= 10:
            break
        page += 1

    print 'Total Retrieved Records: '
    print total_retrieved


def get_contacts():
    """Retrieve contact information including leadsource."""
    fields = ['Id', 'FirstName', 'LastName', 'Email', 'Leadsource']
    query = {'Id': "%"}

    total_retrieved = 0
    retrieved_ids = []

    limit = 1000  # Limit the number of rows that will be returned
    page = 0  # Start with the first page

    while True:
        results = server.DataService.query(key, 'Contact', limit, page,
                                           query, fields, 'Id', False)
        for result in results:
            # print result
            total_retrieved += 1
            retrieved_ids.append(result['Id'])
            for field in fields:
                if field not in result.keys():
                    result[field] = ''
            # all_results.append(result)
        mysql.insert_sql('infu_contacts', results)
        if len(results) < 1000:
            break
        elif page >= 25:
            break
        page += 1

    print 'Total Retrieved Records: '
    print total_retrieved


if __name__ == '__main__':
    get_payments()
    get_contacts()
    mysql.close_cnx()
