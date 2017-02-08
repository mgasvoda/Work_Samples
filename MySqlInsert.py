"""Handles the sql_monster class for interfacing with remote databases."""

import mysql.connector
from mysql.connector import errorcode


class sql_monster:
    """SQL connection for interfacing with remote database.

    Built for and tested with MariaDB on a remote hostgator server.
    """

    def __init__(self, username, password, host, database):
        """Initiate database connection."""
        self.username = username
        self.password = password
        self.host = host
        self.database = database
        self.x = 'id'

        try:
            cnx = mysql.connector.connect(user=self.username,
                                          password=self.password,
                                          host=self.host,
                                          database=self.database)
            self.cnx = cnx

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def removeDupes(self, cursor, data, table):
        """Clear duplicates from import data.

        Currently only checks off of "id" field, can be
        modified for greater flexibility.

        """
        result = []
        ids = []
        unique = 0
        dupe = 0
        get_ids = "SELECT id FROM {}".format(table)

        cursor.execute(get_ids)
        ids = [row[0] for row in cursor.fetchall()]

        for x in data:
            if long(x[self.x]) not in ids:
                result.append(x)
                unique += 1
            else:
                dupe += 1

        print ''
        print "unique results: " + str(unique)
        print "duplicate results: " + str(dupe)
        return result

    @staticmethod
    def generate_sql(table, variables):
        """Generate insert statement for use in executemany."""
        col_hold = ''
        val_hold = ''
        count = 0

        for variable in variables:
            if (count + 1) == len(variables):
                col_hold += variable
                val_hold += ('%(' + variable + ')s')
            else:
                col_hold += (variable + ', ')
                val_hold += ('%(' + variable + ')s, ')
                count += 1

        result = ('INSERT INTO {} ({}) VALUES ({})'.format(table, col_hold,
                                                           val_hold))

        return result

    def insert_sql(self, table, start_data):
        """Handle the import of data into the remote database.

        Batching is recommended for larger jobs (20k + rows) as timeout issues
        have occurred.

        """
        cursor = self.cnx.cursor()
        null = ''
        data_raw = start_data

        sql = self.generate_sql(table, data_raw[0].keys())

        for x in data_raw:
            for attr in x:
                if x[attr] is None:
                    x[attr] = null

        data = self.removeDupes(cursor, data_raw, table)

        cursor.executemany(sql, data)

        self.cnx.commit()
        cursor.close()

    @staticmethod
    def generate_table(keys):
        """Generate SQL for large table creation.

        Data types are set automatically and manipulated later for
        simplicity and speed, bypassing an unsolved import error when
        doing bulk uploads.

        """
        name = 'Test'
        col_hold = ''
        count = 0
        for key in keys:
            if (count + 1) == len(key):
                col_hold += ('`' + key + '` VARCHAR(255) NOT NULL ')
            else:
                col_hold += ('`' + key + '` VARCHAR(255) NOT NULL , ')
                count += 1

        result = 'CREATE TABLE `test`.`{}` ( {}, PRIMARY KEY (`id`))\
                  ENGINE = InnoDB;'.format(name, col_hold)

        return result

    def close_cnx(self):
        """End connection.

        Allowing the session to be safely terminated without
        data loss.

        """
        self.cnx.close()
