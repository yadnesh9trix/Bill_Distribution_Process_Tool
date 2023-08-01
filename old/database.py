"""
database module is contain all database related class and method of class which is used to do database related
operation.

"""
import sqlalchemy as sql
import psycopg2
# from config import config
import pandas as pd
import yaml
import os
from datetime import datetime

chunk_size = 10000


class DataBase:

    db_name = 'postgresql'
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yml")
    with open(config_path, "r") as yml_file:
        try:
            cfg = yaml.safe_load(yml_file)
            config = cfg[db_name]
        except yaml.YAMLError as exc:
            print(exc)

    # def __init__(self):
    #     """
    #     DataBase class is used to create database connection using class method, this class read database configuration
    #     .yml file which contain all database configuration along with file path and other data.
    #     This class contain method which is used to create connection, read the data read the data using query and
    #     and close connection.
    #     In initialisation of the class database configuration .yml file will read and database configuration object will
    #     created, this object will used in connect method to create connection with remote database.
    #     """
    #     self.constr = 'mysql+mysqlconnector://' + DataBase.config['user'] + ':' + \
    #                   DataBase.config['passwd'] + '@' + DataBase.config['host'] + '/' + DataBase.config['db']

    def __init__(self):
        """
        DataBase class is used to create database connection using class method, this class read database configuration
        .yml file which contain all database configuration along with file path and other data.
        This class contain method which is used to create connection, read the data read the data using query and
        and close connection.
        In initialisation of the class database configuration .yml file will read and database configuration object will
        created, this object will used in connect method to create connection with remote database.
        """
        self.constr = DataBase.config['user'] + ':' + \
                      DataBase.config['passwd'] + '@' + DataBase.config['host'] + '/' + DataBase.config['db']

    def connect(self):
        """
        connect method of database class is used to create connection to the remote database using constr variable
        of class object.

        :return: connection of database.
        """
        # eng = sql.engine.create_engine(self.constr, echo=False, pool_pre_ping=True)
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        # conn = psycopg2.connect(self.constr)
        conn = psycopg2.connect(host=DataBase.config['host'], dbname=DataBase.config['db'],
                                user=DataBase.config['user'], password=DataBase.config['passwd'])
        return conn.cursor()

    # def connect(self):
    #     """ Connect to the PostgreSQL database server """
    #     conn = None
    #     try:
    #         # read connection parameters
    #         # params = config()
    #
    #         # connect to the PostgreSQL server
    #         print('Connecting to the PostgreSQL database...')
    #         # conn = psycopg2.connect(self.constr)
    #         conn = psycopg2.connect(host=DataBase.config['host'], dbname=DataBase.config['db'],
    #                                 user=DataBase.config['user'], password=DataBase.config['passwd'])
    #         # create a cursor
    #         cur = conn.cursor()
    #
    #         # execute a statement
    #         print('PostgreSQL database version:')
    #         cur.execute('SELECT version()')
    #
    #         # display the PostgreSQL database server version
    #         db_version = cur.fetchone()
    #         print(db_version)
    #
    #         # close the communication with the PostgreSQL
    #         cur.close()
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print(error)
    #     finally:
    #         if conn is not None:
    #             conn.close()
    #             print('Database connection closed.')

    def read_db(self, query):
        """
        This method execute the sql query and fetch the table from database, to perform data retrieval from database
        this method used connection method of the database class and execute the input query and return retrieved data.
        After retrieving data connection will close.

        :param query: sql query passed as argument while calling this this method using class object.
        :return: data which retrieved from database using sql query.
        """
        # connection = self.connect()
        connection = self.connect()
        try:
            # readrows = connection.execute(query).fetchall()
            readrows = connection.execute(query)
            sites_result = connection.fetchall()
            names = [x[0] for x in connection.description]
            dff = pd.DataFrame(sites_result,columns=names)
        except Exception as e:
            print("Error reading data from MySQL table", e)
            raise e
        finally:
            connection.close()
        return dff

    def load_df(self, df, table):
        """
        As name load_df method is load the dataframe to the database using pandas to_sql method, this method accept two
        parameter one dataframe as df and table name as table.
        This method is create connection using connection method and load dataframe to the database and finally close
        connection.
        :param table: table name on which dataframe to be loaded.
        :param df: dataframe to be load.
        :return: None
        """
        df = pd.DataFrame(df)
        connection = self.connect()
        try:
            df.to_sql(name=table, con=connection, if_exists='append', index=False, index_label='id',
                      chunksize=chunk_size)
        except Exception as e:
            print(f"Error loading data to MySQL table:{table}", e)
            raise e
        finally:
            connection.close()

        return None


aa = DataBase()
# conn = aa.connect()
query = f"select property_billreceipt.propertykey, property_billreceipt.propertybillkey, " \
        f"property_billreceipt.financialyearkey, property_billreceipt.receiptdate, property_billreceipt.modeofpayment," \
        f"property_billreceipt.honoureddate, sum((select coalesce(sum(property_billreceiptdetail.paidamount),0) as billamount " \
        f"from property_billreceiptdetail " \
        f"where property_billreceipt.propertybillreceiptkey=property_billreceiptdetail.propertybillreceiptkey) ) as paidamount " \
        f"from property_billreceipt as property_billreceipt INNER join (select financialyearkey, fromdate " \
        f"from financialyear) as financialyear on financialyear.financialyearkey = property_billreceipt.financialyearkey " \
        f"where property_billreceipt.receiptreturn = 'N' " \
        f"group by property_billreceipt.propertykey, property_billreceipt.propertybillkey, property_billreceipt.financialyearkey, property_billreceipt.receiptdate, property_billreceipt.modeofpayment, property_billreceipt.honoureddate"

# query = f"SELECT * FROM public.property_billreceipt " \
#         f"ORDER BY propertybillreceiptkey ASC LIMIT 100"
aazzz = aa.read_db(query)
aazzz.to_csv("trialplist.csv",sep="|",index=False)
# names = [x[0] for x in cur.description]

