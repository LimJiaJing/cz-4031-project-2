import psycopg2

def connect():
    #login instructions
    username="postgres"
    password="cz4031group34"
    database="TPC-H"
    login_data= f"dbname={database} user={username} password={password}"
    conn = None

    print('Connecting to the PostgreSQL database...')

    conn = psycopg2.connect(login_data)
    print('Connected')

    cur = conn.cursor()
    #https://support.labs.cognitiveclass.ai/knowledgebase/articles/835206-access-postgresql-from-python-using-pscopg2
    cur.execute("SELECT datname FROM pg_database;")
    # list all the databses in our account
    list_database = cur.fetchall()
    print(list_database)
    print("\n\n\n")
    #https://www.folkstalk.com/2022/09/postgresql-show-tables-with-code-examples.html
    #this one to fetch the tables we created
    cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
    list_tables = list(map(lambda x: x[0], cur.fetchall()))
    print(list_tables)
    print("\n\n\n")
    table_dict = {}
    ##fetch the columns of the tables
    table_dict = {}
    for i in list_tables:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE TABLE_NAME = '{}';".format(i))
        column = list(map(lambda x: x[0], cur.fetchall()))
        table_dict[i] = column
    return conn, table_dict

if __name__ == "__main__" :
    print("okay")
    start = connect()
    conn = start[0]
    tables = start[1]
    print(tables)
