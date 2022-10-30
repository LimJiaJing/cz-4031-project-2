import psycopg2
import sys
import json

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
    print("\n\n")
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
    list_of_tables = list(map(lambda x: x[0], cur.fetchall()))
    print(list_of_tables)
    print("\n\n\n")
    ##fetch the columns of the tables
    table_cols = {}
    for i in list_of_tables:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE TABLE_NAME = '{}';".format(i))
        column = list(map(lambda x: x[0], cur.fetchall()))
        table_cols[i] = column
    return conn, table_cols

def query_asker():
    #first ask for input
    # append Explain (from json) to front of input string
    print("Key in your Query. At the end of your query, press enter, Key in Ctrl + Z and then press enter again.\n")
    query = sys.stdin.read()
    modified_query= "Explain (format JSON) " + query
    return modified_query

def QEP_Generator(database_conn,input_string,flag):
    cursor = database_conn.cursor()
    cursor.execute(input_string)
    data = cursor.fetchall()
    json_object = json.dumps(data, indent=4)
    # Writing to sample.json
    if flag == 0:
        with open("test.json", "w") as outfile:
            outfile.write(json_object)
    else:
        with open("test_alt.json", "w") as outfile:
            outfile.write(json_object)

def generate_operation_list(plan):
    plan_list = []
    plan_list.append(plan)
    tlist=[plan["Node Type"]]

    while len(plan_list) > 0:
        node = plan_list.pop()
        cur_plan = node
        if "Plans" in  cur_plan.keys():
            for subplan in cur_plan["Plans"]:
                plan_list.append(subplan)
                tlist.append(subplan["Node Type"])
    return tlist

def conditions_generator(operation_list,input_string):
    #keyword_list = ["Hash","Hash Join","Seq Scan","Sort","Aggregate","Index Only Scan","Nested Loop","Index Scan","Materialize","Bitmap Heap Scan","Bitmap Index Scan"]
    # options =["SET enable_bitmapscan TO off","SET enable_hashagg TO off","SET enable_hashjoin TO off"
    #           ,"SET enable_indexscan TO off","SET enable_indexonlyscan TO off","SET enable_material TO off","SET enable_mergejoin TO off"
    #           ,"SET enable_nestloop TO off","SET enable_seqscan TO off","SET enable_sort TO off","SET enable_tidscan TO off"]
    modified_string = input_string
    for i in operation_list:
        #not sure about hash/aggregate
        if i == "Hash" or i == "Aggregate":
            modified_string="SET enable_hashagg TO off;\n"+modified_string
        elif i == "Hash Join":
            modified_string="SET enable_hashjoin TO off;\n"+modified_string
        elif i == "Bitmap Heap Scan" or i == "Bitmap Index Scan":
            modified_string="SET enable_bitmapscan TO off;\n"+modified_string
        elif i == "Seq Scan":
            modified_string="SET enable_seqscan TO off;\n"+modified_string
        elif i == "Index Only Scan":
            modified_string="SET enable_indexonlyscan TO off;\n"+modified_string
        elif i == "Index Scan":
            modified_string="SET enable_indexscan TO off;\n"+modified_string
        elif i == "Materialize":
            modified_string="SET enable_material TO off;\n"+modified_string
        elif i == "Sort":
            modified_string="SET enable_sort TO off;\n"+modified_string
        elif i == "Nested Loop":
            modified_string="SET enable_nestloop TO off;\n"+modified_string
        #not sure about gather merge
        elif i == "Gather Merge":
            modified_string="SET enable_mergejoin TO off;\n"+modified_string
    return modified_string

def AQP_generator(database_conn,input_string):
    with open("sample.json") as my_data_file:
        my_data = json.load(my_data_file)
    operation_list = generate_operation_list(my_data[0][0][0]["Plan"])
    modified_string = conditions_generator(operation_list,input_string)
    print(modified_string)
    QEP_Generator(database_conn,modified_string,1)

if __name__ == "__main__" :
    # print("okay")
    start = connect()
    conn = start[0]
    tables = start[1]
    # print(tables)
    query_string = query_asker()
    QEP_Generator(conn,query_string,0)
    AQP_generator(conn,query_string)