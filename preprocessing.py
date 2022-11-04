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
    #https://www.folkstalk.com/2022/09/postgresql-show-tables-with-code-examples.html
    #this one to fetch the tables we created
    cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
    list_of_tables = list(map(lambda x: x[0], cur.fetchall()))
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

def QEP_Generator(database_conn,input_string):
    cursor = database_conn.cursor()
    cursor.execute(input_string)
    data = cursor.fetchall()
    json_object = json.dumps(data, indent=4)
    # Writing to sample.json
    with open("QEP.json", "w") as outfile:
        outfile.write(json_object)
    print("QEP is generated in QEP.json")
    
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
    #The following was not added as of yet
    #enable_async_append (boolean)
    # enable_incremental_sort (boolean) 
    # enable_tidscan (boolean) 
    # enable_parallel_append (boolean)
    # enable_parallel_hash (boolean)
    # enable_partition_pruning (boolean)
    # enable_partitionwise_join (boolean)
    # enable_partitionwise_aggregate (boolean)

    list_of_AQP_Queries = []
    list_of_disables = []
    count = 1
    for i in operation_list:
        #Gather not in because i dont know where to pu
        #not sure about hash/aggregate
        # if i == "Hash" or i == "Aggregate":
        #     modified_string="SET enable_hashagg TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        if i == "Hash Join":
            modified_string="SET enable_hashjoin TO off;\n"+input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        elif i == "Bitmap Heap Scan" or i == "Bitmap Index Scan":
            modified_string="SET enable_bitmapscan TO off;\n"+input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        # elif i == "Seq Scan":
        #     modified_string="SET enable_seqscan TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        elif i == "Index Only Scan":
            modified_string="SET enable_indexonlyscan TO off;\n"+input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        elif i == "Index Scan":
            modified_string="SET enable_indexscan TO off;\n"+input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        # elif i == "Materialize":
        #     modified_string="SET enable_material TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        # elif i == "Sort":
        #     modified_string="SET enable_sort TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        elif i == "Nested Loop":
            modified_string="SET enable_nestloop TO off;\n"+input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #not sure about gather merge
        elif i == "Merge Join":
            modified_string="SET enable_mergejoin TO off;\n"+input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        elif i == "Gather Merge":
            modified_string = "SET enable_gathermerge TO off;\n"+input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        # elif i == "Memoize":
        #     modified_string = "SET enable_memoize TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        count+=1
    with open("AQPInfo.txt", "w") as my_data_file:
        for i in range(len(list_of_disables)):
            my_data_file.write(list_of_disables[i])
    return list_of_AQP_Queries

def AQP_generator(database_conn,input_string):
    with open("QEP.json") as my_data_file:
        my_data = json.load(my_data_file)
    operation_list = generate_operation_list(my_data[0][0][0]["Plan"])
    list_of_AQP_Queries = conditions_generator(operation_list,input_string)
    for i in range(len(list_of_AQP_Queries)):
        cursor = database_conn.cursor()
        cursor.execute(list_of_AQP_Queries[i])
        data = cursor.fetchall()
        json_object = json.dumps(data, indent=4)
        # Writing to sample.json
        with open("AQP" + "{}.json".format(i+1), "w") as outfile:
            outfile.write(json_object)
            print("AQP(s) is generated in AQP{}.json".format(i+1))



if __name__ == "__main__" :
    # print("okay")
    start = connect()
    conn = start[0]
    tables = start[1]
    # print(tables)
    query_string = query_asker()
    QEP_Generator(conn,query_string)
    AQP_generator(conn,query_string)