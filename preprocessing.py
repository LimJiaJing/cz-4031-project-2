import psycopg2
import sys
import json
import os
import re

QEP_FILENAME = "qep.json"
PLANS_DIRECTORY = "generated_plans"
JSON_KEYS_TO_KEEP = [
    "Plans",
    "Total Cost",
    "Node Type",
    "Relation Name",
    "Alias",
    "Filter",
    "Hash Cond",
    "Merge Cond",
    "Index Cond",
    "Join Filter",
    "Index Name",
]

JOIN_CONDS = [
    "Hash Cond",
    "Merge Cond",
    "Index Cond",
]

FILTERS = ["Filter", "Join Filter"]


def connect():
    # login instructions
    username = "postgres"
    password = "cz4031group34"
    database = "TPC-H"
    login_data = f"dbname={database} user={username} password={password}"
    conn = None

    print('Connecting to the PostgreSQL database...')

    conn = psycopg2.connect(login_data)
    print('Connected')
    print("\n\n")
    cur = conn.cursor()
    # https://support.labs.cognitiveclass.ai/knowledgebase/articles/835206-access-postgresql-from-python-using-pscopg2
    cur.execute("SELECT datname FROM pg_database;")
    # list all the databses in our account
    list_database = cur.fetchall()
    # https://www.folkstalk.com/2022/09/postgresql-show-tables-with-code-examples.html
    # this one to fetch the tables we created
    cur.execute(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
    list_of_tables = list(map(lambda x: x[0], cur.fetchall()))
    ##fetch the columns of the tables
    table_cols = {}
    for i in list_of_tables:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE TABLE_NAME = '{}';".format(i))
        column = list(map(lambda x: x[0], cur.fetchall()))
        table_cols[i] = column
    return conn, table_cols


def query_asker():
    query = ""
    print("Please key in your Query:")
    while True:
        newline = input().rstrip()
        if newline:
            query = f"{query}\n{newline}"
        else:
            break
    print("Finished reading query.\n")
    print("Generating QEP and AQP(s).\n")
    modified_query = "SET max_parallel_workers_per_gather = 0;EXPLAIN (FORMAT JSON, ANALYZE, VERBOSE) " + query
    return modified_query


def qep_generator(database_conn, query):
    cursor = database_conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    json_object = json.dumps(data, indent=4)
    # Writing to sample.json
    qep_path = os.path.join(PLANS_DIRECTORY, QEP_FILENAME)
    with open(qep_path, "w") as f:
        f.write(json_object)
    print(f"QEP is generated and written to {qep_path}.")


def generate_operation_list(plan):
    plan_list = []
    plan_list.append(plan)
    tlist = [plan["Node Type"]]

    while len(plan_list) > 0:
        node = plan_list.pop()
        cur_plan = node
        if "Plans" in cur_plan.keys():
            for subplan in cur_plan["Plans"]:
                plan_list.append(subplan)
                tlist.append(subplan["Node Type"])
    return tlist


def conditions_generator(operation_list, input_string):
    # The following was not added as of yet
    # enable_async_append (boolean)
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
    temp_input_string = "SET max_parallel_workers_per_gather = 0;\n" + input_string
    for i in operation_list:
        print(i)
        # Gather not in because i dont know where to pu
        # not sure about hash/aggregate
        # if i == "Hash" or i == "Aggregate":
        #     modified_string="SET enable_hashagg TO off;\n"+temp_input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        if i == "Hash Join":
            modified_string = "SET enable_hashjoin TO off;\n" + temp_input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        elif i == "Bitmap Heap Scan" or i == "Bitmap Index Scan":
            modified_string = "SET enable_bitmapscan TO off;\n" + temp_input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # elif i == "Seq Scan":
        #     modified_string="SET enable_seqscan TO off;\n"+temp_input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        elif i == "Index Only Scan":
            modified_string = "SET enable_indexonlyscan TO off;\n" + temp_input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        elif i == "Index Scan":
            modified_string = "SET enable_indexscan TO off;\n" + "SET enable_bitmapscan TO off;\n" + temp_input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # elif i == "Materialize":
        #     modified_string="SET enable_material TO off;\n"+temp_input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        # elif i == "Sort":
        #     modified_string="SET enable_sort TO off;\n"+temp_input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        elif i == "Nested Loop":
            modified_string = "SET enable_nestloop TO off;\n" + temp_input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # not sure about gather merge
        elif i == "Merge Join":
            modified_string = "SET enable_mergejoin TO off;\n" + temp_input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        elif i == "Gather Merge":
            modified_string = "SET enable_gathermerge TO off;\n" + temp_input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # elif i == "Memoize":
        #     modified_string = "SET enable_memoize TO off;\n"+temp_input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        # print(temp_input_string)
    aqp_info_filename = "aqp_info.txt"
    aqp_info_path = os.path.join(PLANS_DIRECTORY, aqp_info_filename)
    with open(aqp_info_path, "w") as f:
        for i in range(len(list_of_disables)):
            f.write(list_of_disables[i])
    return list_of_AQP_Queries


def aqp_generator(database_conn, query):
    qep_path = os.path.join(PLANS_DIRECTORY, QEP_FILENAME)
    with open(qep_path) as f:
        my_data = json.load(f)
    operation_list = generate_operation_list(my_data[0][0][0]["Plan"])
    aqps = conditions_generator(operation_list, query)
    print(aqps)
    for i in range(len(aqps)):
        cursor = database_conn.cursor()
        cursor.execute(aqps[i])
        data = cursor.fetchall()
        json_object = json.dumps(data, indent=4)

        # output to file
        aqp_filename = f"aqp_{i+1}.json"
        aqp_path = os.path.join(PLANS_DIRECTORY, aqp_filename)
        with open(aqp_path, "w") as f:
            f.write(json_object)
            print(f"AQP {i+1} is generated and written to {aqp_path}")


def initialize_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))

def read_json(path):
    f = open(path)
    res = json.load(f)
    return res;


def clean_plan(plan):

    if "Plans" in list(plan):
        plans = plan["Plans"]
        for subplan in plans:
            clean_plan(subplan)
    for key in list(plan):
        # remove unneeded keys from plan
        if key not in JSON_KEYS_TO_KEEP:
            plan.pop(key, None)
        else:
            if key in JOIN_CONDS + FILTERS:
                plan[key] = clean_cond(plan[key])


def clean_cond(raw):
    cond = raw.replace("(", "").replace(")", "")
    cond = cond.split(" AND ")
    for i in range(len(cond)):
        cond[i] = cond[i].split(" OR ")
    clean = ""
    for OR_list in cond:
        if len(clean) != 0:
            clean = clean + " AND"
        for temp_cond in OR_list:
            temp_cond = parse_cond(temp_cond.strip())
            if len(clean) == 0:
                clean = temp_cond
            elif clean[-3:] == "AND":
                clean = clean + " " + temp_cond
            else:
                clean = clean + " OR " + temp_cond
    return clean


def parse_cond(raw):
    # need to create condition for if there is decimal
    raw = re.sub(r'([a-z].*\.)?(.*) (>=|<=|=|<|>|<>) ([a-z].*\.)?(.*)', r'\2 \3 \5', raw)
    raw = re.sub(r"([a-z].*\.)?(.*) (>=|<=|=|<|>|<>) (.*)(::.*)", r'\2 \3 \4', raw)
    raw = re.sub(r"([a-z]...*\.)?(.*)(::.*) (>=|<=|=|<|>|<>) (.*)", r'\2 \4 \5', raw)
    raw = re.sub(r"([a-z].*\.)?(.*)(::.*) (>=|<=|=|<|>|<>) (.*)(::.*)", r'\2 \4 \5', raw)
    raw = re.sub(r"([a-z].*\.)?(.*)(::.*) ~~ ('.*')(::.*)", r'\2 like \4', raw)
    raw = re.sub(r"([a-z].*\.)?(.*)(::.*) !~~ ('.*')(::.*)", r'\2 not like \4', raw)
    raw = re.sub(r"([a-z].*\.)?(.*) ANY '({.*})'(::.*)", r'\3', raw)
    raw = re.sub(r"([a-z].*\.)?(.*) ANY '({.*})'", r'\3', raw)
    raw = re.sub(r"(.*) (>=|<=|=|<|>|<>) '([0-9]*)'", r'\1 \2 \3', raw)
    raw = re.sub(r"(.*)(SubPlan [0-9])(.*)", r'\1_\3', raw)
    return raw

def clean_json_files():
    json_filenames = [f for f in os.listdir(PLANS_DIRECTORY) if (
            (os.path.isfile(os.path.join(PLANS_DIRECTORY, f)) and
             f[-4:] == "json") and
            ("clean" not in f)
    )
                      ]
    for json_filename in json_filenames:
        json_path = os.path.join(PLANS_DIRECTORY, json_filename)
        plan = read_json(json_path)[0][0][0]["Plan"]
        clean_plan(plan)
        json_object = json.dumps(plan, indent=4)
        clean_filename = f"clean_{json_filename}"
        clean_path = os.path.join(PLANS_DIRECTORY, clean_filename)
        with open(clean_path, "w") as f:
            f.write(json_object)
            print(f"Clean {json_filename} is generated and written to {clean_path}")


def run_preprocessing():
    start = connect()
    conn = start[0]
    tables = start[1]
    # print(tables)
# <<<<<<< Updated upstream
    initialize_dir(PLANS_DIRECTORY)
    query = query_asker()
    qep_generator(conn, query)
    aqp_generator(conn, query)

    ## generate clean json files
    clean_json_files()
# =======
#     # read all files
#     sql_filenames = [f for f in listdir("Queries&Json") if (isfile(join("Queries&Json", f)) and f[-3:] == "sql")]
#
#     for sql_filename in sql_filenames:
#         if sql_filename[-5:] == "e.sql":
#             continue
#         f = open("Queries&Json\\"+sql_filename, "r")
#         sql_file = f.read()
#         f.close()
#         query_string = query_asker(sql_file)
#         qep_generator(conn, query_string, sql_filename)
#         # aqp_generator(conn, query_string)
# >>>>>>> Stashed changes
if __name__ == "__main__":
    run_preprocessing()