import psycopg2
import sys
import json
import os
import re
from collections import defaultdict
from difflib import SequenceMatcher
import sqlparse
import re
from os import listdir
from os.path import isfile, join

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

# clean_plan##########################

json_keys_to_keep = [
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
join_conds = [
    "Hash Cond",
    "Merge Cond",
    "Index Cond",
    "Join Filter",
]

table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
def clean_plan(plan):

    if "Plans" in list(plan):
        plans = plan["Plans"]
        for subplan in plans:
            clean_plan(subplan)
    for key in list(plan):
        # remove unneeded keys from plan
        if key not in json_keys_to_keep:
            plan.pop(key, None)
        else:
            if key in join_conds + ["Filter"]:
                plan[key] = clean_cond(plan[key])

def clean_cond(join_cond):
    cond = join_cond.replace("(", "").replace(")", "")
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

# summarize_plan#######################

index_join_cp = ("Index Scan", "Nested Loop")
join_conds = [
    "Hash Cond",
    "Merge Cond",
    "Index Cond",
]
filters = ["Filter", "Join Filter"]
scans = ["Index Scan", "Seq Scan"]

def summarize_plan(plan, summary, depth, cost, parent_node_type):
    if "Plans" in list(plan):
        plans = plan["Plans"]
        for subplan in plans:
            summarize_plan(subplan, summary, depth+1, cost, plan["Node Type"])
    conditions = []
    # keys to annotate are conditions
    common_keys = set(join_conds+filters).intersection(list(plan))
    for json_key in common_keys:
        conditions.append((plan[json_key], json_key))
    for node, json_key in conditions:
        args = re.split(" AND | OR ", node)
        for arg in args:
            if node not in list(summary):
                add_to_summary(plan, parent_node_type, arg, depth, cost, json_key)
            else:
                add_to_summary(plan, parent_node_type, arg, depth, cost, json_key)
                duplicates.append(arg.strip())
    node_type = plan["Node Type"]
    # keys to annotate are relations
    if node_type in scans:
        if plan["Relation Name"] in plan["Alias"]:
            key = plan["Relation Name"]
        else:
            key = f"{plan['Relation Name']} {plan['Alias']}"
        if key not in list(summary):
            add_to_summary(plan, parent_node_type, key, depth, cost, node_type, relation = True)
        else:
            add_to_summary(plan, parent_node_type, key, depth, cost, node_type, relation = True)
            duplicates.append(key.strip())


def add_to_summary(plan, parent_node_type, arg, depth, cost, json_key, relation=False):
    child = plan["Node Type"]
    index_key = None
    relation_name = None
    cond = None
    join_algo = None

    # if the key is from a filter, we want to know based on what is it filtered from
    if json_key in filters and len(set(join_conds).intersection(list(plan))) == 1:
        join_algo = set(join_conds).intersection(list(plan)).pop()
        cond = plan[join_algo]

    # if the key is from a filter and the filter is based on Index Scan node type, we want to know the index name and on what table is it filtered from
    if json_key == "Filter" and plan["Node Type"] == "Index Scan":
        index_key = plan["Index Name"]
        relation_name = plan["Relation Name"]

    # if the key is from a condition, we want to know which index (and its table) is being used to find the required data
    if json_key == "Index Cond":
        index_key = plan["Index Name"]
        relation_name = plan["Relation Name"]

    # if the key is "Index Scan", its algorithm data for a table scan, we only need to know what is the index name since the relation name will be the key in the result
    if json_key == "Index Scan":
        index_key = plan["Index Name"]

    # create empty list if new key
    if arg.strip() not in list(summary):
        summary[arg.strip()] = []

    # check if this condition is fulfilled using an index join
    if is_index_join(child, parent_node_type) and json_key == "Index Cond":
        summary[arg.strip()].append(
                ("Index Join", index_key, relation_name, cond, join_algo, cost) # removed depth and parent_node_type
        ) # (algorithm, index_key, relation, cond, join_algo, cost)

    else:
        summary[arg.strip()].append(
            (plan["Node Type"], index_key, relation_name, cond, join_algo, cost)
        )


def is_index_join(child, parent):
    if (child, parent) == index_join_cp:
        return True
    else:
        return False
###########################

# sql_parser##########################
def parse_sql(raw):
    query = sqlparse.format(raw.strip(), strip_comments=True,
                            reindent=True, keyword_case="upper")
    # print(query)
    query_list = [line.strip() for line in query.split("\n")]

    '''
    sql_to_level_mapping = {
        line: [index_0, index_1, ..., index_n]
    }
    '''
    # sql_to_level_mapping = get_sql_to_level_mapping(query_list)
    # print_sql_to_level_mapping(sql_to_level_mapping)
    return get_sql_to_level_mapping(query_list)


def print_sql_to_level_mapping(sql_to_level_mapping):
    for k, v in sql_to_level_mapping.items():
        print(f"Key: {k} | Index: {v}")


def get_sql_to_level_mapping(query_list):
    sql_to_level_mapping = defaultdict(list)
    is_unwanted_line = True
    i = 0
    while i < len(query_list):
        line_to_skip = 1
        if re.search(r'\(SELECT', query_list[i]):
            new_cleaned_key = modify_key_for_subquery(i, query_list, sql_to_level_mapping)
            query_list[i] = new_cleaned_key
            is_unwanted_line = True
            cleaned_key = remove_unwanted_keywords(
                query_list[i], has_in_keyword)
            sql_to_level_mapping[cleaned_key].append(i)
            i += line_to_skip
            continue

        has_in_keyword = False
        if (re.search(r'FROM', query_list[i]) and not re.search(r'\(|\)', query_list[i])) or re.search(r'WHERE',
                                                                                                       query_list[
                                                                                                           i]) or (
                not is_unwanted_line and not re.search(r'SELECT|ORDER BY', query_list[i])):
            is_unwanted_line = False

            if re.search(r'in \(', query_list[i]):
                has_in_keyword = True
                new_line, local_line_to_skip = modify_line_with_in_keyword(
                    i, query_list)
                query_list[i] = new_line
                line_to_skip = local_line_to_skip

            cleaned_key = remove_unwanted_keywords(
                query_list[i], has_in_keyword)
            sql_to_level_mapping[cleaned_key].append(i)

            regex = r'(.*) BETWEEN (.*) AND (.*)'
            if re.match(regex, cleaned_key):
                try:
                    result1 = str(eval(re.match(regex, cleaned_key).groups()[1]))
                    result2 = str(eval(re.match(regex, cleaned_key).groups()[2]))
                    key1 = re.sub(regex, r'\1 >= ' + result1, cleaned_key)
                    key2 = re.sub(regex, r'\1 <= ' + result2, cleaned_key)
                    sql_to_level_mapping[key1].append(i)
                    sql_to_level_mapping[key2].append(i)
                    del sql_to_level_mapping[cleaned_key]
                except:
                    # might be a date
                    regex = r'(.*) BETWEEN date (.*) AND date (.*) AS .*'
                    if re.match(regex, cleaned_key):
                        key1 = re.sub(regex, r'\1 >= ' + "date " + r'\2', cleaned_key).strip()
                        key2 = re.sub(regex, r'\1 <= ' + "date " + r'\3', cleaned_key).strip()
                        sql_to_level_mapping[key1].append(i)
                        sql_to_level_mapping[key2].append(i)
                        del sql_to_level_mapping[cleaned_key]

            if re.match(r'(.*) (=|>|<|>=|<=|<>) (.*)', cleaned_key):
                reversed_cleaned_key = re.sub(r'(.*) (=) (.*)', r'\3 \2 \1', cleaned_key)
                sql_to_level_mapping[reversed_cleaned_key] = sql_to_level_mapping[cleaned_key]
        else:
            is_unwanted_line = True
        i += line_to_skip

    return sql_to_level_mapping


def modify_key_for_subquery(i, query_list, sql_to_level_mapping):
    cleaned_key = remove_unwanted_keywords(query_list[i - 1])
    v = sql_to_level_mapping[cleaned_key]
    del sql_to_level_mapping[cleaned_key]
    new_cleaned_key = cleaned_key + " _"
    sql_to_level_mapping[new_cleaned_key] = v
    return new_cleaned_key


def modify_line_with_in_keyword(start, query_list):
    line = query_list[start]
    line_to_skip = 0
    for i in range(start + 1, len(query_list)):
        line += query_list[i]
        line_to_skip += 1
        if re.search(r'\)', query_list[i]):
            break

    stack = []
    temp_line = ""
    for i in line:
        if i == "(":
            stack.append(i)
        elif i == ")" and stack:
            break
        elif stack:
            temp_line += i

    split_line = [i.replace("'", "") for i in temp_line.split(",")]
    new_line = ""
    for i in range(len(split_line)):
        try:
            if i != len(split_line) - 1:
                new_line += int(split_line[i]) + ","
            else:
                new_line += int(split_line[i])
        except:
            format_string = ""
            if len(split_line[i].split(" ")) > 1:
                format_string = '"'
            if i != len(split_line) - 1:
                new_line += format_string + split_line[i] + format_string + ","
            else:
                new_line += format_string + split_line[i] + format_string

    return "{" + new_line + "}", line_to_skip + 1


def remove_unwanted_keywords(key, has_in_keyword=False):
    regex = re.compile(r'^(AND|OR|FROM|WHERE|GROUP BY) (.*)')
    if re.match(regex, key):
        key = re.sub(regex, r'\2', key, 1)

    regex = re.compile(r'HAVING sum(.*) (>) (.*)')
    if re.match(regex, key):
        key = re.sub(regex, r"\1 \2 '\3'", key)

    regex = re.compile(r"\(|\)|,|;")
    if not has_in_keyword and re.search(regex, key):
        key = re.sub(regex, "", key).strip()

    regex = re.compile(r'([a-z].*\.)?([a-z].*) (=|>|<|>=|<=|<>) ([a-z].*\.)?([a-z].*)')
    if re.match(regex, key):
        key = re.sub(regex, r'\2 \3 \5', key)

    regex = re.compile(r'([a-z].*\.)?([a-z].*) (=|>|<|>=|<=|<>) ([a-z].*\.)?(.*)')
    if re.match(regex, key):
        key = re.sub(regex, r'\2 \3 \5', key)

    regex = r'(.*\.)?(.*) (=|>|<|>=|<=|<>) (.*\.)?(.*)(AS.*)'
    if re.match(regex, key):
        key = re.sub(regex, r'\2 \3 \5', key)

    regex = r'(.*JOIN.*ON.*) (.*) = (.*)'
    if re.match(regex, key):
        key = re.sub(regex, r'\2 = \3', key)

    regex = re.compile(r'(.*) (>|<|>=|<=) (.*)')
    if re.match(regex, key):
        try:
            if re.search(r'\+|\-|\*|\/', key):
                result = str(eval(re.match(regex, key).groups()[2]))
                key = re.sub(regex, r'\1 \2 ' + result, key)
        except:
            pass

    return key.strip()
############################

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
    # print("Finished reading query.\n")
    # print("Generating QEP and AQP(s).\n")
    #modified_query = "SET max_parallel_workers_per_gather = 0;\n" + "SET enable_bitmapscan TO off;\n" + "SET enable_indexonlyscan TO off;\n"+"EXPLAIN (FORMAT JSON, ANALYZE, VERBOSE) " + query
    query = sqlparse.format(query.strip(), strip_comments=True,
                    reindent=True, keyword_case="upper")
    return query

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
    for i in operation_list:
        print(i)
        # Gather not in because i dont know where to pu
        # not sure about hash/aggregate
        # if i == "Hash" or i == "Aggregate":
        #     modified_string="SET enable_hashagg TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        if i == "Hash Join":
            modified_string = "SET enable_hashjoin TO off;\n" + input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # elif i == "Bitmap Heap Scan" or i == "Bitmap Index Scan":
        #     modified_string = "SET enable_bitmapscan TO off;\n" + input_string
        #     if modified_string not in list_of_AQP_Queries:
        #         list_of_AQP_Queries.append(modified_string)
        #         list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
        #         count += 1
        # elif i == "Seq Scan":
        #     modified_string="SET enable_seqscan TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        # elif i == "Index Only Scan":
        #     modified_string = "SET enable_indexonlyscan TO off;\n" + input_string
        #     if modified_string not in list_of_AQP_Queries:
        #         list_of_AQP_Queries.append(modified_string)
        #         list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
        #         count += 1
        elif i == "Index Scan":
            modified_string = "SET enable_indexscan TO off;\n" + "SET enable_bitmapscan TO off;\n" + input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # elif i == "Materialize":
        #     modified_string="SET enable_material TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        # elif i == "Sort":
        #     modified_string="SET enable_sort TO off;\n"+input_string
        #     if modified_string not in list_of_AQP_Queries:
        #       list_of_AQP_Queries.append(modified_string)
        #       list_of_disables.append("AQP{0} has {1} disabled\n".format(count,i))
        #       count+=1
        elif i == "Nested Loop":
            modified_string = "SET enable_nestloop TO off;\n" + input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # not sure about gather merge
        elif i == "Merge Join":
            modified_string = "SET enable_mergejoin TO off;\n" + input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        elif i == "Gather Merge":
            modified_string = "SET enable_gathermerge TO off;\n" + input_string
            if modified_string not in list_of_AQP_Queries:
                list_of_AQP_Queries.append(modified_string)
                list_of_disables.append("AQP{0} has {1} disabled\n".format(count, i))
                count += 1
        # elif i == "Memoize":
        #     modified_string = "SET enable_memoize TO off;\n"+input_string
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


def run_preprocessing(query):
    start = connect()
    conn = start[0]
    tables = start[1]
    # print(tables)
    initialize_dir(PLANS_DIRECTORY)
    query = "SET max_parallel_workers_per_gather = 0;\n" + "SET enable_bitmapscan TO off;\n" + "SET enable_indexonlyscan TO off;\n"+"EXPLAIN (FORMAT JSON, ANALYZE, VERBOSE) " + query
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
    input_query = query_asker()
    run_preprocessing(input_query)