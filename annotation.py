#Test

from treelib import Node, Tree
from deepdiff import DeepDiff
import re
from preprocessing import read_json
from preprocessing import PLANS_DIRECTORY, JOIN_CONDS, FILTERS, QEP_FILENAME
import os
from preprocessing import parse_sql
from difflib import SequenceMatcher

INDEX_JOIN_CP = ["Index Scan", "Nested Loop"]
SCANS = ["Index Scan", "Seq Scan"]

def parse_cond(raw):
    raw = re.sub(r'(.*\.)?(.*) = (.*\.)?(.*)', r'\2 = \4', raw)
    return raw


def generate_comparison(dic):
    for item in dic.items():
        print("input", item)
    print("input", dic)
    annotations = {}

    for pair in dic.items():
        qep_tuple = pair[1][0]
        if qep_tuple == None:
            continue
        explanation = []
        qep_algo, qep_cost = qep_tuple[0], qep_tuple[1]
        explanation.append(qep_algo)

        if len(pair[1]) == 1:
            explanation.append("There is no alternative way for this operation.")
        else:
            aqp_tuples = pair[1][1:]
            explanation.append(compare_plan(qep_tuple, aqp_tuples))
        # for aqp_tuple in pair[1][1:]:
        #     if qep_tuple[0] != aqp_tuple[0]:
        #         explanation.append(compare(qep_tuple, aqp_tuple))
        for id in pair[0][1]:
            annotations[id] = " ".join(explanation)
    return annotations


def compare_plan(qep_tuple, aqp_tuples):
    qep_algo, qep_cost = get_node_type(qep_tuple[0]), qep_tuple[1]
    explanation = []
    diff_algos = []


    for aqp_tuple in aqp_tuples:
        if aqp_tuple == None:
            continue
        aqp_algo, aqp_cost = get_node_type(aqp_tuple[0]), aqp_tuple[1]
        if not qep_algo == aqp_algo and aqp_algo not in diff_algos:
            diff_algos.append(aqp_algo)
            if qep_cost < aqp_cost:
                explanation.append("{} is not used because its cost is {}, " \
                   "{} times larger than {}.".format(aqp_algo, aqp_cost, round(aqp_cost / qep_cost), qep_algo))
            else:
                explanation.append("Although {} has lower cost ({})," \
                                   " DBMS still choose to use {} with a higher cost ({}) "
                                   "for some other consideration".format(aqp_algo, aqp_cost, qep_algo,
                                                                     qep_cost))
    if len(diff_algos) == 0:
        if (qep_algo == "Seq Scan"):
            explanation.append("There is no index available.")
        explanation.append("There is no alternative way for this operation.")
    return " ".join(explanation)

def get_node_type(str):
    joins = ["Hash Join", "Merge Join", "Index Join", "Nested Loop"]
    scans = ["Seq Scan", "Index Scan", "Index Only Scan"]

    for join in joins:
        if join in str:
            return join
    for scan in scans:
        if scan in str:
            return scan
    return "Error"

def test_parsing(tree):
    join_cond = ["Merge Cond", "Hash Cond", "Index Cond"]
    cond_dic = {"Merge Cond": "Merge Join", "Hash Cond": "Hash Join", "Index Cond": "Index-based Join"}
    scans = ["Seq Scan", "Index Scan", "Bitmap Scan", "Index Only Scan"]
    for node in tree.all_nodes():
        res = None
        find_conds = [c for c in join_cond if c in node.data.keys()]
        if len(find_conds) > 0:
            cond = find_conds[0]
            temp_cond = node.data[cond].replace("(", "").replace(")", "")
            for subcond in temp_cond.split(" AND "):
                for subcond1 in subcond.split(" OR "):
                    res = " ".join((cond_dic[cond], "join condition:", parse_cond(subcond), "cost:", str(node.data["Total Cost"])))
        elif node.tag in scans:
            res = " ".join((node.tag, "on table", node.data["Relation Name"], "cost:", str(node.data["Total Cost"])))

def generate_operation_tree(plan):
    tree = Tree()

    queue = []
    node_id = 0
    queue.append((plan, node_id))
    tree.create_node(plan["Node Type"], node_id, data = get_attributes(plan))
    node_id += 1

    while len(queue) > 0:
        node_tuple = queue.pop()
        cur_plan = node_tuple[0]
        cur_plan_id = node_tuple[1]
        if "Plans" in  cur_plan.keys():
            for subplan in cur_plan["Plans"]:
                queue.append((subplan, node_id))
                tag = subplan["Node Type"]
                if subplan["Parent Relationship"] == "SubPlan":
                    tag = tag + "(SubPlan)" # why?
                tree.create_node(tag, node_id, data = get_attributes(subplan), parent = cur_plan_id)
                node_id += 1

    return tree


def get_attributes(plan):
    attributes = {}
    for key, value in plan.items():
        if not key == "Plans" and not key == "Node Type":
            attributes[key] = value
    return attributes

def summarize_plan(plan, summary, depth, cost, parent_node_type):
    if "Plans" in list(plan):
        plans = plan["Plans"]
        for subplan in plans:
            summarize_plan(subplan, summary, depth+1, cost, plan["Node Type"])
    conditions = []
    # keys to annotate are conditions
    common_keys = set(JOIN_CONDS+FILTERS).intersection(list(plan))
    for json_key in common_keys:
        conditions.append((plan[json_key], json_key))
    for node, json_key in conditions:
        args = re.split(" AND | OR ", node)
        for arg in args:
            if node not in list(summary):
                add_to_summary(summary, plan, parent_node_type, arg, depth, cost, json_key)
            else:
                add_to_summary(summary, plan, parent_node_type, arg, depth, cost, json_key)
                # duplicates.append(arg.strip())
    node_type = plan["Node Type"]
    # keys to annotate are relations
    if node_type in SCANS:
        if plan["Relation Name"] in plan["Alias"]:
            key = plan["Relation Name"]
        else:
            key = f"{plan['Relation Name']} {plan['Alias']}"
        if key not in list(summary):
            add_to_summary(summary, plan, parent_node_type, key, depth, cost, node_type)
        else:
            add_to_summary(summary, plan, parent_node_type, key, depth, cost, node_type)
            # duplicates.append(key.strip())


def add_to_summary(summary, plan, parent_node_type, arg, depth, cost, json_key):
    child = plan["Node Type"]
    index_key = None
    relation_name = None
    cond = None
    join_algo = None
   
    # if the key is from a filter, we want to know based on what is it filtered from
    if (json_key in FILTERS) and (len(set(JOIN_CONDS).intersection(list(plan))) == 1):
        join_algo = plan["Node Type"]
        join_algo_cond = set(JOIN_CONDS).intersection(list(plan)).pop()
        cond = plan[join_algo_cond]

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
    print(parent_node_type, child)
    # check if this condition is fulfilled using an index join
    print(is_index_join(child, parent_node_type))
    if (is_index_join(child, parent_node_type)) and ((json_key == "Index Cond") or (json_key == "Filter")):
        if (join_algo == "Index Scan"):
            join_algo = "Index Join"
        summary[arg.strip()].append(
                ("Index Join", index_key, relation_name, cond, join_algo, cost) # removed depth and parent_node_type
        ) # (algorithm, index_key, relation, cond, join_algo, cost)

    else:
        summary[arg.strip()].append(
            (plan["Node Type"], index_key, relation_name, cond, join_algo, cost)
        )


def is_index_join(child, parent):
    if [child, parent] == INDEX_JOIN_CP:
        return True
    else:
        return False
def summarize_plans():
    json_filenames = [f for f in os.listdir(PLANS_DIRECTORY) if (
        (os.path.isfile(os.path.join(PLANS_DIRECTORY, f)))
        and ("clean" in f))
                      ]

    if f"clean_{QEP_FILENAME}" not in json_filenames:
        raise Exception("No QEP file found")
    print(f"There are {len(json_filenames)-1} AQPs found.")
    for json_filename in json_filenames:
        print(f"{json_filename}\n")
    plans = []
    # rearrange filenames
    qep_index = json_filenames.index(f"clean_{QEP_FILENAME}")
    qep_file = json_filenames.pop(qep_index)
    json_filenames.insert(0, qep_file)
    for i in range(len(json_filenames)):
        if i == 0:
            continue  # skip qep
        aqp_num = i
        aqp_filename = f"clean_aqp_{aqp_num}.json"
        aqp_index = json_filenames.index(aqp_filename)
        aqp_file = json_filenames.pop(aqp_index)
        json_filenames.insert(i, aqp_file)
    print("=" * 88)
    print(json_filenames)

    for json_filename in json_filenames:
        json_path = os.path.join(PLANS_DIRECTORY, json_filename)
        plan = read_json(json_path)
        summary = {}
        # try:
        cost = plan["Total Cost"]
        summarize_plan(plan, summary, 0, cost, None)
        for k, v in summary.items():
            summary[k] = [*set(v)]
        plans.append(summary)
        print(f"Finish summarizing {json_filename}")
        print("="*100)
    return plans


def longest_common_substring(candidates, string, threshold):
    if len(candidates) == 0:
        return None
    longest_match_size = threshold
    chosen = None
    common_substring = None
    for e, e_refined in candidates:
        match = SequenceMatcher(None, e_refined, string).find_longest_match()
        if match.size == longest_match_size:
            print(string)
            print(e_refined)
            print(chosen, common_substring)
            print(f"SAME SUBSTRING LEN {match.size}\n")
        if match.size > longest_match_size:
            longest_match_size = match.size
            chosen = e
            common_substring = string[match.b:match.b+match.size]

    return chosen

def add_to_res(res, qep_summary, sql_summary, sql_key, qep_key):
    if len(qep_summary[qep_key]) > 1:
        if "Seq Scan" in set([e[0] for e in qep_summary[qep_key]]):
            for e in qep_summary[qep_key]:
                if e[0] == "Seq Scan":
                    res[(sql_key, tuple(sql_summary[sql_key]))] = (create_explanation(e), e[-1])
                    break
        else:
            print("Not resolved")

    else:
        res[(sql_key, tuple(sql_summary[sql_key]))] = (create_explanation(qep_summary[qep_key][0]), qep_summary[qep_key][0][-1])

def create_explanation(info):
    # (algorithm, index_key, relation, cond, join_algo, cost)
    algo, key, relation, cond, join_algo, cost = info
    # index join filter
    if (algo == "Index Join") and cond != None:
        return f"Filtered from the results of {algo} on '{cond}'. " \
               f"The {algo} used  relation '{relation}' index key '{key}'."
    # hash join filter
    elif (algo == "Hash Join" or algo == "Merge Join") and cond != None:
        return f"Filtered from the results of {algo} on '{cond}'."
    # index join
    elif (algo == "Index Join"):
        return f"Join using {algo}." \
               f"The {algo} used relation '{relation}' index key '{key}'."
    # nested loop join, hash join
    elif (algo == "Nested Loop") or (algo == "Hash Join") or (algo == "Merge Join"):
        return f"Join using {algo}."
    # table read using index scan
    elif (algo == "Index Scan") and (cond == None):
        return f"Read using {algo} with index key '{key}'."
    # table read using sequential scan
    elif (algo == "Seq Scan"):
        return f"Read using {algo}."
    else:
        print(info)
        return "Unresolved"

def match_plan(plan, sql_summary):
    res = {}
    missing_keys = []
    plan_list = list(plan)
    for key in sql_summary.keys():
        if key in plan_list:
            add_to_res(res, plan, sql_summary, key, key)
        elif (" date " in key):
            # edge case: date
            conds_with_date = []
            for e in plan_list:
                if re.match("(.*)(>=|<=|=|<|>|<>)(.*)(([0-9]){4})-(([0-9]){2})-(([0-9]){2})(.*)", e):
                    index = re.search("(>=|<=|=|<|>|<>)", e).end()
                    e_refined = e[:index + 1] + "date " + e[index + 1:]
                    conds_with_date.append((e, e_refined))
            # compute threshold
            threshold = len(re.split("(>=|<=|=|<|>|<>)", key)[0].strip())
            replacement_key = longest_common_substring(conds_with_date, key, threshold)
            if replacement_key != None:
                add_to_res(res, plan, sql_summary, key, replacement_key)
            else:
                missing_keys.append(key)
        else:
            missing_keys.append(key)

    keys_in_res = [e[0] for e in list(res)]
    missing_keys_cleaned = []
    for key in missing_keys:
        if "=" in key:
            args = key.split("=")
            for i in range(len(args)):
                args[i] = args[i].strip()
            s = f"{args[1]} = {args[0]}"
            if s in keys_in_res:
                continue
            else:
                missing_keys_cleaned.append(key)
        else:
            missing_keys_cleaned.append(key)

    # if len(missing_keys_cleaned) > 0:
    #     print("missing keys:")
    # for key in missing_keys_cleaned:
    #     print(key)
    # print(f"Number of keys missing, sql -> json = {len(missing_keys_cleaned)}")

    # for k, v in res.items():
    #     print(f"{k}:{v}")

    return res


def generate_annotation(sql):
    # summarize plans
    plans = summarize_plans()

    # parse sql
    sql_summary = parse_sql(sql)
    sql_options = {(k,tuple(v)):[] for k,v in sql_summary.items()}

    # match plans to sql
    plan_index = 0
    for plan in plans:
        match = match_plan(plan, sql_summary)
        for key in match.keys():
            sql_options[key].append(match[key])
        for key in sql_options.keys():
            if len(sql_options[key]) == plan_index:
                sql_options[key].append(None)

        for key in sql_options.keys():
            if (len(sql_options[key]) != (plan_index + 1)):
                raise Exception("Number of options in {sql_options[key]} does not match number of plans.")

        plan_index += 1
    # for item in sql_options.items():
    #     print("Test", item)
    # for item in generate_comparison(sql_options).items():
    #      print("compare", item)
    return generate_comparison(sql_options)

if __name__ == "__main__":
    f = open("Queries&Json\\q2.sql", "r")
    sql = f.read()
    print("test", generate_annotation(sql))
