import preprocessing
from Annotation import read_json
import json
from os import listdir
from os.path import isfile, join
import re

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


if __name__ == "__main__":
    test_read_dir = "test\\clean"
    json_filenames = [f for f in listdir(test_read_dir) if isfile(join(test_read_dir, f))]
    test_write_dir = "test"
    summary_filename = "summary.txt"
    summary_fullpath = join(test_write_dir, summary_filename)
    with open(summary_fullpath, "w") as outfile:
        pass
    for json_filename in json_filenames:
        duplicates = []
        json_fullpath = join(test_read_dir, json_filename)
        qep = read_json(json_fullpath)
        summary = {}
        try:
            cost = qep["Total Cost"]
            summarize_plan(qep, summary, 0, cost, None)
            for k,v in summary.items():
                summary[k] = [*set(v)]
        except KeyError:
            print(f"No 'Total Cost' key in {json_filename[:-5]}")
            summarize_plan(qep, summary, 0, -1)
        json_object = json.dumps(summary, indent=4)

        with open(summary_fullpath, "a") as outfile:
            outfile.write("\n")
            outfile.write(json_filename+"\n")
            outfile.write(json_object)
            print(json_filename + " appended to " + summary_fullpath)
            cleaned_duplicates = []
            for e in duplicates:
                if len(summary[e]) > 1:
                    cleaned_duplicates.append(e)
            outfile.write(f"\ntotal number of duplicates = {len(cleaned_duplicates)}\n")
            if len(cleaned_duplicates) > 0:
                outfile.write(f"duplicates:\n")
                for e in cleaned_duplicates:
                    outfile.write(f"{e}\n")
            outfile.write("="*80)



