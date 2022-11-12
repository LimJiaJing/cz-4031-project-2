import preprocessing
from Annotation import read_json
import json
import re
from os import listdir
from os.path import isfile, join

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




if __name__ == "__main__":
    start = preprocessing.connect()
    conn = start[0]
    tables = start[1]
    test_read_dir = "test\\raw"
    json_filenames = [f for f in listdir(test_read_dir) if isfile(join(test_read_dir, f))]
    test_write_dir = "test\\clean"
    for json_filename in json_filenames:
        json_fullpath = join(test_read_dir, json_filename)
        qep = read_json(json_fullpath)[0][0][0]["Plan"]
        clean_plan(qep)
        json_object = json.dumps(qep, indent = 4)
        clean_filename = "clean_" + json_filename
        clean_fullpath = join(test_write_dir, clean_filename)
        with open(clean_fullpath, "w") as outfile:
            outfile.write(json_object)
            print(json_filename + " QEP is generated in " + clean_fullpath)

