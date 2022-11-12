import preprocessing
from Annotation import read_json
import json
import re

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
    cond = cond.split("AND")
    for i in range(len(cond)):
        cond[i] = cond[i].split("OR")
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
    print(raw)
    raw = re.sub(r'([a-z].*\.)?(.*) (>=|<=|=|<|>|<>) ([a-z].*\.)?(.*)', r'\2 \3 \5', raw)  # relation name must start with a-z, to prevent conflict with decimal values
    raw = re.sub(r"([a-z].*\.)?(.*) (>=|<=|=|<|>|<>) (.*)(::.*)", r'\2 \3 \4', raw)
    raw = re.sub(r"([a-z].*\.)?(.*)(::.*) (>=|<=|=|<|>|<>) (.*)", r'\2 \4 \5', raw)
    raw = re.sub(r"([a-z].*\.)?(.*)(::.*) (>=|<=|=|<|>|<>) (.*)(::.*)", r'\2 \4 \5', raw)
    raw = re.sub(r"([a-z].*\.)?(.*)(::.*) ~~ ('.*')(::.*)", r'\2 like \4', raw)
    raw = re.sub(r"([a-z].*\.)?(.*)(::.*) !~~ ('.*')(::.*)", r'\2 not like \4', raw)
    raw = re.sub(r"([a-z].*\.)?(.*) ANY '({.*})'(::.*)", r'\3', raw)
    raw = re.sub(r"([a-z].*\.)?(.*) ANY '({.*})'", r'\3', raw)
    print(raw)
    return raw

if __name__ == "__main__":
    start = preprocessing.connect()
    conn = start[0]
    tables = start[1]
    query_string = preprocessing.query_asker()
    preprocessing.QEP_Generator(conn, query_string)
    qep = read_json("QEP.json")[0][0][0]["Plan"]
    clean_plan(qep)
    json_object = json.dumps(qep, indent = 4)
    with open("QEP_clean.json", "w") as outfile:
        outfile.write(json_object)
        print("QEP is generated in QEP_clean.json")

