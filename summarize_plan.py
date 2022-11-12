import preprocessing
from Annotation import read_json
import json
import re

join_conds = [
    "Hash Cond",
    "Merge Cond",
    "Index Cond",
    "Join Filter",
]

def summarize_plan(plan, summary):
    if "Plans" in list(plan):
        plans = plan["Plans"]
        for subplan in plans:
            summarize_plan(subplan, summary)
    nodes = []
    for key in list(plan):
        if key in join_conds + ["Filter"]:
            nodes.append(plan[key])
    for node in nodes:
        if node not in list(summary):
            summary[node] = [(plan["Node Type"], plan["Total Cost"]),] # (algorithm, cost)
        else:
            summary[node].append((plan["Node Type"], plan["Total Cost"]))


if __name__ == "__main__":
    qep = read_json("QEP_Clean.json")
    summary = {}
    summarize_plan(qep, summary)
    for key, value in summary.items():
        print(key, value)