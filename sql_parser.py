from collections import defaultdict
import sqlparse
import re


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
        if (re.search(r'FROM', query_list[i]) and not re.search(r'\(|\)', query_list[i])) or re.search(r'WHERE', query_list[i]) or (not is_unwanted_line and not re.search(r'SELECT|ORDER BY', query_list[i])):
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
    cleaned_key = remove_unwanted_keywords(query_list[i-1])
    v = sql_to_level_mapping[cleaned_key]
    del sql_to_level_mapping[cleaned_key]
    new_cleaned_key = cleaned_key + " _"
    sql_to_level_mapping[new_cleaned_key] = v
    return new_cleaned_key


def modify_line_with_in_keyword(start, query_list):
    line = query_list[start]
    line_to_skip = 0
    for i in range(start+1, len(query_list)):
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
                format_string = '\\"'
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