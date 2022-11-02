"""
Some links:
https://gitlab.com/postgres/postgres/blob/master/src/include/nodes/plannodes.h
https://docs.gitlab.com/ee/development/understanding_explain_plans.html
"""

class FontFormat:
    """
        Class to define constants, which are used for formating the annotations
    """
    BOLD_START = "<b>"
    BOLD_END = "</b>"
    ITALIC_START = "<em>"
    ITALIC_END = "</em>"


def make_bold(string):
    """
        To make words bold
    """
    return FontFormat.BOLD_START + string + FontFormat.BOLD_END

def make_italic(string):
    """
         To italicise words
    """
    return FontFormat.ITALIC_START + string + FontFormat.ITALIC_END

def retrieve_aqp_annotation(query_plan:dict, comparison:dict):
    """
        Checks if any of the node type's query is in the comparison's keys,
        and returns the AQP comparison annotation. 

        Works because each annotation function is called only when it's relevant plan is retrieved.
    """
    query_keys = query_plan.keys()
    compare_keys = comparison.keys()

    for query_key in query_keys:
        if(query_plan[query_key] in compare_keys):
            return comparison[query_key]
        else:
            return None


def default_annotation(query_plan):
    """
         Default Annotation if none of the node types are identified in the annotation functions listed below
    """
    return f"The {make_italic(query_plan['Node Type'])} operation is performed."



def append_annotation(query_plan, comparison):
    """
        Generate annotation for append
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation combines the results of the child sub-operations. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation combines the results of the child sub-operations."



def func_scan_annotation(query_plan, comparison):
    """
        Generate annotation for function scan
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The function {make_italic(query_plan['Function Name'])} is executed and the set of records are returned. {aqp_annotation}"
    else:
        return f"The function {make_italic(query_plan['Function Name'])} is executed and the set of records are returned."



def limit_annotation(query_plan, comparison):
    """
        Generate annotation for limit
    """
    
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation takes {make_bold(str(query_plan['Plan Rows']))} records and disregard the remaining records. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation takes {make_bold(str(query_plan['Plan Rows']))} records and disregard the remaining records."



def subquery_scan_annotation(query_plan, comparison):
    """
        Generate annotation for subquery scan
    """
    
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation reads on the results from a subquery. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation reads on the results from a subquery."


def value_scan_annotation(query_plan, comparison):
    """
        Generate annotation for value scan
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation reads the given constant values from the query. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation reads the given constant values from the query."


def materialize_annotation(query_plan, comparison):
    """
        Generate annotation for materialize
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation stores the results of child operations in memory for faster access by parent operations. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation stores the results of child operations in memory for faster access by parent operations."

def nl_join_annotation(query_plan, comparison):
    """
        Generate annotation for Nested Loop Join
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation implements a join or lookup where the first child node is run once, then for every row it produces, its partner is looked up in the second node. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation implements a join or lookup where the first child node is run once, then for every row it produces, its partner is looked up in the second node."

def unique_annotation(query_plan, comparison):
    """
        Generates annotation for unique which removes duplicates
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation removes duplicates from a sorted result set. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation removes duplicates from a sorted result set."
    


def hash_func_annotation(query_plan, comparison):
    """
        Generates annotation for hash function
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} function hashes the query rows into memory, for use by its parent operation. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} function hashes the query rows into memory, for use by its parent operation."

def gather_merge_annotation(query_plan, comparison):
    """
        Generates annotation for gather merge
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    if aqp_annotation is not None:
        return f"The {make_italic(query_plan['Node Type'])} operation combines the output table from sub-operations by executing the operation in parallel. {aqp_annotation}"
    else:
        return f"The {make_italic(query_plan['Node Type'])} operation combines the output table from sub-operations by executing the operation in parallel."


def aggregate_annotation(query_plan, comparison):

    """
        Generates annotation for aggregate
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    # Obtain strategy from query plan
    strategy = query_plan["Strategy"]

    # Sorted strategy
    if strategy == "Sorted":
        result = f"The {make_italic(query_plan['Node Type'])} operation sorts the tuples based on their keys, "

        # Obtain the attributes that the records are grouped by
        if "Group Key" in query_plan:
            result += f" where the tuples are {make_bold('aggregated')} by the following keys: "

            for key in query_plan["Group Key"]:
                result += make_bold(key) + ","

            result = result[:-1]
            result += "."

        # Get the filtered attribute and remove unnecessary strings
        if "Filter" in query_plan:
            result += f" where the tuples are filtered by {make_bold(query_plan['Filter'].replace('::text', ''))}."

        if aqp_annotation is not None:
            return f'{result}. {aqp_annotation}'
        else:
            return result

    # Hashed strategy
    elif strategy == "Hashed":
        result = f"The {make_italic(query_plan['Node Type'])} operation {make_bold('hashes')} all rows based on these key(s): "

        # Obtain the attributes that the records are grouped by
        for key in query_plan["Group Key"]:

            # Remove unnecessary strings
            result += make_bold(key.replace("::text", "")) + ", "

        result += f"which are then {make_bold('aggregated')} into a bucket given by the hashed key."

        if aqp_annotation is not None:
            return f'{result}. {aqp_annotation}'
        else:
            return result

    # Plain strategy
    elif strategy == "Plain":
        if aqp_annotation is not None:
            return f"The result is {make_bold('aggregated')} with the {make_italic(query_plan['Node Type'])} operation. {aqp_annotation}"
        else:
            return f"The result is {make_bold('aggregated')} with the {make_italic(query_plan['Node Type'])} operation."

    # If the strategy value is neither of the above
    else:
        raise ValueError("Annotation does not work: " + strategy)



def cte_scan_annotation(query_plan, comparison):
    """
        Generates annotation for CTE scan
    """
    result = f"The {make_italic(query_plan['Node Type'])} operation is performed on the table {make_bold(str(query_plan['CTE Name']))} which the results are stored in memory for use later. "

    # Get the index condition and remove unnecessary strings
    if "Index Cond" in query_plan:
        result += " The condition(s) are " + make_bold(
            query_plan["Index Cond"].replace("::text", "")
        )

    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += " and then further filtered by " + make_bold(
            query_plan["Filter"].replace("::text", "")
        )

    result += "."
    return result


def group_annotation(query_plan, comparison):
    """
        Generates annotation for group
    """

    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    result = f"The {make_italic(query_plan['Node Type'])} operation groups the results from the previous operation together with the following keys: "

    # Obtain the attributes that the records are grouped by
    for i, key in enumerate(query_plan["Group Key"]):

        # Remove unnecessary strings
        result += make_bold(key.replace("::text", ""))

        # Condition checks to put comma or full stop after attribute
        if i == len(query_plan["Group Key"]) - 1:
            result += "."
        else:
            result += ", "

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result


def index_scan_annotation(query_plan, comparison):
    """
        Generates annotation for index scan
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    result = f"The {make_italic(query_plan['Node Type'])} operation scans the index for rows"

    # Get the index condition and remove unnecessary strings
    if "Index Cond" in query_plan:
        result += " which match the following conditions: " + make_bold(
            query_plan["Index Cond"].replace("::text", "")
        )

    result += f", and then reads the records from the table that match the conditions."

    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += f" The result is further filtered by {make_bold(query_plan['Filter'].replace('::text', ''))}."

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result


def index_only_scan_annotation(query_plan, comparison):
    """
        Generates annotation for index-only scan
    """

    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    result = f"The {make_italic(query_plan['Node Type'])} function is conducted using an index table {make_bold(query_plan['Index Name'])}"

    # Get the index condition and remove unnecessary strings
    if "Index Cond" in query_plan:
        result += " with condition(s) " + make_bold(
            query_plan["Index Cond"].replace("::text", "")
        )

    result += ". The records obtained from the index table is returned as the result."

    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += f" The result is further filtered by {make_bold(query_plan['Filter'].replace('::text', ''))}."

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result


def merge_join_annotation(query_plan, comparison):
    """
        Generates annotation for Merge Join
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    result = f"The {make_italic(query_plan['Node Type'])} operation joins the results that have been sorted on join keys from sub-operations"

    # Get the merge condition and remove unnecessary strings
    if "Merge Cond" in query_plan:
        result += " with condition " + make_bold(
            query_plan["Merge Cond"].replace("::text", "")
        )

    # Check the join type
    if "Join Type" == "Semi":
        result += " but only the records from the left relation is returned as the result"

    result += "."

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result


def set_operation_annotation(query_plan, comparison):
    """
        Generates annotation for setOp
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    result = f"The {make_italic(query_plan['Node Type'])} operation finds the "

    # Get the command
    command = make_bold(str(query_plan["Command"]))

    # SQL 'Except' command
    if command == "Except" or command == "Except All":
        result += "differences"

    # SQL 'Intercept' command
    else:
        result += "similarities"

    result += " in records between the two previously scanned tables."

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result

def sequential_scan_annotation(query_plan, comparison):
    """
        Generates annotation for sequential scan
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    result = f"The {make_italic(query_plan['Node Type'])} operation performs a scan on relation "

    # Get the relation name from query plan
    if "Relation Name" in query_plan:
        result += make_bold(query_plan["Relation Name"])

    # Get the alias from query plan if it is not the same as relation name
    if "Alias" in query_plan:
        if query_plan["Relation Name"] != query_plan["Alias"]:
            result += f" with an alias of {make_bold(query_plan['Alias'])}"

    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += f" and then filtered with the condition {make_bold(query_plan['Filter'].replace('::text', ''))}"

    result += "."

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result



def sort_annotation(query_plan, comparison):
    """
        Generates annotation for sort
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)

    result = (
        f"The {make_italic(query_plan['Node Type'])} operation sorts the rows "
    )

    # If the specified sort key is DESC
    if "DESC" in query_plan["Sort Key"]:
        result += (
            make_bold(str(query_plan["Sort Key"].replace("DESC", "")))
            + " in descending order"
        )

    # If the specified sort key is INC
    elif "INC" in query_plan["Sort Key"]:
        result += (
            make_bold(str(query_plan["Sort Key"].replace("INC", "")))
            + " in increasing order"
        )

    # Otherwise specify the attribute
    else:
        result += f"based on {make_bold(str(query_plan['Sort Key']))}"

    result += "."

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result

def hash_join_annotation(query_plan, comparison):
    """
        Generates annotation for hash join
    """
    aqp_annotation = retrieve_aqp_annotation(query_plan, comparison)
    
    result = f"The {make_italic(query_plan['Node Type'])} operation joins the results from the previous operations using a hash {make_bold(query_plan['Join Type'])} {make_bold('Join')}"

    # Get the hash condition and remove unnecessary strings
    if "Hash Cond" in query_plan:
        result += f" on the condition: {make_bold(query_plan['Hash Cond'].replace('::text', ''))}"

    result += "."

    if aqp_annotation is not None:
        return f"{result}. {aqp_annotation}"
    else:
        return result


class Annotation(object):
    """
    List of possible node types based on this source:
    https://www.pgmustard.com/docs/explain
    """

    annotation_dict = {
        "Aggregate": aggregate_annotation,
        "Append": append_annotation,
        "CTE Scan": cte_scan_annotation,
        "Function Scan": func_scan_annotation,
        "Group": group_annotation,
        "Index Scan": index_scan_annotation,
        "Index Only Scan": index_only_scan_annotation,
        "Limit": limit_annotation,
        "Materialize": materialize_annotation,
        "Unique": unique_annotation,
        "Merge Join": merge_join_annotation,
        "SetOp": set_operation_annotation,
        "Subquery Scan": subquery_scan_annotation,
        "Values Scan": value_scan_annotation,
        "Seq Scan": sequential_scan_annotation,
        "Nested Loop": nl_join_annotation,
        "Sort": sort_annotation,
        "Hash": hash_func_annotation,
        "Hash Join": hash_join_annotation,
        "Gather Merge": gather_merge_annotation,
    }


if __name__ == "__main__":

    # For testing only
    query_plan = {"Node Type": "Values Scan"}
    annotation = Annotation().annotation_dict.get(
        query_plan["Node Type"], default_annotation(query_plan)
    )(query_plan)
    print(annotation)