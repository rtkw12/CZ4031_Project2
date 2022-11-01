from psycopg2 import connect, sql
from functools import wraps
from interface import *

DEFAULT_SEQ_PAGE_COST = 1.0
DEFAULT_RAND_PAGE_COST = 4.0

""" cost = ( #blocks * seq_page_cost ) + ( #records * cpu_tuple_cost ) + ( #records * cpu_filter_cost )"""

"""
Check if the query is valid.
Args:
    query (string): Query string that was entered by the user.
Returns:
    dict: Output dict consisting of error status and error message.
"""


def validate(query):
    output = {"query": query, "error": False, "error_message": ""}

    if not len(query):
        output["error"] = True
        output["error_message"] = "Query is empty."

    if not query_processor.query_valid(query):
        output["error"] = True
        output["error_message"] = "Query is invalid."
        return output

    return output


class SimplifiedPlan:
    def __init__(self, node: str, condition: dict, cost: float):
        self.node = node
        self.condition = condition
        self.cost = cost

    def compare_type(self, plan):
        """
        Compares the type of node (Index or Seq / Hash or Nested Loop)
        Args:
            plan (SimplifiedPlan): plan to compare to
        Returns:
            bool: whether it is of different type
        """

        node_string = self.node.split(" ")
        aqp_string = plan.node.split(" ")
        if len(node_string) > 0 and len(aqp_string) > 0:
            # If the (Index and Seq/ Hash Join and Nested Loop) conditions match return false
            if node_string[0] != aqp_string[0]:
                return True
        return False

    def compare_node(self, plan):
        """
        Method to check whether it needs to compare the values (Scan / Join)
        Args:
            plan (SimplifiedPlan): the plan that is being compared to
        Returns:
            bool: True if the types are equal else false
        """
        node_string = self.node.split(" ")
        aqp_string = plan.node.split(" ")

        if len(node_string) > 1 and len(aqp_string) > 1:
            print("QEP:", self.condition, ": ", self.node)
            print("AQP:", plan.condition, ": ", plan.node)
            # If the (Scan/ Join/ Loop) conditions match return true
            if node_string[1] == aqp_string[1]:
                return True
            elif (node_string[1] == "Loop" and aqp_string[1] == "Join") or (
                    node_string[1] == "Join" and aqp_string[1] == "Loop"):
                return True
        return False

    def cost_difference(self, cost):
        return self.cost - cost


class QueryProcessor:
    def __init__(self, db_config):
        self.conn = self.start_db_connection(db_config)
        self.cursor = self.conn.cursor()

    def start_db_connection(self, db_config):
        """
            Establishes connection with PostgreSQL database.
            Returns:
                connection: Connection to the database.
        """
        return connect(
            dbname=db_config.POSTGRES_DBNAME,
            user=db_config.POSTGRES_USERNAME,
            password=db_config.POSTGRES_PASSWORD,
            host=db_config.POSTGRES_HOST,
            port=db_config.POSTGRES_PORT,
        )

    def single_transaction(func):
        """
            Decorator to create cursor each time the function is called.
            Args:
                func (function): Function to be wrapped
            Returns:
                function: Wrapped function
        """

        @wraps(func)
        def inner_func(self, *args, **kwargs):
            try:
                self.cursor = self.conn.cursor()
                ans = func(self, *args, **kwargs)
                self.conn.commit()
                return ans
            except Exception as error:
                print(f"Exception encountered, rolling back: {error}")
                self.conn.rollback()

        return inner_func

    def stop_db_connection(self):
        self.conn.close()
        self.cursor.close()

    def change_parameters(self, seq_page, rand_page):
        self.cursor.execute("SET seq_page_cost TO " + str(seq_page))
        self.cursor.execute("SET random_page_cost TO " + str(rand_page))

    @single_transaction
    def explain(self, query: str) -> QueryPlan:
        """
            Gets execution plan of statement from PostgreSQL
            Args:
                query (str): Query string that was entered by the user.
            Returns:
                QueryPlan: An object consisting of all the necessary information in the QEP
                to be displayed to the user.
        """
        query_explainer = "EXPLAIN (FORMAT JSON, SETTINGS ON) " + query
        # Do default settings first
        qep_plan: dict = self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST, DEFAULT_RAND_PAGE_COST)

        # First AQP
        aqp_plan1: dict = self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST + 10,
                                             DEFAULT_RAND_PAGE_COST + 2)
        comparison1 = self.scan_tree(qep_plan, aqp_plan1)

        # Second AQP
        aqp_plan2: dict = self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST + 5,
                                             DEFAULT_RAND_PAGE_COST)

        # Do the comparisons
        comparison2 = self.scan_tree(qep_plan, aqp_plan2)

        # Combine the dictionaries
        comparison_dict = {}
        comparison_dict = self.add_comparisons(comparison_dict, comparison1)
        comparison_dict = self.add_comparisons(comparison_dict, comparison2)

        print(comparison_dict)
        return QueryPlan(qep_plan, comparison_dict)

    @single_transaction
    def query_valid(self, query: str):
        """
           Tries executing query to validate the query
           Args:
               query (str): Query string
           Returns:
               bool: Whether the query is valid.
        """

        self.cursor.execute(query)
        try:
            self.cursor.fetchone()
        except:
            return False
        return True

    def execute_query(self, query, seq_cost, rand_cost) -> dict:
        """
        Executes query with different cost plans
        Args:
            query (str): Query string (with the EXPLAIN statement)
            seq_cost (float): sequential scan cost of database
            rand_cost (float): random scan cost of database
        Returns:
            dict: results of the EXPLAIN function and what plans were selected
        """
        self.change_parameters(seq_cost, rand_cost)
        self.cursor.execute(query)
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        return query_plan_dict

    def scan_tree(self, qep: dict, aqp: dict) -> dict:
        """
        Scan the entire tree to find the differences
        Args:
            qep: the best Query Execution Plan
            aqp: a Alternate Query Plan

        Returns:
            dict: comparisons that were indexed
        """
        comparisons = {}

        # Check if qep and aqp has "plans"
        qep_plans = qep.get("Plans")
        aqp_plans = aqp.get("Plans")

        if qep_plans is not None and aqp_plans is not None:
            # Scan plans
            for qep_plan, aqp_plan in zip(qep_plans, aqp_plans):
                result = self.scan_tree(qep_plan, aqp_plan)
                # Update any results if any
                if len(result) > 0:
                    comparisons.update(result)

        # Scan through current plan to check whether there are differences
        comparison_string, condition = self.compare_query_plan(qep, aqp)

        if comparison_string is not None:
            hash_value = " "
            # Use the condition as the hash (key)
            if len(condition) > 0:
                if type(condition) is dict:
                    dict_values = list(condition.values())
                    hash_value = dict_values[0]
                elif type(condition) is list:
                    hash_value = condition[0][0]
            # Place into dictionary
            comparisons[hash_value] = [comparison_string]

        return comparisons

    def add_comparisons(self, comparison_dict: dict, comparison: dict) -> dict:
        """
        Adds the comparison dictionary and compares whether to add to a list
        Args:
            comparison_dict: the dictionary that needs to be added to
            comparison: the dictionary to query from
        Returns:
            dict: the updated comparison dictionary
        """
        for key in list(comparison.keys()):
            if comparison_dict.get(key) is not None:
                # Extend the list of comparisons at the condition if there are more found
                comparison_dict[key].extend(comparison[key])
            else:
                comparison_dict[key] = comparison[key]
        return comparison_dict

    def retrieve_plans(self, query_dict: dict) -> SimplifiedPlan:
        """
            Gets all plans within the current query plan
            Args:
                query_dict (dict): dictionary that contains the query plan (obtained from executing EXPLAIN query)
            Returns:
                list: a list of tuples that contain information on the entire plan
        """
        node = query_dict['Node Type']
        cost = query_dict['Total Cost']
        value = {}

        # Retrieve all data related to conditions
        if query_dict.get('Filter') is not None:
            value['Filter'] = query_dict['Filter']
        elif query_dict.get('Sort Key') is not None:
            value['Sort Key'] = query_dict['Sort Key']
        elif query_dict.get('Group Key') is not None:
            value['Group Key'] = query_dict['Group Key']
        elif query_dict.get('Hash Cond') is not None:
            value['Hash Cond'] = query_dict['Hash Cond']
        elif query_dict.get('Index Cond') is not None:
            value['Index Cond'] = query_dict['Index Cond']

        return SimplifiedPlan(node, value, cost)

    def compare_query_plan(self, qep: dict, aqp: dict):
        # Retrieve the current plan and place into a class
        qep_simple = self.retrieve_plans(qep)
        aqp_simple = self.retrieve_plans(aqp)

        # Compare the results and check whether they are the same
        return self.compare_item(qep_simple, aqp_simple), qep_simple.condition

    def compare_item(self, qep_item: SimplifiedPlan, aqp_item: SimplifiedPlan):
        # Check whether it is of the same type (Scan / Join)
        if qep_item.compare_node(aqp_item):
            # If cost is the same, skip and ignore
            diff = aqp_item.cost_difference(qep_item.cost)
            if diff == 0:
                return None
            else:
                # Check if difference is greater
                if diff < 0:
                    return None
                keys = list(aqp_item.condition.keys())
                aqp_values = list(aqp_item.condition.values())
                qep_values = list(qep_item.condition.values())
                aqp_values.sort()
                qep_values.sort()
                print(diff)
                # Check that the Node Type are the same
                if qep_item.compare_type(aqp_item):
                    if len(aqp_values) > 0:
                        return f"AQP chooses to do {aqp_item.node} on {aqp_values[0]} that increases cost by {diff}"
                    if len(qep_values) > 0:
                        return f"AQP chooses to do {aqp_item.node} on {qep_values[0]} that increases cost by {diff}"

        return None


def __main__():
    query_processor.explain(
        "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " +
        "FROM customer, orders, lineitem " +
        "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < date '1995-03-15' AND l_shipdate > date '1995-03-15' " +
        "GROUP BY l_orderkey, o_orderdate, o_shippriority " +
        "ORDER BY revenue desc, o_orderdate LIMIT 20")
    return


query_config = Config()
query_processor = QueryProcessor(query_config)
__main__()
