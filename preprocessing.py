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
        query_plan_dict: dict = self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST, DEFAULT_RAND_PAGE_COST)
        qep_plan = self.retrieve_plans(query_plan_dict)

        # First AQP
        query_plan_dict2: dict = self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST + 2,
                                                    DEFAULT_RAND_PAGE_COST + 2)
        aqp_plan = self.retrieve_plans(query_plan_dict2)
        comparison_dict = self.compare_query(qep_plan, aqp_plan)

        # Second AQP
        query_plan_dict3: dict = self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST + 3,
                                                    DEFAULT_RAND_PAGE_COST - 4)
        aqp_plan2 = self.retrieve_plans(query_plan_dict3)
        comparison_dict = self.compare_query(qep_plan, aqp_plan)
        print(comparison_dict)

        return QueryPlan(query_plan_dict, comparison_dict)

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
            dict: results of the explain and what plans were selected
        """
        self.change_parameters(seq_cost, rand_cost)
        self.cursor.execute(query)
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        return query_plan_dict

    def retrieve_plans(self, query_dict: dict, index=0, boom=0) -> list:
        """
            Gets all plans within the current query plan
            Args:
                query_dict (dict): dictionary that contains the query plan (obtained from executing EXPLAIN query)
                index (int): index of tree position (default = 0)
                boom (int): a check in cases where it could go wrong
            Returns:
                list: a list of tuples that contain information on the entire plan
        """

        # Prevents infinite loop (recursion)
        if boom > 100000:
            self.conn.rollback()
            raise Exception("Exception encountered, infinite recursion detected while retrieving plans")

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

        # Recursively access each node in the plan to retrieve the plans within
        plans_list = []
        if query_dict.get('Plans') is not None:
            for inner_dict in query_dict['Plans']:
                new_plan = self.retrieve_plans(inner_dict, index + 1, boom + 1)
                for tuple_plan in new_plan:
                    # Append to list once there are plans available
                    plans_list.append(tuple_plan)

        # Select and place the values into a tuple (indexed by tree level)
        plans_list.append((node, value, cost, index))
        return plans_list

    def compare_query(self, qep, aqp) -> dict:
        qep_sort = sorted(qep, key=lambda x: x[-1])
        aqp_sort = sorted(aqp, key=lambda x: x[-1])

        compare_dict = {}

        for qep_item in qep_sort:
            compare_string = None
            for aqp_item in aqp_sort:
                # If index is the same then compare
                if qep_item[-1] == aqp_item[-1]:
                    print(qep_item)
                    print(aqp_item)
                    compare_string = self.compare_item(qep_item, aqp_item)
                    print(compare_string)
            # Check if there is any string to input
            if compare_string is not None:
                values = list(qep_item[1].values())
                # Use the condition as the hash (key)
                if len(values) > 0:
                    hash_value = values[0]
                    if type(values[0]) is list:
                        hash_value = values[0][0]
                    # Place into dictionary
                    compare_dict[hash_value] = compare_string
                    continue

        return compare_dict

    def compare_item(self, qep_item, aqp_item):
        # Check whether it is of the same type (Scan / Join)
        if self.compare_type(qep_item[0], aqp_item[0]):
            # If cost is the same, skip and ignore
            if qep_item[2] == aqp_item[2]:
                return None
            else:
                # Check that the keys are different
                keys = list(aqp_item[1].keys())
                aqp_values = list(aqp_item[1].values())
                qep_values = list(qep_item[1].values())
                aqp_values.sort()
                qep_values.sort()
                if qep_values == aqp_values:
                    return None
                diff = aqp_item[2] - qep_item[2]
                if diff < 0:
                    return None
                if len(aqp_values) != 0:
                    return f"AQP chooses to do {keys[0]} on {aqp_values[0]} that increases cost by {diff}"

        return None

    def compare_type(self, qep: str, aqp: str) -> bool:
        """
            Method to check whether it needs to compare the values (Scan / Join)
            Args:
                qep (str): QEP string node type
                aqp (str): AQP string node type
            Returns:
                bool: True if the types are equal else false
        """
        qep_string = qep.split(" ")
        aqp_string = aqp.split(" ")

        if len(qep_string) > 1 and len(aqp_string) > 1:
            print(qep_string[1])
            print(aqp_string[1])
            # If the (Scan/ Join/ Loop) conditions match return true
            if qep_string[1] == aqp_string[1]:
                return True
            elif (qep_string[1] == "Loop" and aqp_string[1] == "Join") or (qep_string[1] == "Join" and aqp_string[1] == "Loop"):
                return True
        return False


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
