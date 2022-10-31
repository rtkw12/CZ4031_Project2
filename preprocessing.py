from psycopg2 import connect, sql
from functools import wraps
from interface import *
from annotation import *
from project import config

DEFAULT_SEQ_PAGE_COST = 1.0
DEFAULT_RAND_PAGE_COST = 4.0


""" cost = ( #blocks * seq_page_cost ) + ( #records * cpu_tuple_cost ) + ( #records * cpu_filter_cost )"""
"""Check if the query is valid.
    Args:
        query (string): Query string that was entered by the user.
    Returns:
        dict: Output dict consisting of error status and error message.
    """
"""def validate(query):
    
    output = {"query": query, "error": False, "error_message": ""}

    if not len(query):
        output["error"] = True
        output["error_message"] = "Query is empty."

    if not query_processor.query_valid(query):
        output["error"] = True
        output["error_message"] = "Query is invalid."
        return output

    return output
"""

class QueryProcessor:
    def __init__(self, db_config):
        self.conn = self.start_db_connection(db_config)
        self.cursor = self.conn.cursor()

    def start_db_connection(self, db_config):
        """Establishes connection with PostgreSQL database.
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

    def wrap_single_transaction(func):
        """Decorator to create cursor each time the function is called.
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

    @wrap_single_transaction
    def explain(self, query: str) -> QueryPlan:
        """Retrives execution plan of statement from PostgreSQL
        Args:
            query (str): Query string that was entered by the user.
        Returns:
            QueryPlan: An object consisting of all the necessary information in the QEP
            to be displayed to the user.
        """
        query_explainer = "EXPLAIN (FORMAT JSON, SETTINGS ON) " + query
        # Do default settings first
        self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST, DEFAULT_RAND_PAGE_COST)
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        qep_plan = self.retrieve_plans(query_plan_dict)

        # First AQP
        self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST - 1, DEFAULT_RAND_PAGE_COST - 1)
        plan = self.cursor.fetchall()
        query_plan_dict2: dict = plan[0][0][0]["Plan"]
        aqp_plan = self.retrieve_plans(query_plan_dict2)
        test = self.compare_query(qep_plan, aqp_plan)
        print(test)

        # Second AQP
        self.execute_query(query_explainer, DEFAULT_SEQ_PAGE_COST + 2, DEFAULT_RAND_PAGE_COST + 2)
        plan = self.cursor.fetchall()
        query_plan_dict3: dict = plan[0][0][0]["Plan"]
        aqp_plan2 = self.retrieve_plans(query_plan_dict3)

        return QueryPlan(query_plan_dict)

    def retrieve_plans(self, query_dict : dict, index=0, boom=0):
        if boom > 100000:
            self.conn.rollback()
            raise Exception("Exception encountered, infinite recursion detected while retrieving plans")

        node = query_dict['Node Type']
        cost = query_dict['Total Cost']
        value = {}
        if query_dict.get('Filter') is not None:
            value['Filter'] = query_dict['Filter']
        elif query_dict.get('Sort Key') is not None:
            value['Sort Key'] = query_dict['Sort Key']
        elif query_dict.get('Group Key') is not None:
            value['Group Key'] = query_dict['Group Key']

        plans_list = []
        if query_dict.get('Plans') is not None:
            for inner_dict in query_dict['Plans']:
                new_plan = self.retrieve_plans(inner_dict, index + 1, boom + 1)
                for tuple_plan in new_plan:
                    plans_list.append(tuple_plan)

        plans_list.append((node, value, cost, index))
        return plans_list

    def compare_query(self, qep, aqp):
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
                    break
            if compare_string is not None:
                values = list(qep_item[1].values())
                if len(values) > 0:
                    compare_dict[values[0]] = compare_string
                    continue

        return compare_dict

    def compare_item(self, qep_item, aqp_item):
        if qep_item[0] == aqp_item[0]:
            if qep_item[2] == aqp_item[2]:
                return None
            else:
                keys = list(aqp_item[1].keys())
                values = list(aqp_item[1].values())
                diff = aqp_item[2] - qep_item[2]
                if diff < 0:
                    return None
                if len(values) != 0:
                    return f"AQP chooses to do {keys[0]} on {values[0]} that increases cost by {diff}"

        return None

    def execute_query(self, query, seq_cost, rand_cost):
        self.change_parameters(seq_cost, rand_cost)
        self.cursor.execute(query)
        return

    @wrap_single_transaction
    def query_valid(self, query: str):
        """Validate query by trying to fetch a single row from the result set.
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


#query_processor = QueryProcessor()

def __main__():
    query_config = config()

    query_processor = QueryProcessor(query_config)
    query_processor.explain("SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " +
                            "FROM customer, orders, lineitem " +
                            "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < date '1995-03-15' AND l_shipdaate > date '1995-03-15' " +
                            "GROUP BY l_orderkey, o_orderdate, o_shippriority " +
                            "ORDER BY revenue desc, o_orderdate LIMIT 20")
    return

__main__()