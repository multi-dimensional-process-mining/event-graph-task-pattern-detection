from neo4j import GraphDatabase
from SubgraphVisualizer import SubgraphVisualizer
import pandas as pd
from tabulate import tabulate


class GraphExplorer:

    def __init__(self, graph, password, name_data_set, entity_labels, action_lifecycle_labels, timestamp_label,
                 print_duration=False, use_abbreviated_event_names=False):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.graph = graph
        self.entity_labels = entity_labels
        self.print_duration = print_duration
        self.use_abbreviated_event_names = use_abbreviated_event_names
        self.svg = SubgraphVisualizer(graph, password, name_data_set, entity_labels, action_lifecycle_labels,
                                      timestamp_label)

    def explore_cases(self):
        explore = "y"
        while explore == "y":
            case_ids = input("Specify the case ids of the cases to explore, separated by a comma,"
                             "e.g., \"case1,case2,case3\": ").split(",")
            self.svg.visualize_graph_from_cases(case_ids, self.print_duration,
                                                self.use_abbreviated_event_names)
            explore = input("Explore more case executions (y/n)? ")

    def explore_patterns(self):
        explore = "y"
        while explore == "y":
            with self.driver.session() as session:
                execution_pattern = input("Specify which execution pattern to explore (1, 2, 3, 4, 7p or 8p): ")
                while execution_pattern not in ["1", "2", "3", "4", "7p", "8p"]:
                    execution_pattern = input("Not a valid execution pattern, please specify 1, 2, 3, 4, 7p or 8p: ")
                action_seq_description = ""
                if execution_pattern in ["1", "4"]:
                    df_execution_patterns = session.read_transaction(get_elementary_execution_patterns,
                                                                     int(execution_pattern))
                elif execution_pattern == "2":
                    df_execution_patterns = session.read_transaction(get_interrupted_execution_patterns,
                                                                     self.entity_labels[1][0], "rID")
                elif execution_pattern == "3":
                    df_execution_patterns = session.read_transaction(get_interrupted_execution_patterns,
                                                                     self.entity_labels[0][0], "cID")
                elif execution_pattern in ["7p", "8p"]:
                    df_batch_action_sequences = session.read_transaction(get_batch_action_sequences,
                                                                         self.entity_labels[0][0], execution_pattern)
                    print(tabulate(df_batch_action_sequences.head(20), headers='keys', tablefmt='fancy_grid'))
                    action_seq_index = int(input("\nSelect specific batch action sequence (by index): "))
                    action_sequence = df_batch_action_sequences.loc[action_seq_index]['action_seq']
                    action_seq_description = f"actseq{action_seq_index}_"
                    print(f"Chosen action sequence: {action_sequence}")
                    df_execution_patterns = session.read_transaction(get_batch_execution_patterns,
                                                                     self.entity_labels[0][0], action_sequence)
                print(tabulate(df_execution_patterns.head(20), headers='keys', tablefmt='fancy_grid'))

                execution_index = int(input("\nSelect specific execution (by index): "))
                execution_path = df_execution_patterns.loc[execution_index]['path']
                print(f"Chosen execution path: {execution_path}")

                if execution_pattern in ["1", "4"]:
                    df_execution_instances = session.read_transaction(get_instances_of_elementary_execution,
                                                                      execution_path)
                elif execution_pattern == "2":
                    df_execution_instances = session.read_transaction(get_instances_of_interrupted_execution,
                                                                      self.entity_labels[1][0], "rID", execution_path)
                elif execution_pattern == "3":
                    df_execution_instances = session.read_transaction(get_instances_of_interrupted_execution,
                                                                      self.entity_labels[0][0], "cID", execution_path)
                elif execution_pattern in ["7p", "8p"]:
                    df_execution_instances = session.read_transaction(get_instances_of_batch_executions,
                                                                      self.entity_labels[0][0], execution_path)

                print(tabulate(df_execution_instances[['resource', 'duration', 'case']].head(20), headers='keys',
                               tablefmt='fancy_grid'))

                ti_index = int(input("\nSelect instance to visualize (by index): "))
                ti_id_path = df_execution_instances.loc[ti_index]['id_path']
                print(f"Visualizing subgraph of task instance at index {ti_index}...")
                description = f"{action_seq_description}ex{execution_index}_inst{ti_index}"
                self.svg.visualize_graph_from_task_instance(execution_pattern, ti_id_path, description,
                                                            self.print_duration, self.use_abbreviated_event_names)

                explore = input("Explore more pattern executions (y/n)? ")


def get_elementary_execution_patterns(tx, execution_pattern):
    if execution_pattern == 1:
        constraint = "WHERE size(ti.path) = 1"
    elif execution_pattern == 4:
        constraint = "WHERE size(ti.path) > 1"
    q = f'''
        MATCH (ti:TaskInstance) {constraint}
        WITH ti, duration.inSeconds(ti.start_time, ti.end_time).seconds AS duration
        WITH DISTINCT ti.path AS path, count(DISTINCT ti.rID) AS distinct_resources, AVG(duration) AS average_duration, 
            count(*) AS count ORDER BY count DESC
        RETURN count, distinct_resources, path, average_duration
        '''
    result = tx.run(q)
    df_execution_patterns = pd.DataFrame([dict(record) for record in result])
    return df_execution_patterns


def get_instances_of_elementary_execution(tx, execution_path):
    q = f'''
        MATCH (ti:TaskInstance) WHERE ti.path = {execution_path}
        WITH ti.rID AS resource, ti.cID AS case, duration.inSeconds(ti.start_time, ti.end_time).seconds AS duration, 
            ID(ti) AS id_path
        RETURN id_path, case, resource, duration ORDER BY duration ASC
        '''
    result = tx.run(q)
    df_execution_instances = pd.DataFrame([dict(record) for record in result])
    return df_execution_instances


def get_interrupted_execution_patterns(tx, df_entity_label, ti_entity_label):
    q = f'''
        MATCH (ti1)-[:DF_TI {{EntityType:"{df_entity_label}"}}]->(ti2)
        WHERE ti1.{ti_entity_label} = ti2.{ti_entity_label} 
            AND NOT (:TaskInstance {{{ti_entity_label}:ti1.{ti_entity_label}}})
            -[:DF_TI {{EntityType:"{df_entity_label}"}}]->(ti1)
        MATCH (ti3)-[:DF_TI {{EntityType:"{df_entity_label}"}}]->(ti4)
        WHERE ti3.{ti_entity_label} = ti4.{ti_entity_label} 
            AND NOT (ti4)-[:DF_TI {{EntityType:"{df_entity_label}"}}]
            ->(:TaskInstance {{{ti_entity_label}:ti4.{ti_entity_label}}})
        MATCH p = (ti1)-[:DF_TI*]->(ti4)
        WHERE all(r IN relationships(p) WHERE (r.EntityType = "{df_entity_label}")) 
            AND all(n IN nodes(p) WHERE n.{ti_entity_label} = ti1.{ti_entity_label}) 
            AND date(ti1.start_time) = date(ti4.end_time)
        WITH [ti IN nodes(p)| ti.path] AS paths, ti1.rID AS resources, 
            duration.inSeconds(ti1.start_time, ti4.end_time).seconds AS duration
        WITH DISTINCT paths AS path, count(DISTINCT resources) AS distinct_resources, 
            AVG(duration) AS average_duration, count(*) AS count ORDER BY count DESC
        RETURN count, distinct_resources, average_duration, path
       '''
    result = tx.run(q)
    df_execution_patterns = pd.DataFrame([dict(record) for record in result])
    return df_execution_patterns


def get_instances_of_interrupted_execution(tx, df_entity_label, ti_entity_label, execution_path):
    q = f'''
        MATCH (ti1)-[:DF_TI {{EntityType:"{df_entity_label}"}}]->(ti2)
            WHERE ti1.{ti_entity_label} = ti2.{ti_entity_label} 
                AND NOT (:TaskInstance {{{ti_entity_label}:ti1.{ti_entity_label}}})
                -[:DF_TI {{EntityType:"{df_entity_label}"}}]->(ti1)
        MATCH (ti3)-[:DF_TI {{EntityType:"{df_entity_label}"}}]->(ti4)
            WHERE ti3.{ti_entity_label} = ti4.{ti_entity_label} 
                AND NOT (ti4)-[:DF_TI {{EntityType:"{df_entity_label}"}}]
                ->(:TaskInstance {{{ti_entity_label}:ti4.{ti_entity_label}}})
        MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE all(r IN relationships(p) WHERE (r.EntityType = "{df_entity_label}")) 
            AND all(n IN nodes(p) WHERE n.{ti_entity_label} = ti1.{ti_entity_label}) 
            AND date(ti1.start_time) = date(ti4.end_time)
        WITH [ti IN nodes(p)| ti.path] AS path, [ti IN nodes(p)| ID(ti)] AS id_path, ti1.cID AS case, 
            ti1.rID AS resource, duration.inSeconds(ti1.start_time, ti4.end_time).seconds AS duration
        WHERE path = {execution_path}
        RETURN id_path, case, resource, duration ORDER BY duration ASC
        '''
    result = tx.run(q)
    df_execution_instances = pd.DataFrame([dict(record) for record in result])
    return df_execution_instances


def get_batch_action_sequences(tx, resource_label, execution_pattern):
    if execution_pattern == "7p":
        constraint1 = "AND size(ti1.path) = 1"
        constraint2 = "AND size(ti2.path) = 1"
    elif execution_pattern == "8p":
        constraint1 = "AND size(ti1.path) > 1"
        constraint2 = "AND size(ti2.path) > 1"
    q = f'''
        MATCH (ti1:TaskInstance) WHERE NOT (:TaskInstance {{path:ti1.path}})
            -[:DF_TI {{EntityType:"{resource_label}"}}]->(ti1) {constraint1}
            AND ti1.r_count = 1 AND ti1.c_count = 1
        MATCH (ti2:TaskInstance) WHERE NOT (ti2)-[:DF_TI {{EntityType:"{resource_label}"}}]->
            (:TaskInstance {{path:ti2.path}}) {constraint2} AND ti2.path=ti1.path AND ti2.r_count = 1 
            AND ti2.c_count = 1
        MATCH p=(ti1)-[:DF_TI*3..]->(ti2) 
        WHERE all(r in relationships(p) WHERE (r.EntityType = "{resource_label}")) AND 
            all(n IN nodes(p) WHERE n.path = ti1.path AND n.r_count = 1 AND n.c_count = 1) 
            AND all(idx in range(0, size(nodes(p))-2) WHERE datetime((nodes(p)[idx]).end_time)
            > (datetime((nodes(p)[idx+1]).start_time) - duration('PT30M')))
        WITH [ti IN nodes(p)| ti.path] AS path, ti1.rID AS resource, size([ti IN nodes(p)| ti.path]) AS length
        WITH DISTINCT path[0] AS action_seq, AVG(length) AS avg_batch_size, 
            count(DISTINCT resource) AS distinct_resources, count(*) AS count ORDER BY count DESC
        RETURN count, avg_batch_size, distinct_resources, action_seq
        '''
    result = tx.run(q)
    df_batch_action_sequences = pd.DataFrame([dict(record) for record in result])
    return df_batch_action_sequences


def get_batch_execution_patterns(tx, resource_label, action_sequence):
    q = f'''
        MATCH (ti1:TaskInstance) WHERE NOT (:TaskInstance {{path:ti1.path}})
            -[:DF_TI {{EntityType:"{resource_label}"}}]->(ti1) AND ti1.r_count = 1 AND ti1.c_count = 1
        MATCH (ti2:TaskInstance) WHERE NOT (ti2)-[:DF_TI {{EntityType:"{resource_label}"}}]->
            (:TaskInstance {{path:ti2.path}}) AND ti2.path=ti1.path AND ti2.r_count = 1 AND ti2.c_count = 1
        MATCH p=(ti1)-[:DF_TI*3..]->(ti2) 
        WHERE all(r in relationships(p) WHERE (r.EntityType = "{resource_label}")) AND 
            all(n IN nodes(p) WHERE n.path = ti1.path AND n.r_count = 1 AND n.c_count = 1) 
            AND all(idx in range(0, size(nodes(p))-2) WHERE datetime((nodes(p)[idx]).end_time)
            > (datetime((nodes(p)[idx+1]).start_time) - duration('PT30M')))
        WITH [ti IN nodes(p)| ti.path] AS paths, ti1.rID AS resources, 
            duration.inSeconds(ti1.start_time, ti2.end_time).seconds AS duration
        WITH DISTINCT paths AS path, count(DISTINCT resources) AS distinct_resources, 
            AVG(duration) AS average_duration, count(*) AS count ORDER BY count DESC
        WHERE path[0] = {action_sequence}
        RETURN count, distinct_resources, average_duration, path
        '''
    result = tx.run(q)
    df_execution_patterns = pd.DataFrame([dict(record) for record in result])
    return df_execution_patterns


def get_instances_of_batch_executions(tx, resource_label, execution_path):
    q = f'''
        MATCH (ti1:TaskInstance) WHERE NOT (:TaskInstance {{path:ti1.path}})
            -[:DF_TI {{EntityType:"{resource_label}"}}]->(ti1) AND ti1.r_count = 1 AND ti1.c_count = 1
        MATCH (ti2:TaskInstance) WHERE NOT (ti2)-[:DF_TI {{EntityType:"{resource_label}"}}]->
            (:TaskInstance {{path:ti2.path}}) AND ti2.path=ti1.path AND ti2.r_count = 1 AND ti2.c_count = 1
        MATCH p=(ti1)-[:DF_TI*3..]->(ti2) 
        WHERE all(r in relationships(p) WHERE (r.EntityType = "{resource_label}")) AND 
            all(n IN nodes(p) WHERE n.path = ti1.path AND n.r_count = 1 AND n.c_count = 1) 
            AND all(idx in range(0, size(nodes(p))-2) WHERE datetime((nodes(p)[idx]).end_time)
            > (datetime((nodes(p)[idx+1]).start_time) - duration('PT30M')))
        WITH [ti IN nodes(p)| ti.path] AS path, [ti IN nodes(p)| ID(ti)] AS id_path, [ti IN nodes(p)| ti.cID] AS case, 
            ti1.rID AS resource, duration.inSeconds(ti1.start_time, ti2.end_time).seconds AS duration
        WHERE path = {execution_path}
        RETURN id_path, resource, duration, case ORDER BY duration ASC
        '''
    result = tx.run(q)
    df_execution_instances = pd.DataFrame([dict(record) for record in result])
    return df_execution_instances
