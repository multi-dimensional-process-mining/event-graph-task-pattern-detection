import pandas as pd
import time
from neo4j import GraphDatabase

### begin config
data_set = "bpic2014"
# data_set = "bpic2017"

# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", data_set))

perf_file_name = f'{data_set}_TI_full_performance.csv'

nodes_to_remove = ["Log"]
if data_set == "bpic2017":
    event_properties_to_remove = ["CreditScore", "LoanGoal", "FirstWithdrawalAmount", "RequestedAmount",
                                  "OfferedAmount", "Selected", "MonthlyCost", "NumberOfTerms", "Accepted", "Action"]
    event_identifier = "Activity_Lifecycle"
    entities = [["Case_R", "rID", True, "resource"],
                ["Case_AWO", "cID", False, "case"]]
elif data_set == "bpic2014":
    nodes_to_remove = ["Log", "Entity {EntityType: \"CIName\"}", "Entity {EntityType: \"Interaction\"}",
                       "Entity {EntityType: \"Change\"}"]
    event_identifier = "Activity"
    event_properties_to_remove = ["KMNo", "InteractionID", "IncidentActivityNumber", "Log", "SubLog"]
    entities = [["Resource", "rID", True, "AssignmentGroup"],
                ["Incident", "cID", False, "IncidentID"]]

step_clear_ti_constructs = False
construct_task_instances = True
step_create_classes = False
step_remove_properties_from_node = True
step_remove_nodes = True

### end config


######################################################
########## DEFAULT METHODS FOR CONSTRUCTION ##########
######################################################


def clear_ti_constructs(tx):
    q_clear_event_df_joint = f'''
            MATCH (e1:Event)-[df:DF {{EntityType:"joint"}}]->(e2:Event)
            DELETE df '''
    tx.run(q_clear_event_df_joint)
    q_clear_activity_lifecycle_property = f'''
            MATCH (e:Event)
            REMOVE e.Activity_Lifecycle '''
    tx.run(q_clear_activity_lifecycle_property)
    q_detach_delete_ti_classes = f'''
            MATCH (c:Class {{Type:"TaskInstance"}})
            DETACH DELETE c '''
    tx.run(q_detach_delete_ti_classes)
    q_detach_delete_ti_nodes = f'''
            MATCH (ti:TaskInstance)
            DETACH DELETE ti '''
    tx.run(q_detach_delete_ti_nodes)


def remove_properties_from_node(tx, node, properties):
    for property in properties:
        q_remove_property = f'''
            MATCH (n:{node})
            REMOVE n.{property}'''
        print(q_remove_property)
        tx.run(q_remove_property)


def remove_nodes(tx, nodes):
    for node in nodes:
        q_remove_node = f'''
            MATCH (n:{node})
            DETACH DELETE n'''
        print(q_remove_node)
        tx.run(q_remove_node)


def remove_events_without_resource(tx, resource_identifier):
    q_remove_events_without_resource = f'''
            MATCH (e:Event) WHERE NOT EXISTS(e.{resource_identifier})
            DETACH DELETE e'''
    print(q_remove_events_without_resource)
    tx.run(q_remove_events_without_resource)


def combine_df_joint(tx):
    q_combine_df_joint = f'''
            MATCH (e1:Event)-[:DF {{EntityType:'{entities[0][0]}'}}]->(e2:Event) 
            WHERE (e1)-[:DF {{EntityType:'{entities[1][0]}'}}]->(e2)
            CREATE (e1)-[:DF {{EntityType:'joint'}}]->(e2)
            '''
    print(q_combine_df_joint)
    tx.run(q_combine_df_joint)


def set_activity_lifecycle_property(tx):
    q_set_activity_lifecycle_property = f'''
            MATCH (e:Event)
            SET e.Activity_Lifecycle = e.Activity+'+'+e.lifecycle
            '''
    print(q_set_activity_lifecycle_property)
    tx.run(q_set_activity_lifecycle_property)


def create_ti_nodes(tx):
    q_create_ti = f'''
            CALL {{
            MATCH (e1:Event)-[:DF {{EntityType:'joint'}}]->() WHERE NOT ()-[:DF {{EntityType:'joint'}}]->(e1)
            MATCH ()-[:DF {{EntityType:'joint'}}]->(e2:Event) WHERE NOT (e2)-[:DF {{EntityType:'joint'}}]->()
            MATCH p=(e1)-[:DF*]->(e2) WHERE all(r in relationships(p) WHERE (r.EntityType = 'joint'))
            RETURN p, e1, e2
            UNION
            MATCH (e:Event)
            WHERE NOT ()-[:DF {{EntityType:'joint'}}]->(e) AND NOT (e)-[:DF {{EntityType:'joint'}}]->()
            MATCH p=(e) RETURN p, e AS e1, e AS e2
            }}
            WITH [event in nodes(p) | event.{event_identifier}] AS path, e1.{entities[0][3]} AS resource,
                e1.{entities[1][3]} AS case_id, nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time
            CREATE (ti:TaskInstance {{path:path, rID:resource, cID:case_id, start_time:start_time,
                end_time:end_time}})
            WITH ti, events
            UNWIND events AS e
            CREATE (e)<-[:CONTAINS]-(ti)
            '''
    print(q_create_ti)
    tx.run(q_create_ti)


def correlate_ti_to_entity(tx, entity_type, entity_ref_id):
    q_correlate_ti_to_entity = f'''
            MATCH (ti:TaskInstance)
            MATCH (n:Entity {{EntityType:"{entity_type}"}}) WHERE ti.{entity_ref_id} = n.ID
            CREATE (ti)-[:CORR]->(n)
            '''
    print(q_correlate_ti_to_entity)
    tx.run(q_correlate_ti_to_entity)


def create_df_ti(tx, entity_type):
    q_create_df_ti = f'''
            MATCH (n:Entity) WHERE n.EntityType="{entity_type}"
            MATCH (ti)-[:CORR]->(n)
            WITH n, ti AS nodes ORDER BY ti.start_time, ID(ti)
            WITH n, COLLECT (nodes) as nodeList
            UNWIND range(0, size(nodeList)-2) AS i
            WITH n, nodeList[i] as ti_first, nodeList[i+1] as ti_second
            MERGE (ti_first)-[df:DF_TI {{EntityType:n.EntityType}}]->(ti_second)
            '''
    print(q_create_df_ti)
    tx.run(q_create_df_ti)


def create_ti_classes(tx):
    q_create_ti_class_nodes = f'''
            MATCH (ti:TaskInstance)
            WITH DISTINCT ti.path AS path, count(*) AS count
            ORDER BY count DESC
            WITH collect(path) as paths, collect(count) as counts, collect(size(path)) as lengths
            UNWIND range(0, size(paths)-1) as pos
            WITH paths[pos] AS path, pos+1 AS rank, counts[pos] AS freq, lengths[pos] AS path_length
            CREATE (:Class {{Type:'TaskInstance', path:path, path_length:path_length, frequency:freq, rank:rank}})
            '''
    print(q_create_ti_class_nodes)
    tx.run(q_create_ti_class_nodes)

    q_create_TI_C_relationships = f'''
            MATCH (c:Class {{Type:'TaskInstance'}})
            MATCH (ti:TaskInstance) WHERE ti.path = c.path
            CREATE (ti)-[:TI_C]->(c)
            '''
    print(q_create_TI_C_relationships)
    tx.run(q_create_TI_C_relationships)

    q_derive_TI_ID_from_class_rank = f'''
            MATCH (ti:TaskInstance)-[:TI_C]->(c:Class)
            SET ti.ID = c.rank
            '''
    print(q_derive_TI_ID_from_class_rank)
    tx.run(q_derive_TI_ID_from_class_rank)


def aggregate_df_ti_classes(tx, entity_type, same_day_only):
    if same_day_only:
        q_aggregate_df_ti_classes = f'''
            MATCH (c1:Class)<-[:TI_C]-(ti1:TaskInstance)-[df:DF_TI {{EntityType:"{entity_type}"}}]->
            (ti2:TaskInstance)-[:TI_C]->(c2:Class)
            WHERE date(ti1.start_time) = date(ti2.start_time)
            WITH c1, c2, count(df) AS df_freq
            MERGE (c1)-[r:DF_C {{EntityType:"{entity_type}"}}]->(c2) ON CREATE SET r.count=df_freq
            '''
    else:
        q_aggregate_df_ti_classes = f'''
            MATCH (c1:Class)<-[:TI_C]-(ti1:TaskInstance)-[df:DF_TI {{EntityType:"{entity_type}"}}]->
            (ti2:TaskInstance)-[:TI_C]->(c2:Class)
            WITH c1, c2, count(df) AS df_freq
            MERGE (c1)-[r:DF_C {{EntityType:"{entity_type}"}}]->(c2) ON CREATE SET r.count=df_freq
            '''
    print(q_aggregate_df_ti_classes)
    tx.run(q_aggregate_df_ti_classes)


def record_performance(performance_table, action, last_time, end_time):
    performance_table = performance_table.append({'name': data_set + action, 'start': last_time, 'end': end_time,
                                                  'duration': (end_time - last_time)}, ignore_index=True)
    print(action[1:] + ' done: took ' + str(end_time - last_time) + ' seconds')
    return performance_table, end_time


######################################################
####################### BPIC 17 ######################
######################################################

# table to measure performance
perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
start = time.time()
last = start

with driver.session() as session:

    if step_clear_ti_constructs:
        print("Clearing all task instance constructs...")
        session.write_transaction(clear_ti_constructs)
        last = time.time()

    if step_remove_properties_from_node:
        print("Removing properties from Event nodes...")
        session.write_transaction(remove_properties_from_node, "Event", event_properties_to_remove)
        perf, last = record_performance(perf, "_remove_properties", last, time.time())

    if step_remove_nodes:
        print("Removing Log (and other) nodes...")
        session.write_transaction(remove_nodes, nodes_to_remove)
        if data_set == "bpic2014":
            print("Removing Events without resource...")
            session.write_transaction(remove_events_without_resource, entities[0][3])
        perf, last = record_performance(perf, "_remove_nodes", last, time.time())

    if construct_task_instances:
        print("Creating [:DF {EntityType:joint}] relationships between events...")
        session.write_transaction(combine_df_joint)
        perf, last = record_performance(perf, "_combine_df_joint", last, time.time())

        if data_set == "bpic2017":
            print("Combining event.activity and event.lifecycle properties...")
            session.write_transaction(set_activity_lifecycle_property)
            perf, last = record_performance(perf, "_set_activity_lifecycle_property", last, time.time())

        print("Creating TaskInstance nodes, properties and :CONTAINS relationships...")
        session.write_transaction(create_ti_nodes)
        perf, last = record_performance(perf, "_create_ti_nodes", last, time.time())

        for entity in entities:
            print("Correlating TaskInstances to Entities...")
            session.write_transaction(correlate_ti_to_entity, entity[0], entity[1])
            perf, last = record_performance(perf, "_correlate_ti_to_entity", last, time.time())

        for entity in entities:
            print(f"Creating :DF {{EntityType:{entity[0]}}} relationships...")
            session.write_transaction(create_df_ti, entity[0])
            perf, last = record_performance(perf, "_create_df_ti_" + entity[0], last, time.time())

    if step_create_classes:
        print("Creating TaskInstance Classes and :HLE_C relationships...")
        session.write_transaction(create_ti_classes)
        perf, last = record_performance(perf, "_create_ti_classes", last, time.time())

        for entity in entities:
            print(f"Creating :DF_C {{Type:TaskInstance, EntityType:{entity[0]}}} relationships...")
            session.write_transaction(aggregate_df_ti_classes, entity[0], entity[2])
            perf, last = record_performance(perf, "_aggregate_df_ti_classes_" + entity[0], last, time.time())


perf, _ = record_performance(perf, "_total", start, time.time())
perf.to_csv(perf_file_name)

driver.close()
