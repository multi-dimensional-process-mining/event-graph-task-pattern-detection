from neo4j import GraphDatabase
import pandas as pd

### begin config

data_set = "bpic2014"
# data_set = "bpic2017"
resource_selection = 2
time_constraint = 0

# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", data_set))

# Dataset settings
if data_set == "bpic2014":
    string_start = 4
    entities = [["Resource", "AssignmentGroup"],
                ["Incident", "IncidentID"]]
    nr_of_events = 466737
    resource_selection = 0
elif data_set == "bpic2017":
    entities = [["Case_R", "resource"],
                ["Case_AWO", "case"]]
    string_start = 5
    nr_of_events = 859705

q_resource_selection = [["", ""],
                        ["AND NOT ti.rID = \"User_1\"", "AND none(n in nodes(p) WHERE n.rID = \"User_1\")"],
                        ["AND ti.rID = \"User_1\"", "AND all(n in nodes(p) WHERE n.rID = \"User_1\")"]]

q_time_constraint = [f"AND any(idx in range(0, size(nodes(p))-2) WHERE datetime((nodes(p)[idx]).end_time) < "
                     f"(datetime((nodes(p)[idx+1]).start_time) - duration('PT30M')))",
                     f"AND all(idx in range(0, size(nodes(p))-2) WHERE datetime((nodes(p)[idx]).end_time) > "
                     f"(datetime((nodes(p)[idx+1]).start_time) - duration('PT30M')))"]

results = pd.DataFrame(columns=['nr. of TIs', 'nr. of events', '% of events', 'mean_length', 'sd_length',
                                'mean_duration', 'sd_duration', 'mean_#_TIs', 'sd_#_TIs'],
                       index=["1", "4", "2", "3", "2*", "3*", "7*", "8*"])


def print_results_to_frame(df_pattern_stats, pattern):
    pattern = str(pattern)
    if time_constraint == 1:
        pattern = pattern + "*"
    if pattern == "1" or pattern == "4":
        results.loc[pattern] = [len(df_pattern_stats), sum(df_pattern_stats['pattern_length']),
                                (sum(df_pattern_stats['pattern_length']) / nr_of_events * 100),
                                (df_pattern_stats['pattern_length']).mean(), (df_pattern_stats['pattern_length']).std(),
                                (df_pattern_stats['duration']).mean(), (df_pattern_stats['duration']).std(), 0, 0]
    else:
        results.loc[pattern] = [len(df_pattern_stats), sum(df_pattern_stats['pattern_length']),
                                (sum(df_pattern_stats['pattern_length']) / nr_of_events * 100),
                                (df_pattern_stats['pattern_length']).mean(), (df_pattern_stats['pattern_length']).std(),
                                (df_pattern_stats['duration']).mean(), (df_pattern_stats['duration']).std(),
                                (df_pattern_stats['nr_of_sub_patterns']).mean(),
                                (df_pattern_stats['nr_of_sub_patterns']).std()]


def print_pattern_stats(df_pattern_stats, pattern):
    print(f"#########################################################\n\nPATTERN {pattern}:\n")
    print(f"Number of pattern instances: {len(df_pattern_stats)}")
    print(f"Number of events: {sum(df_pattern_stats['pattern_length'])}")
    print(f"Percentage of total events: {sum(df_pattern_stats['pattern_length']) / nr_of_events}")
    print(f"Mean, stDev pattern length: {(df_pattern_stats['pattern_length']).mean()}, "
          f"{(df_pattern_stats['pattern_length']).std()}")
    print(f"Mean, stDev duration (minutes): {(df_pattern_stats['duration']).mean()}, "
          f"{(df_pattern_stats['duration']).std()}")
    if pattern > 2:
        print(f"Mean, stDev number of sub patterns: {(df_pattern_stats['nr_of_sub_patterns']).mean()}, "
              f"{(df_pattern_stats['nr_of_sub_patterns']).std()}")
    print("\n")


def get_stats_pattern_1(tx):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) = 1 {q_resource_selection[resource_selection][0]}
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("1")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    print_results_to_frame(df_pattern_stats, 1)


def get_stats_pattern_4(tx):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) > 1 {q_resource_selection[resource_selection][0]}
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("4")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    print_results_to_frame(df_pattern_stats, 4)


def get_stats_pattern_3(tx):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entities[0][0]}"}}]->(ti2)
            WHERE ti1.cID = ti2.cID 
                AND NOT (:TaskInstance {{cID:ti1.cID}})-[:DF_TI {{EntityType:"{entities[0][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entities[0][0]}"}}]->(ti4)
            WHERE ti3.cID = ti4.cID 
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entities[0][0]}"}}]->(:TaskInstance {{cID:ti4.cID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entities[0][0]}")) 
                AND all(n IN nodes(p) WHERE n.cID = ti1.cID) AND date(ti1.start_time) = date(ti4.end_time) 
                {q_resource_selection[resource_selection][1]} {q_time_constraint[time_constraint]}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("3")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    print_results_to_frame(df_pattern_stats, 3)


def get_stats_pattern_2(tx):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entities[1][0]}"}}]->(ti2)
            WHERE ti1.rID = ti2.rID 
                AND NOT (:TaskInstance {{rID:ti1.rID}})-[:DF_TI {{EntityType:"{entities[1][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entities[1][0]}"}}]->(ti4)
            WHERE ti3.rID = ti4.rID 
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entities[1][0]}"}}]->(:TaskInstance {{rID:ti4.rID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entities[1][0]}")) 
                AND all(n IN nodes(p) WHERE n.rID = ti1.rID) AND date(ti1.start_time) = date(ti4.end_time)
                {q_resource_selection[resource_selection][1]} {q_time_constraint[time_constraint]}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("2")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    print_results_to_frame(df_pattern_stats, 2)


def get_stats_pattern_7(tx):
    q = f'''
            MATCH (ti1:TaskInstance) WHERE NOT (:TaskInstance {{path:ti1.path}})
                -[:DF_TI {{EntityType:"{entities[0][0]}"}}]->(ti1) AND size(ti1.path) = 1
            MATCH (ti2:TaskInstance) WHERE NOT (ti2)-[:DF_TI {{EntityType:"{entities[0][0]}"}}]->
                (:TaskInstance {{path:ti2.path}}) AND size(ti2.path) = 1 AND ti2.path=ti1.path
            MATCH p=(ti1)-[:DF_TI*3..]->(ti2) 
            WHERE all(r in relationships(p) WHERE (r.EntityType = "{entities[0][0]}")) AND 
                none(n IN nodes(p) WHERE NOT n.path = ti1.path) 
                {q_resource_selection[resource_selection][1]} {q_time_constraint[time_constraint]}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, size((nodes(p)[0]).path) AS sub_pattern_length, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            RETURN nr_of_sub_patterns, sub_pattern_length, duration ORDER BY nr_of_sub_patterns DESC
            '''
    result = tx.run(q)
    print("7")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    df_pattern_stats['pattern_length'] = df_pattern_stats['nr_of_sub_patterns'] * df_pattern_stats['sub_pattern_length']
    print_results_to_frame(df_pattern_stats, 7)


def get_stats_pattern_8(tx):
    q = f'''
            MATCH (ti1:TaskInstance) WHERE NOT (:TaskInstance {{path:ti1.path}})
                -[:DF_TI {{EntityType:"{entities[0][0]}"}}]->(ti1) AND size(ti1.path) > 1
            MATCH (ti2:TaskInstance) WHERE NOT (ti2)-[:DF_TI {{EntityType:"{entities[0][0]}"}}]->
                (:TaskInstance {{path:ti2.path}}) AND size(ti2.path) > 1 AND ti2.path=ti1.path
            MATCH p=(ti1)-[:DF_TI*3..]->(ti2) 
            WHERE all(r in relationships(p) WHERE (r.EntityType = "{entities[0][0]}")) AND 
                none(n IN nodes(p) WHERE NOT n.path = ti1.path) 
                {q_resource_selection[resource_selection][1]} {q_time_constraint[time_constraint]}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, size((nodes(p)[0]).path) AS sub_pattern_length, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            RETURN nr_of_sub_patterns, sub_pattern_length, duration ORDER BY nr_of_sub_patterns DESC
            '''
    result = tx.run(q)
    print("8")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    df_pattern_stats['pattern_length'] = df_pattern_stats['nr_of_sub_patterns'] * df_pattern_stats['sub_pattern_length']
    print_results_to_frame(df_pattern_stats, 8)


with driver.session() as session:
    session.read_transaction(get_stats_pattern_1)
    session.read_transaction(get_stats_pattern_4)
    session.read_transaction(get_stats_pattern_2)
    session.read_transaction(get_stats_pattern_3)
    time_constraint = 1
    session.read_transaction(get_stats_pattern_2)
    session.read_transaction(get_stats_pattern_3)
    session.read_transaction(get_stats_pattern_7)
    session.read_transaction(get_stats_pattern_8)
    # print("#########################################################")

    results.to_csv(f"analysis_results/{data_set}_measurements_rs{resource_selection}.csv")
