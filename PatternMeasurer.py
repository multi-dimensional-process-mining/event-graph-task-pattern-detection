from neo4j import GraphDatabase
import pandas as pd
from numpy import nan as Nan


class PatternMeasurer:

    def __init__(self, graph, password, entity_labels, total_events):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.graph = graph
        self.entity_labels = entity_labels
        self.entity_labels[0].append('rID')
        self.entity_labels[1].append('cID')

        self.qs_resource_selection = [["", ""],
                                      ["AND NOT ti.rID = \"User_1\"",
                                       "AND none(n in nodes(p) WHERE n.rID = \"User_1\")"],
                                      ["AND ti.rID = \"User_1\"", "AND all(n in nodes(p) WHERE n.rID = \"User_1\")"]]

        self.qs_time_constraint = [f"AND any(idx in range(0, size(nodes(p))-2) WHERE datetime((nodes(p)[idx]).end_time)"
                                   f"< (datetime((nodes(p)[idx+1]).start_time) - duration('PT30M')))",
                                   f"AND all(idx in range(0, size(nodes(p))-2) WHERE datetime((nodes(p)[idx]).end_time)"
                                   f"> (datetime((nodes(p)[idx+1]).start_time) - duration('PT30M')))"]
        self.total_events = total_events

    def get_all_pattern_measurements(self, pm_selection):

        pm_index = []
        for row in pm_selection:
            pm_index.append(f"{row[0]}_{row[1]}_{row[2]}")

        self.df_measurements = pd.DataFrame(columns=['nr. of TIs', 'nr. of events', '% of events', 'mean_length',
                                                     'sd_length', 'mean_duration', 'sd_duration', 'mean_#_TIs',
                                                     'sd_#_TIs'], index=pm_index)

        with self.driver.session() as session:
            for i, row in enumerate(pm_selection):
                pattern = row[0]
                q_time_constraint = self.qs_time_constraint[row[1]]
                q_resource_selection = self.qs_resource_selection[row[2]]
                if pattern == 1:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_1, q_resource_selection)
                elif pattern == 2:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_2, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 3:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_3, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 4:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_4, q_resource_selection)
                elif pattern == 5:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_5, q_resource_selection)
                elif pattern == 6:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_6, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 7:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_7, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 8:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_8, q_resource_selection)
                elif pattern == 9:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_9, q_resource_selection)
                elif pattern == 10:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_10, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 11:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_11, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 12:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_12, q_resource_selection)
                elif pattern == 13:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_13, q_resource_selection)
                elif pattern == 14:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_14, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 15:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_15, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == 16:
                    df_pattern_stats = session.read_transaction(get_stats_pattern_16, q_resource_selection)
                elif pattern == "7p":
                    df_pattern_stats = session.read_transaction(get_stats_pattern_7p, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                elif pattern == "8p":
                    df_pattern_stats = session.read_transaction(get_stats_pattern_8p, q_resource_selection,
                                                                q_time_constraint, self.entity_labels)
                else:
                    print(f"This pattern {pattern} cannot be queried for measurements.")

                self.df_measurements = update_measurements_with_pattern_stats(self.df_measurements, df_pattern_stats,
                                                                              self.total_events, pattern, pm_index[i])

            self.df_measurements.to_csv(f'output\\measurements\\measurements_{self.graph}.csv')


def update_measurements_with_pattern_stats(df_measurements, df_pattern_stats, total_events, pattern, index):
    if len(df_pattern_stats) == 0:
        df_measurements.loc[index] = [Nan, Nan, Nan, Nan, Nan, Nan, Nan, Nan, Nan]
    elif pattern in [1, 4, 5, 8, 9, 12, 13, 16]:  # if pattern is elementary
        df_measurements.loc[index] = [len(df_pattern_stats), sum(df_pattern_stats['pattern_length']),
                                      (sum(df_pattern_stats['pattern_length']) / total_events * 100),
                                      (df_pattern_stats['pattern_length']).mean(),
                                      (df_pattern_stats['pattern_length']).std(),
                                      (df_pattern_stats['duration']).mean(), (df_pattern_stats['duration']).std(),
                                      Nan, Nan]
    else:  # if pattern is non-elementary
        df_measurements.loc[index] = [len(df_pattern_stats), sum(df_pattern_stats['pattern_length']),
                                      (sum(df_pattern_stats['pattern_length']) / total_events * 100),
                                      (df_pattern_stats['pattern_length']).mean(),
                                      (df_pattern_stats['pattern_length']).std(),
                                      (df_pattern_stats['duration']).mean(), (df_pattern_stats['duration']).std(),
                                      (df_pattern_stats['nr_of_sub_patterns']).mean(),
                                      (df_pattern_stats['nr_of_sub_patterns']).std()]
    return df_measurements


def get_stats_pattern_1(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) = 1 AND ti.r_count = 1 AND ti.c_count = 1
            {q_resource_selection[0]}
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("1")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_4(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) > 1 AND ti.r_count = 1 AND ti.c_count = 1 
            {q_resource_selection[0]}
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("4")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_2(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti2)
            WHERE ti1.rID = ti2.rID AND ti1.r_count = 1 AND ti1.c_count = 1
                AND NOT (:TaskInstance {{rID:ti1.rID}})-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti4)
            WHERE ti3.rID = ti4.rID AND ti4.r_count = 1 AND ti4.c_count = 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(:TaskInstance {{rID:ti4.rID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[1][0]}")) 
                AND all(n IN nodes(p) WHERE n.rID = ti1.rID AND n.r_count = 1 AND n.c_count = 1) 
                AND date(ti1.start_time) = date(ti4.end_time)
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("2")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_3(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti2)
            WHERE ti1.cID = ti2.cID AND ti1.r_count = 1 AND ti1.c_count = 1
                AND NOT (:TaskInstance {{cID:ti1.cID}})-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti4)
            WHERE ti3.cID = ti4.cID AND ti4.r_count = 1 AND ti4.c_count = 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(:TaskInstance {{cID:ti4.cID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[0][0]}")) 
                AND all(n IN nodes(p) WHERE n.cID = ti1.cID AND n.r_count = 1 AND n.c_count = 1) 
                AND date(ti1.start_time) = date(ti4.end_time) 
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("3")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_5(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) = 1 AND ti.r_count = 1 AND ti.c_count > 1
            {q_resource_selection[0]} 
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("5")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_8(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) > 1 AND ti.r_count = 1 AND ti.c_count > 1
            {q_resource_selection[0]}
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("8")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_6(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti2)
            WHERE ti1.rID = ti2.rID AND ti1.r_count = 1 AND ti1.c_count > 1
                AND NOT (:TaskInstance {{rID:ti1.rID}})-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti4)
            WHERE ti3.rID = ti4.rID AND ti4.r_count = 1 AND ti4.c_count > 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(:TaskInstance {{rID:ti4.rID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4) 
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[1][0]}")) 
                AND all(n IN nodes(p) WHERE n.rID = ti1.rID AND n.r_count = 1 AND n.c_count > 1) 
                AND date(ti1.start_time) = date(ti4.end_time)
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("6")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_7(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti2)
            WHERE ti1.cID = ti2.cID AND ti1.r_count = 1 AND ti1.c_count > 1
                AND NOT (:TaskInstance {{cID:ti1.cID}})-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti4)
            WHERE ti3.cID = ti4.cID AND ti4.r_count = 1 AND ti4.c_count > 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(:TaskInstance {{cID:ti4.cID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[0][0]}")) 
                AND all(n IN nodes(p) WHERE n.cID = ti1.cID AND n.r_count = 1 AND n.c_count > 1) 
                AND date(ti1.start_time) = date(ti4.end_time) 
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("7")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_9(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) = 1 AND ti.r_count > 1 AND ti.c_count = 1
            {q_resource_selection[0]} 
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("9")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_12(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) > 1 AND ti.r_count > 1 AND ti.c_count = 1
            {q_resource_selection[0]} 
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("12")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_10(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti2)
            WHERE ti1.rID = ti2.rID AND ti1.r_count > 1 AND ti1.c_count = 1
                AND NOT (:TaskInstance {{rID:ti1.rID}})-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti4)
            WHERE ti3.rID = ti4.rID AND ti4.r_count > 1 AND ti4.c_count = 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(:TaskInstance {{rID:ti4.rID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4) 
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[1][0]}")) 
                AND all(n IN nodes(p) WHERE n.rID = ti1.rID AND n.r_count > 1 AND n.c_count = 1) 
                AND date(ti1.start_time) = date(ti4.end_time)
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("10")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_11(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti2)
            WHERE ti1.cID = ti2.cID AND ti1.r_count > 1 AND ti1.c_count = 1
                AND NOT (:TaskInstance {{cID:ti1.cID}})-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti4)
            WHERE ti3.cID = ti4.cID AND ti4.r_count > 1 AND ti4.c_count = 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(:TaskInstance {{cID:ti4.cID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[0][0]}")) 
                AND all(n IN nodes(p) WHERE n.cID = ti1.cID AND n.r_count > 1 AND n.c_count = 1) 
                AND date(ti1.start_time) = date(ti4.end_time) 
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("11")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_13(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) = 1 AND ti.r_count > 1 AND ti.c_count > 1
            {q_resource_selection[0]} 
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("13")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_16(tx, q_resource_selection):
    q = f'''
            MATCH (ti:TaskInstance) WHERE size(ti.path) > 1 AND ti.r_count > 1 AND ti.c_count > 1
            {q_resource_selection[0]} 
            WITH duration.inSeconds(ti.start_time, ti.end_time).minutes AS duration, size(ti.path) AS pattern_length
            RETURN pattern_length, duration
            '''
    result = tx.run(q)
    print("16")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_14(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti2)
            WHERE ti1.rID = ti2.rID AND ti1.r_count > 1 AND ti1.c_count > 1
                AND NOT (:TaskInstance {{rID:ti1.rID}})-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(ti4)
            WHERE ti3.rID = ti4.rID AND ti4.r_count > 1 AND ti4.c_count > 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[1][0]}"}}]->(:TaskInstance {{rID:ti4.rID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[1][0]}")) 
                AND all(n IN nodes(p) WHERE n.rID = ti1.rID AND n.r_count > 1 AND n.c_count > 1) 
                AND date(ti1.start_time) = date(ti4.end_time)
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("14")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_15(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti2)
            WHERE ti1.cID = ti2.cID AND ti1.r_count > 1 AND ti1.c_count > 1
                AND NOT (:TaskInstance {{cID:ti1.cID}})-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti1)
            MATCH (ti3)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti4)
            WHERE ti3.cID = ti4.cID AND ti4.r_count > 1 AND ti4.c_count > 1
                AND NOT (ti4)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(:TaskInstance {{cID:ti4.cID}})
            MATCH p = (ti1)-[:DF_TI*]->(ti4)
            WHERE none(r IN relationships(p) WHERE NOT (r.EntityType = "{entity_labels[0][0]}")) 
                AND all(n IN nodes(p) WHERE n.cID = ti1.cID AND n.r_count > 1 AND n.c_count > 1) 
                AND date(ti1.start_time) = date(ti4.end_time) 
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, 
                [tp IN nodes(p) | size(tp.path)] AS sub_pattern_lengths, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            WITH reduce(s = 0, x IN sub_pattern_lengths | s + x) AS pattern_length, nr_of_sub_patterns, duration
            RETURN nr_of_sub_patterns, pattern_length, duration ORDER BY nr_of_sub_patterns, pattern_length DESC
            '''
    result = tx.run(q)
    print("15")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    return df_pattern_stats


def get_stats_pattern_7p(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1:TaskInstance) WHERE NOT (:TaskInstance {{path:ti1.path}})
                -[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti1) AND size(ti1.path) = 1
                AND ti1.r_count = 1 AND ti1.c_count = 1
            MATCH (ti2:TaskInstance) WHERE NOT (ti2)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->
                (:TaskInstance {{path:ti2.path}}) AND size(ti2.path) = 1 AND ti2.path=ti1.path
                AND ti2.r_count = 1 AND ti2.c_count = 1
            MATCH p=(ti1)-[:DF_TI*3..]->(ti2) 
            WHERE all(r in relationships(p) WHERE (r.EntityType = "{entity_labels[0][0]}")) AND 
                all(n IN nodes(p) WHERE n.path = ti1.path AND n.r_count = 1 AND n.c_count = 1) 
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, size((nodes(p)[0]).path) AS sub_pattern_length, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            RETURN nr_of_sub_patterns, sub_pattern_length, duration ORDER BY nr_of_sub_patterns DESC
            '''
    result = tx.run(q)
    print("7p")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    if len(df_pattern_stats) > 0:
        df_pattern_stats['pattern_length'] = df_pattern_stats['nr_of_sub_patterns'] * df_pattern_stats['sub_pattern_length']
    return df_pattern_stats


def get_stats_pattern_8p(tx, q_resource_selection, q_time_constraint, entity_labels):
    q = f'''
            MATCH (ti1:TaskInstance) WHERE NOT (:TaskInstance {{path:ti1.path}})
                -[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->(ti1) AND size(ti1.path) > 1
                AND ti1.r_count = 1 AND ti1.c_count = 1
            MATCH (ti2:TaskInstance) WHERE NOT (ti2)-[:DF_TI {{EntityType:"{entity_labels[0][0]}"}}]->
                (:TaskInstance {{path:ti2.path}}) AND size(ti2.path) > 1 AND ti2.path=ti1.path
                AND ti2.r_count = 1 AND ti2.c_count = 1
            MATCH p=(ti1)-[:DF_TI*3..]->(ti2) 
            WHERE all(r in relationships(p) WHERE (r.EntityType = "{entity_labels[0][0]}")) AND 
                all(n IN nodes(p) WHERE n.path = ti1.path AND n.r_count = 1 AND n.c_count = 1) 
                {q_resource_selection[1]} {q_time_constraint}
            WITH size([tp in nodes(p) | tp.path]) AS nr_of_sub_patterns, size((nodes(p)[0]).path) AS sub_pattern_length, 
                duration.inSeconds(ti1.start_time, ti2.end_time).minutes AS duration
            RETURN nr_of_sub_patterns, sub_pattern_length, duration ORDER BY nr_of_sub_patterns DESC
            '''
    result = tx.run(q)
    print("8p")
    df_pattern_stats = pd.DataFrame([dict(record) for record in result])
    if len(df_pattern_stats) > 0:
        df_pattern_stats['pattern_length'] = df_pattern_stats['nr_of_sub_patterns'] * df_pattern_stats['sub_pattern_length']
    return df_pattern_stats
