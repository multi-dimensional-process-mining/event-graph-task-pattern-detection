from neo4j import GraphDatabase
from graphviz import Digraph
import LabelMakerSelector

# COLORS
white = "#ffffff"
black = "#000000"

dark_yellow = "#ffc000"

medium_red = '#d73027'
dark_red = '#570000'

medium_blue = '#4575b4'
dark_blue = '#002759'


class SubgraphVisualizer:

    def __init__(self, graph, password, name_data_set, entity_labels, action_lifecycle_labels, timestamp_label,
                 activity_label_sep=" "):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.graph = graph
        self.name_data_set = name_data_set
        self.property_labels = [entity_labels[0], entity_labels[1], action_lifecycle_labels, timestamp_label]
        self.activity_label_sep = activity_label_sep

    def visualize_graph_from_cases(self, case_ids, print_duration=False, use_label_dict=False):
        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", ranksep="0.5", nodesep="0.5")
        ti_list = []
        lm = LabelMakerSelector.get_label_maker(self.name_data_set, use_label_dict, self.property_labels[2],
                                                print_duration, self.activity_label_sep)
        with self.driver.session() as session:
            for case_id in case_ids:
                ti_case_path = session.read_transaction(get_ti_path_from_case, case_id)
                session.read_transaction(get_case_df_single, dot, lm, ti_case_path, self.property_labels, False)
                ti_list = ti_list + ti_case_path

            resource_ids = session.read_transaction(get_resource_ids, ti_list)
            for resource_id in resource_ids:
                ti_resource_path = session.read_transaction(get_ti_path_from_resource, resource_id, ti_list)
                session.read_transaction(get_resource_df_multi, dot, lm, ti_resource_path, case_ids,
                                         self.property_labels)
            description = ""
            for case_id in case_ids:
                description += lm.get_case_label(case_id)
                if case_ids.index(case_id) < len(case_ids) - 1:
                    description += ","

            dot.render(f'output\\subgraphs\\from_cases\\{self.graph}_{description}', view=True)

    def visualize_graph_from_task_instance(self, execution_pattern, ti_id_path, description, print_duration=False,
                                           use_label_dict=False):
        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", ranksep="0.5", nodesep="0.5")
        lm = LabelMakerSelector.get_label_maker(self.name_data_set, use_label_dict, self.property_labels[2],
                                                print_duration, self.activity_label_sep)
        with self.driver.session() as session:
            if execution_pattern in ["1", "4"]:
                session.read_transaction(get_resource_df_single, dot, lm, [ti_id_path], self.property_labels, True)
                session.read_transaction(get_case_df_single, dot, lm, [ti_id_path], self.property_labels, True)
            elif execution_pattern == "2":
                ti_id_full_path, case_path = session.read_transaction(get_full_ti_id_path_pattern_2, ti_id_path,
                                                                      self.property_labels[:2])
                session.read_transaction(get_resource_df_single, dot, lm, ti_id_full_path, self.property_labels, True)
                distinct_cases = list(set(case_path))
                for distinct_case in distinct_cases:
                    case_ti_path = []
                    for i, case in enumerate(case_path):
                        if case == distinct_case:
                            case_ti_path.append(ti_id_full_path[i])
                    session.read_transaction(get_case_df_single, dot, lm, case_ti_path, self.property_labels, True)
            elif execution_pattern == "3":
                ti_id_full_path, resource_path = session.read_transaction(get_full_ti_id_path_pattern_3, ti_id_path,
                                                                          self.property_labels[:2])
                distinct_resources = list(set(resource_path))
                for distinct_resource in distinct_resources:
                    resource_ti_path = []
                    for i, resource in enumerate(resource_path):
                        if resource == distinct_resource:
                            resource_ti_path.append(ti_id_full_path[i])
                    session.read_transaction(get_resource_df_single, dot, lm, resource_ti_path, self.property_labels,
                                             True)
                session.read_transaction(get_case_df_single, dot, lm, ti_id_full_path, self.property_labels, True)
            elif execution_pattern in ["7p", "8p"]:
                session.read_transaction(get_resource_df_single, dot, lm, ti_id_path, self.property_labels, True)
                case_path = session.read_transaction(get_case_path_batch_pattern, ti_id_path,
                                                     self.property_labels[0][0])
                distinct_cases = list(set(case_path))
                for distinct_case in distinct_cases:
                    case_ti_path = []
                    for i, case in enumerate(case_path):
                        if case == distinct_case:
                            case_ti_path.append(ti_id_path[i])
                    session.read_transaction(get_case_df_single, dot, lm, case_ti_path, self.property_labels, True)

        dot.render(f'output\\subgraphs\\task_instances\\{self.graph}_pattern{execution_pattern}_{description}',
                   view=True)


def get_case_df_single(tx, dot, lm, ti_case_path, entity_labels, context):
    if context:
        q = f'''
            MATCH (ti:TaskInstance) WHERE ID(ti) IN {ti_case_path}
            MATCH (e1:Event)-[df:DF {{EntityType: "{entity_labels[1][0]}"}}]-(e2:Event)<-[:CONTAINS]-(ti)
            WITH DISTINCT df
            MATCH (e1)-[df]->(e2)
            WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
            RETURN e1, df, e2, duration ORDER BY e1.timestamp, e1.idx
            '''
    else:
        q = f'''
            MATCH (e1:Event)<-[:CONTAINS]-(h:TaskInstance) WHERE ID(h) IN {ti_case_path}
            OPTIONAL MATCH (e1:Event)-[df:DF {{EntityType: "{entity_labels[1][0]}"}}]->(e2:Event)
            WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
            RETURN e1, df, e2, duration ORDER BY e1.timestamp, e1.idx
            '''
    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica",
             fontsize=lm.get_node_label_font_size(), margin="0", color=black, style="filled", fillcolor=white,
             fontcolor=black)
    dot.attr("edge", color=medium_blue, penwidth="2", fontname="Helvetica", fontsize="8", fontcolor=medium_blue)

    records = list(tx.run(q))
    record_nr = 1

    for record in records:
        if len(records) == 1:
            c_id = lm.get_case_label(record['e1'][entity_labels[1][1]])
            dot.node(f"c{c_id}", c_id, color=medium_blue, shape="rectangle", height='0.4', fixedsize="false",
                     style="filled",
                     fontsize='24', fillcolor=medium_blue, fontcolor=white)
            dot.edge(f"c{c_id}", str(record["e1"].id), style="dashed", arrowhead="none", color=medium_blue)
            if check_first_event_in_ti_path(tx, record["e1"].id, ti_case_path):
                dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_blue, fontsize="10")
                dot.node(str(record["e2"].id), "")
            else:
                dot.node(str(record["e1"].id), "")
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_blue, fontsize="10")
                dot.node(str(record["e2"].id), lm.get_event_label(record, "e2"))
        elif record_nr == 1:
            c_id = lm.get_case_label(record['e1'][entity_labels[1][1]])
            dot.node(f"c{c_id}", c_id, color=medium_blue, shape="rectangle", height='0.4', fixedsize="false", style="filled",
                     fontsize='24', fillcolor=medium_blue, fontcolor=white)
            dot.edge(f"c{c_id}", str(record["e1"].id), style="dashed", arrowhead="none", color=medium_blue)
            if context and not check_df_for_other_entity(tx, record["e1"].id, record["e2"].id, entity_labels[0][0]):
                dot.node(str(record["e1"].id), "")
                dot.edge(str(record["e1"].id), str(record["e2"].id))
            else:
                dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_blue, fontsize="10")
        elif record_nr == len(records):
            dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
            if context and check_df_for_other_entity(tx, record["e1"].id, record["e2"].id, entity_labels[0][0]):
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_blue, fontsize="10")
                dot.node(str(record["e2"].id), lm.get_event_label(record, "e2"))
            elif context and not check_df_for_other_entity(tx, record["e1"].id, record["e2"].id, entity_labels[0][0]):
                dot.edge(str(record["e1"].id), str(record["e2"].id))
                dot.node(str(record["e2"].id), "")
        else:
            dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                     fontcolor=dark_blue, fontsize="10")
        record_nr += 1
    return dot


def get_resource_df_single(tx, dot, lm, ti_resource_path, entity_labels, context):
    if context:
        q = f'''
                MATCH (ti:TaskInstance) WHERE ID(ti) IN {ti_resource_path}
                MATCH (e1:Event)-[df:DF {{EntityType: "{entity_labels[0][0]}"}}]-(e2:Event)<-[:CONTAINS]-(ti)
                WITH DISTINCT df
                MATCH (e1)-[df]->(e2)
                WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
                RETURN e1, df, e2, duration ORDER BY e1.timestamp, e1.idx
                '''
    else:
        q = f'''
                MATCH (e1:Event)<-[:CONTAINS]-(h:TaskInstance) WHERE ID(h) IN {ti_resource_path}
                MATCH (e1:Event)-[df:DF {{EntityType: "{entity_labels[0][0]}"}}]->(e2:Event)
                WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
                RETURN e1, df, e2, duration ORDER BY e1.timestamp, e1.idx
                '''
    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica",
             fontsize=lm.get_node_label_font_size(), margin="0", color=black, style="filled", fillcolor=white,
             fontcolor=black)
    dot.attr("edge", color=medium_red, penwidth="2", fontname="Helvetica", fontsize="8", fontcolor=medium_red)

    records = list(tx.run(q))
    record_nr = 1

    for record in records:
        if len(records) == 1:
            r_id = lm.get_case_label(record['e1'][entity_labels[0][1]])
            dot.node(f"r{r_id}", r_id, color=medium_red, shape="rectangle", height='0.4', fixedsize="false",
                     style="filled", fontsize='24', fillcolor=medium_red, fontcolor=white)
            dot.edge(f"r{r_id}", str(record["e1"].id), style="dashed", arrowhead="none", color=medium_red)
            if check_first_event_in_ti_path(tx, record["e1"].id, ti_resource_path):
                dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_red, fontsize="10")
                dot.node(str(record["e2"].id), "")
            else:
                dot.node(str(record["e1"].id), "")
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_red, fontsize="10")
                dot.node(str(record["e2"].id), lm.get_event_label(record, "e2"))
        elif record_nr == 1:
            r_id = lm.get_resource_label(record['e1'][entity_labels[0][1]])
            dot.node(f"r{r_id}", r_id, color=medium_red, shape="rectangle", height='0.4', fixedsize="false", style="filled",
                     fontsize='24', fillcolor=medium_red, fontcolor=white)
            dot.edge(f"r{r_id}", str(record["e1"].id), style="dashed", arrowhead="none", color=medium_red)
            if context and not check_df_for_other_entity(tx, record["e1"].id, record["e2"].id, entity_labels[1][0]):
                dot.node(str(record["e1"].id), "")
                dot.edge(str(record["e1"].id), str(record["e2"].id))
            else:
                dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_red, fontsize="10")
        elif record_nr == len(records):
            dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
            if context and check_df_for_other_entity(tx, record["e1"].id, record["e2"].id, entity_labels[1][0]):
                dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                         fontcolor=dark_red, fontsize="10")
                dot.node(str(record["e2"].id), lm.get_event_label(record, "e2"))
            elif context and not check_df_for_other_entity(tx, record["e1"].id, record["e2"].id, entity_labels[1][0]):
                dot.edge(str(record["e1"].id), str(record["e2"].id))
                dot.node(str(record["e2"].id), "")
        else:
            dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=lm.get_edge_label_duration(record),
                     fontcolor=dark_red, fontsize="10")
        record_nr += 1
    return dot


def get_resource_df_multi(tx, dot, lm, ti_resource_path, case_ids, entity_labels):
    q = f'''
            MATCH (e1:Event)<-[:CONTAINS]-(h:TaskInstance) WHERE ID(h) IN {ti_resource_path}
            MATCH (e1:Event)-[df:DF {{EntityType: "{entity_labels[0][0]}"}}]->(e2:Event)
            WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
            RETURN e1, df, e2, duration ORDER BY e1.timestamp, e1.idx
            '''
    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica",
             fontsize=lm.get_node_label_font_size(), margin="0", color=black, style="filled", fillcolor=white,
             fontcolor=black)
    dot.attr("edge", color=medium_red, penwidth="2", fontname="Helvetica", fontsize="8", fontcolor=medium_red)

    records = list(tx.run(q))
    first_of_subsequence = True
    last_of_subsequence = False
    first_in_path = True
    last_node_id = ""

    for record in records:
        if record['e1']['case'] != record['e2']['case'] and record['e2']['case'] not in case_ids:
            last_of_subsequence = True
        else:
            last_of_subsequence = False
        if first_of_subsequence:
            if first_in_path:
                r_id = lm.get_resource_label(record['e1'][entity_labels[0][1]])
                dot.node(f"r{r_id}", r_id, color=medium_red, shape="rectangle", height='0.4', fixedsize="false",
                         fontsize='24', style="filled", fillcolor=medium_red, fontcolor=white)
                dot.edge(f"r{r_id}", str(record["e1"].id), style="dashed", arrowhead="none", color=medium_red)
                first_in_path = False
            else:
                dot.edge(last_node_id, str(record["e1"].id), style='dashed')
            dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
            last_node_id = str(record["e1"].id)
            if not last_of_subsequence:
                dot.edge(str(record["e1"].id), str(record["e2"].id))
                first_of_subsequence = False
        else:
            dot.node(str(record["e1"].id), lm.get_event_label(record, "e1"))
            last_node_id = str(record["e1"].id)
            if not last_of_subsequence:
                dot.edge(str(record["e1"].id), str(record["e2"].id))
            else:
                first_of_subsequence = True
    return dot


def check_df_for_other_entity(tx, e1_id, e2_id, entity):
    q = f'''
        MATCH (e1:Event)-[df:DF {{EntityType: "{entity}"}}]->(e2:Event) 
        WHERE ID(e1) = {e1_id} AND ID(e2) = {e2_id}
        RETURN count(df)
        '''
    if [record["count(df)"] for record in list(tx.run(q))][0] == 1:
        last_in_entity = True
    else:
        last_in_entity = False
    return last_in_entity


def check_first_event_in_ti_path(tx, e1_id, ti_id_path):
    q = f'''
        MATCH (e1:Event)<-[c:CONTAINS]-(ti:TaskInstance) 
        WHERE ID(e1) = {e1_id} AND ID(ti) IN {ti_id_path}
        RETURN count(c)
        '''
    if [record["count(c)"] for record in list(tx.run(q))][0] == 1:
        first_event_ti_path = True
    else:
        first_event_ti_path = False
    return first_event_ti_path


def get_ti_path_from_case(tx, case_id):
    q = f'''
        MATCH (h:TaskInstance) WHERE h.cID = "{case_id}"
        WITH ID(h) AS h
        RETURN h
        '''
    ti_path = [record["h"] for record in list(tx.run(q))]
    return ti_path


def get_ti_path_from_resource(tx, resource_id, ti_list):
    q = f'''
        MATCH (h:TaskInstance) WHERE h.rID = "{resource_id}" AND ID(h) IN {ti_list}
        WITH ID(h) AS h
        RETURN h
        '''
    ti_path = [record["h"] for record in list(tx.run(q))]
    return ti_path


def get_full_ti_id_path_pattern_2(tx, ti_id_path, entity_labels):
    q = f'''
        MATCH p = (ti1:TaskInstance)-[:DF_TI*]->(ti2:TaskInstance) WHERE ID(ti1) = {ti_id_path[0]}
        AND ID(ti2) = {ti_id_path[len(ti_id_path) - 1]} 
            AND all(r IN relationships(p) WHERE (r.EntityType = '{entity_labels[0][0]}'))
        WITH [ti IN nodes(p)| ID(ti)] AS ti_path, [ti IN nodes(p)| ti.cID] AS case_path
        RETURN ti_path, case_path
        '''
    ti_full_path = [record["ti_path"] for record in list(tx.run(q))][0]
    case_path = [record["case_path"] for record in list(tx.run(q))][0]
    return ti_full_path, case_path


def get_full_ti_id_path_pattern_3(tx, ti_id_path, entity_labels):
    q = f'''
        MATCH p = (ti1:TaskInstance)-[:DF_TI*]->(ti2:TaskInstance) WHERE ID(ti1) = {ti_id_path[0]}
        AND ID(ti2) = {ti_id_path[len(ti_id_path) - 1]} 
            AND all(r IN relationships(p) WHERE (r.EntityType = '{entity_labels[1][0]}'))
        WITH [ti IN nodes(p)| ID(ti)] AS ti_path, [ti IN nodes(p)| ti.rID] AS resource_path
        RETURN ti_path, resource_path
        '''
    ti_full_path = [record["ti_path"] for record in list(tx.run(q))][0]
    resource_path = [record["resource_path"] for record in list(tx.run(q))][0]
    return ti_full_path, resource_path


def get_case_path_batch_pattern(tx, ti_id_path, resource_label):
    q = f'''
            MATCH p = (ti1:TaskInstance)-[:DF_TI*]->(ti2:TaskInstance) WHERE ID(ti1) = {ti_id_path[0]}
            AND ID(ti2) = {ti_id_path[len(ti_id_path) - 1]} 
                AND all(r IN relationships(p) WHERE (r.EntityType = '{resource_label}'))
            WITH [ti IN nodes(p)| ti.cID] AS case_path
            RETURN case_path
            '''
    case_path = [record["case_path"] for record in list(tx.run(q))][0]
    return case_path


def get_resource_ids(tx, ti_list):
    q = f'''
        MATCH (h:TaskInstance) WHERE ID(h) IN {ti_list}
        WITH h.rID AS resource
        RETURN DISTINCT resource
        '''
    resource_ids = [record["resource"] for record in list(tx.run(q))]
    return resource_ids
