from neo4j import GraphDatabase
from graphviz import Digraph
import pydot
from bpic2017_dictionaries import abbr_dictionary

### begin config

# data_set = "bpic2014"
data_set = "bpic2017"
abbreviate_node_label = True

# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", data_set))

# COLORS
white = "#ffffff"
black = "#000000"

dark_yellow = "#ffc000"

medium_red = '#d73027'
dark_red = '#570000'

medium_blue = '#4575b4'
dark_blue = '#002759'

# Dataset settings
if data_set == "bpic2014":
    string_start = 4
    entities = [["Resource", "AssignmentGroup"],
                ["Incident", "IncidentID"]]
elif data_set == "bpic2017":
    entities = [["Case_R", "resource"],
                ["Case_AWO", "case"]]
    string_start = 5


step_set_pattern_id = True


def set_pattern_id(tx):
    q_set_pattern_id = f'''
        MATCH (ti:TaskInstance)
        WITH DISTINCT ti.path AS path, count(*) AS count
        ORDER BY count DESC
        WITH collect(path) as paths
        UNWIND range(0, size(paths)-1) as pos
        WITH paths[pos] AS path, pos+1 AS rank
        MATCH (ti:TaskInstance {{path:path}})
        SET ti.ID = rank 
        '''
    print(q_set_pattern_id)
    tx.run(q_set_pattern_id)


def get_ti_path_from_pattern_id(tx, pattern_id):
    q = f'''
        MATCH (ti:TaskInstance) WHERE ti.ID = {pattern_id}
        WITH ID(ti) AS ti
        RETURN ti LIMIT 1
        '''
    ti = list(tx.run(q))[0]['ti']
    return ti


def get_node_label_event(record, element):
    if data_set == "bpic2017":
        if abbreviate_node_label:
            node_label = abbr_dictionary.get(record[element]["Activity_Lifecycle"])
        else:
            node_label = record[element]["Activity"][0:1] + '\n' + record[element]["Activity"][2:8] \
                         + '\n' + record[element]["lifecycle"][0:4]
    else:
        activity_name = record[element]["Activity"]
        space_positions = [pos for pos, char in enumerate(activity_name) if char == " "]
        if not space_positions:
            node_label = activity_name[0:6]
            print(node_label)
        elif len(space_positions) == 1:
            node_label = activity_name[0:min(5, space_positions[0])] + "\n" \
                           + activity_name[space_positions[0] + 1:space_positions[0] + 6]
            print(node_label)
        elif len(space_positions) > 1:
            node_label = activity_name[0:min(4, space_positions[0])] + "\n" \
                           + activity_name[space_positions[0] + 1:min(space_positions[0] + 7, space_positions[1])] \
                           + "\n" + activity_name[space_positions[1] + 1:space_positions[1] + 5]
            print(node_label)
    return node_label


def get_case_df(tx, ti_case_path):

    q = f'''
        MATCH (ti:TaskInstance) WHERE ID(ti) = {ti_case_path}
        MATCH (e1:Event)-[df:DF {{EntityType: "{entities[1][0]}"}}]-(e2:Event)<-[:CONTAINS]-(ti)
        WITH DISTINCT df
        MATCH (e1)-[df]->(e2)
        WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
        RETURN e1, df, e2, duration ORDER BY e1.timestamp
        '''

    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica", fontsize="30",
             margin="0", color=black, style="filled", fillcolor=white, fontcolor=black)
    dot.attr("edge", color=medium_blue, penwidth="2", fontname="Helvetica", fontsize="8", fontcolor=medium_blue)

    records = list(tx.run(q))
    record_nr = 1

    for record in records:
        # print(record)
        if record_nr == 1:
            c_id = str(record['e1'][entities[1][1]]).replace("_", "\n")
            c_name = "c"
            # dot.node(c_id, "", color=white, shape="square", fixedsize="true", style="filled", fillcolor=white)
            # dot.edge(c_id, str(record["e1"].id), style="dashed", xlabel=f"<<B>{c_name}   </B>>", fontsize='24', arrowhead="none", color=medium_blue)
            dot.node(c_id, c_name, color=medium_blue, shape="rectangle", height='0.4', fixedsize="false", fontsize='24', style="filled", fillcolor=medium_blue, fontcolor=white)
            dot.edge(c_id, str(record["e1"].id), style="dashed", arrowhead="none", color=medium_blue)
            dot.node(str(record["e1"].id), "")
            dot.edge(str(record["e1"].id), str(record["e2"].id))
        elif record_nr == len(records):
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
            dot.node(str(record["e2"].id), "")
        else:
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
            dot.node(str(record["e2"].id), "")
        record_nr += 1


def get_resource_df(tx, ti_resource_path):

    q = f'''
        MATCH (ti:TaskInstance) WHERE ID(ti) = {ti_resource_path}
        MATCH (e1:Event)-[df:DF {{EntityType: "{entities[0][0]}"}}]-(e2:Event)<-[:CONTAINS]-(ti)
        WITH DISTINCT df
        MATCH (e1)-[df]->(e2)
        WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
        RETURN e1, df, e2, duration ORDER BY e1.timestamp
        '''

    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica", fontsize="30",
             margin="0", color=black, style="filled", fillcolor=white, fontcolor=black)
    dot.attr("edge", color=medium_red, penwidth="2", fontname="Helvetica", fontsize="8",fontcolor=medium_red)

    records = list(tx.run(q))
    record_nr = 1

    for record in records:
        # print(record)
        if record_nr == 1:
            r_id = str(record['e1'][entities[0][1]])
            r_name = "r"
            dot.node(r_id, r_name, color=medium_red, shape="rectangle", height='0.4', fixedsize="false", fontsize='24', style="filled", fillcolor=medium_red, fontcolor=white)
            dot.edge(r_id, str(record["e1"].id), style="dashed", arrowhead="none", color=medium_red)
            # dot.node(r_id, "", color=white, shape="square", fixedsize="true", style="filled", fillcolor=white)
            # dot.edge(r_id, str(record["e1"].id), style="dashed", xlabel=f"<<B>{r_name}   </B>>", fontsize='24',
            #          arrowhead="none", color=medium_red)
            dot.node(str(record["e1"].id), "")
            dot.edge(str(record["e1"].id), str(record["e2"].id))
        elif record_nr == len(records):
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
            dot.node(str(record["e2"].id), "")
        else:
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
            dot.node(str(record["e2"].id), "")
        record_nr += 1


with driver.session() as session:
    if step_set_pattern_id:
        session.read_transaction(set_pattern_id)

    for pattern_id in range(1, 56):

        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", ranksep="0.4", nodesep="0.6")

        ti_path = session.read_transaction(get_ti_path_from_pattern_id, pattern_id)
        session.read_transaction(get_case_df, ti_path)
        session.read_transaction(get_resource_df, ti_path)

        (graph,) = pydot.graph_from_dot_data(dot.source)
        graph.write_pdf(f"graph_results\\elementary_patterns\\{data_set}_elementary_pattern_{pattern_id}.pdf")
