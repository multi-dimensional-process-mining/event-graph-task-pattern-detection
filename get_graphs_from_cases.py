from neo4j import GraphDatabase
from graphviz import Digraph
import pydot
from bpic2017_dictionaries import abbr_dictionary

### begin config

# data_set = "bpic2014"
data_set = "bpic2017"
abbreviate_node_label = True

example_nr = 1

descriptions = ["example_three_cases", "example_5_cases_pattern_10", "example_cases_pattern_4"]
case_id_descriptions = [["Application_2018242966", "Application_83648929", "Application_1303359620"],
                        ["Application_1111458873", "Application_1372864243", "Application_206394826",
                         "Application_1877008365", "Application_1992048266"],
                        ["Application_1302702228", "Application_1383850952"]]


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


def get_string_duration(record, element):
    duration = record[element]
    if duration.hours_minutes_seconds[0] > 0:
        str_duration = f"{duration.hours_minutes_seconds[0]}:{duration.hours_minutes_seconds[1]}:" \
                       f"{duration.hours_minutes_seconds[2]:.1f} "
    else:
        str_duration = f"{duration.hours_minutes_seconds[1]}:{duration.hours_minutes_seconds[2]:.1f} "
    return str_duration


def get_ti_case_path(tx, case_id):
    q = f'''
        MATCH (h:TaskInstance) WHERE h.cID = "{case_id}"
        WITH ID(h) AS h
        RETURN h
        '''
    ti_case_path = [record["h"] for record in list(tx.run(q))]
    # print(ti_case_path)
    return ti_case_path


def get_ti_resource_path(tx, resource_id, ti_list):
    q = f'''
        MATCH (h:TaskInstance) WHERE h.rID = "{resource_id}" AND ID(h) IN {ti_list}
        WITH ID(h) AS h
        RETURN h
        '''
    ti_resource_path = [record["h"] for record in list(tx.run(q))]
    print(ti_resource_path)
    return ti_resource_path


def get_resource_ids(tx, ti_list):
    q = f'''
        MATCH (h:TaskInstance) WHERE ID(h) IN {ti_list}
        WITH h.rID AS resource
        RETURN DISTINCT resource
        '''
    resource_ids = [record["resource"] for record in list(tx.run(q))]
    print(resource_ids)
    return resource_ids


def get_case_df(tx, ti_case_path):
    q = f'''
        MATCH (e1:Event)<-[:CONTAINS]-(h:TaskInstance) WHERE ID(h) IN {ti_case_path}
        OPTIONAL MATCH (e1:Event)-[df:DF {{EntityType: "{entities[1][0]}"}}]->(e2:Event)
        WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
        RETURN e1, df, e2, duration ORDER BY e1.timestamp
        '''
    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica", fontsize="30",
             margin="0", color=black, style="filled", fillcolor=white, fontcolor=black)
    dot.attr("edge", color=medium_blue, penwidth="2", fontname="Helvetica", fontsize="8", fontcolor=medium_blue)

    records = list(tx.run(q))
    record_nr = 1

    for record in records:
        if record_nr == 1:
            c_id = str(record['e1'][entities[1][1]])[12:]
            dot.node(c_id, c_id, color=medium_blue, shape="rectangle", height='0.4', fixedsize="false", style="filled",
                     fontsize='24', fillcolor=medium_blue, fontcolor=white)
            # dot.node(c_id, "", color=white, fixedsize="true", style="filled", fillcolor=white, fontcolor=medium_red,
            #          fontsize="18")
            dot.edge(c_id, str(record["e1"].id), style="dashed", arrowhead="none", color=medium_blue)
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
        elif record_nr == len(records):
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
        else:
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
            dot.node(str(record["e2"].id), get_node_label_event(record, "e2"))
        record_nr += 1


def get_resource_df(tx, ti_resource_path):
    q = f'''
            MATCH (e1:Event)<-[:CONTAINS]-(h:TaskInstance) WHERE ID(h) IN {ti_resource_path}
            MATCH (e1:Event)-[df:DF {{EntityType: "{entities[0][0]}"}}]->(e2:Event)
            WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
            RETURN e1, df, e2, duration ORDER BY e1.timestamp
            '''
    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica", fontsize="30",
             margin="0", color=black, style="filled", fillcolor=white, fontcolor=black)
    dot.attr("edge", color=medium_red, penwidth="2", fontname="Helvetica", fontsize="8", fontcolor=medium_red)

    records = list(tx.run(q))
    first_of_subsequence = True
    last_of_subsequence = False
    first_in_path = True
    last_node_id = ""

    for record in records:
        if record['e1']['case'] != record['e2']['case'] and record['e2']['case'] not in case_id_descriptions[example_nr]:
            last_of_subsequence = True
        else:
            last_of_subsequence = False
        if first_of_subsequence:
            if first_in_path:
                r_id = str(record['e1'][entities[0][1]])[5:]
                dot.node(r_id, r_id, color=medium_red, shape="rectangle", height='0.4', fixedsize="false",
                         fontsize='24', style="filled", fillcolor=medium_red, fontcolor=white)
                # dot.node(r_id, "", color=white, fixedsize="true", style="filled", fillcolor=white, fontcolor=medium_red,
                #          fontsize="18")
                dot.edge(r_id, str(record["e1"].id), style="dashed", arrowhead="none", color=medium_red)
                first_in_path = False
            else:
                dot.edge(last_node_id, str(record["e1"].id), style='dashed')
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            last_node_id = str(record["e1"].id)
            if not last_of_subsequence:
                dot.edge(str(record["e1"].id), str(record["e2"].id))
                first_of_subsequence = False
        else:
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            last_node_id = str(record["e1"].id)
            if not last_of_subsequence:
                dot.edge(str(record["e1"].id), str(record["e2"].id))
            else:
                first_of_subsequence = True


dot = Digraph(comment='Query Result')
dot.attr("graph", rankdir="LR", margin="0", ranksep="0.5", nodesep="0.5")

with driver.session() as session:

    ti_list = []

    for case_id in case_id_descriptions[example_nr]:
        ti_case_path = session.read_transaction(get_ti_case_path, case_id)
        session.read_transaction(get_case_df, ti_case_path)
        ti_list = ti_list + ti_case_path

    resource_ids = session.read_transaction(get_resource_ids, ti_list)
    for resource_id in resource_ids:
        ti_resource_path = session.read_transaction(get_ti_resource_path, resource_id, ti_list)
        session.read_transaction(get_resource_df, ti_resource_path)

    (graph,) = pydot.graph_from_dot_data(dot.source)
    # graph.write_png(f"graph_results\\graphs_from_cases\\{data_set}_{descriptions[example_nr]}.png")
    graph.write_pdf(f"graph_results\\graphs_from_cases\\{data_set}_{descriptions[example_nr]}.pdf")