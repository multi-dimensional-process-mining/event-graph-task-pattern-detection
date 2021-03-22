from neo4j import GraphDatabase
from graphviz import Digraph
import pydot
from bpic2017_dictionaries import abbr_dictionary

### begin config

# data_set = "bpic2014"
data_set = "bpic2017"
abbreviate_node_label = True
print_duration = False

pattern_nr = 7

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
    if pattern_nr == 1:
        ti_path = [384063]
    elif pattern_nr == 2:
        ti_path = [246061]
    elif pattern_nr == 3:
        ti_path = [246146, 246152, 246155]
    elif pattern_nr == 4:
        ti_path = [384066, 386022, 248811]
    elif pattern_nr == 10:
        ti_path = [384073, 384074, 384075, 384083, 384086]
    elif pattern_nr == 12:
        ti_path = [252013, 252751, 253544, 253553, 253762]
elif data_set == "bpic2017":
    entities = [["Case_R", "resource"],
                ["Case_AWO", "case"]]
    string_start = 5
    if pattern_nr == 1:
        ti_path = [1248848]
    elif pattern_nr == 4:
        ti_path = [894091]
    elif pattern_nr == 3:
        # ti_path = [904505, 1250909, 904506]
        # ti_path = [897217, 897218, 897219]
        ti_path = [956439, 1252030, 956440]
        # ti_path = [954285, 954286, 954287]
    elif pattern_nr == 2:
        # ti_path = [1250690, 1251206, 894115]
        # ti_path = [897913, 1068356, 897914]
        # ti_path = [900901, 1078467, 900913]
        ti_path = [1250869, 1045783, 902013]
    elif pattern_nr == 7:
        # ti_path = [1249222, 1256841, 1250856, 1252939, 1253513]
        # ti_path = [1249385, 1257479, 1257254, 1251111, 1256621]
        # ti_path = [1249526, 1251131, 1255590, 1254378, 1255154]
        ti_path = [1250148, 1256650, 1253190, 1258037, 1259362]
    elif pattern_nr == 8:
        # ti_path = [894086, 1044027, 1218072, 1438377, 1443211]
        ti_path = [894289, 1063421, 1003526, 1036614, 1075183]
        # ti_path = [894366, 1233956, 957394, 1199491, 1444530]
        # ti_path = [894680, 1054056, 921707, 1440766, 1452860]
        # ti_path = [895093, 1195935, 1235881, 1228144, 1231380]


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
    if print_duration:
        str_duration = f"{(duration.hours_minutes_seconds[0] * 60) + duration.hours_minutes_seconds[1]}m{duration.hours_minutes_seconds[2]:.0f}s"
    else:
        str_duration = ""
    return str_duration


def get_case_df(tx, ti_case_path):
    q = f'''
        MATCH (ti:TaskInstance) WHERE ID(ti) IN {ti_case_path}
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
            c_id = str(record['e1'][entities[1][1]])[12:]
            # c_id = str(record['e1'][entities[1][1]]).replace("_", "\n")
            # c_id = str(record['e1'][entities[1][1]]).replace("_", "<br align=\"left\"/>")
            dot.node(c_id, c_id, color=medium_blue, shape="rectangle", fixedsize="false", height='0.4', style="filled",
                     fontsize='24', fillcolor=medium_blue, fontcolor=white)
            # dot.node(c_id, f"<<B>{c_name}</B>>", color=white, shape="square",fixedsize="true", style="filled", fillcolor=white, fontcolor=medium_blue, fontsize="18")
            # dot.node(c_id, "", color=white, shape="square", fixedsize="true", style="filled", fillcolor=white)
            dot.edge(c_id, str(record["e1"].id), style="dashed", arrowhead="none", color=medium_blue)
            # dot.edge(c_id, str(record["e1"].id), style="dashed", xlabel=f"<<B>{c_name}   </B>>", fontsize='24', arrowhead="none", color=medium_blue)
            # dot.edge(c_id, str(record["e1"].id), style="dashed", xlabel=c_name, fontsize='18', arrowhead="none", color=medium_blue)
            dot.node(str(record["e1"].id), "")
            dot.edge(str(record["e1"].id), str(record["e2"].id))
        elif record_nr == len(records):
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
            dot.node(str(record["e2"].id), "")
        else:
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=get_string_duration(record, "duration"), fontcolor=dark_blue, fontsize="10")
            dot.node(str(record["e2"].id), "")
        record_nr += 1


def get_resource_df(tx, ti_resource_path):
    q = f'''
        MATCH (ti:TaskInstance) WHERE ID(ti) IN {ti_resource_path}
        MATCH (e1:Event)-[df:DF {{EntityType: "{entities[0][0]}"}}]-(e2:Event)<-[:CONTAINS]-(ti)
        WITH DISTINCT df
        MATCH (e1)-[df]->(e2)
        WITH e1, df, e2, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration
        RETURN e1, df, e2, duration ORDER BY e1.timestamp
        '''

    dot.attr("node", shape="square", fixedsize="true", width="0.6", height="0.6", fontname="Helvetica", fontsize="30",
             margin="0", color=black, style="filled", fillcolor=white, fontcolor=black)
    dot.attr("edge", color=medium_red, penwidth="2", fontname="Helvetica", fontsize="8", fontcolor=medium_red)

    records = list(tx.run(q))
    record_nr = 1

    for record in records:
        # print(record)
        if record_nr == 1:
            r_id = str(record['e1'][entities[0][1]])[5:]
            # r_id = str(record['e1'][entities[0][1]]).replace("_", "\n")
            # r_id = str(record['e1'][entities[0][1]]).replace("_", "<br align=\"left\"/>")
            dot.node(r_id, r_id, color=medium_red, shape="rectangle", height='0.4', fixedsize="false", style="filled",
                     fontsize='24', fillcolor=medium_red, fontcolor=white)
            # dot.node(r_id, f"<<B>{r_name}</B>>", color=white, fixedsize="true", style="filled", fillcolor=white, fontcolor=medium_red, fontsize="18")
            # dot.node(r_id, "", color=white, fixedsize="true", style="filled", fillcolor=white, fontcolor=medium_red, fontsize="18")
            dot.edge(r_id, str(record["e1"].id), style="dashed", arrowhead="none", color=medium_red)
            # dot.edge(r_id, str(record["e1"].id), style="dashed", xlabel=f"<<B>{r_name}   </B>>", fontsize='24', arrowhead="none", color=medium_red)
            # dot.edge(r_id, str(record["e1"].id), style="dashed", xlabel=r_name, fontsize='18', arrowhead="none", color=medium_red)
            dot.node(str(record["e1"].id), "")
            dot.edge(str(record["e1"].id), str(record["e2"].id))
        elif record_nr == len(records):
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id))
            dot.node(str(record["e2"].id), "")
        else:
            dot.node(str(record["e1"].id), get_node_label_event(record, "e1"))
            dot.edge(str(record["e1"].id), str(record["e2"].id), xlabel=get_string_duration(record, "duration"), fontcolor=dark_red, fontsize="10")
            dot.node(str(record["e2"].id), "")
        record_nr += 1


dot = Digraph(comment='Query Result')
# dot.attr("graph", rankdir="LR", margin="0")
dot.attr("graph", rankdir="LR", margin="0", ranksep="0.5", nodesep="0.5")

with driver.session() as session:
    if pattern_nr == 1 or pattern_nr == 4:
        session.read_transaction(get_case_df, ti_path)
        session.read_transaction(get_resource_df, ti_path)
    elif pattern_nr == 3:
        session.read_transaction(get_case_df, ti_path)
        session.read_transaction(get_resource_df, [ti_path[0], ti_path[2]])
        session.read_transaction(get_resource_df, [ti_path[1]])
    elif pattern_nr == 2:
        session.read_transaction(get_resource_df, ti_path)
        session.read_transaction(get_case_df, [ti_path[0], ti_path[2]])
        session.read_transaction(get_case_df, [ti_path[1]])
    elif pattern_nr == 7 or pattern_nr == 8:
        for ti in range(len(ti_path)):
            session.read_transaction(get_case_df, [ti_path[ti]])
        session.read_transaction(get_resource_df, ti_path)

    (graph,) = pydot.graph_from_dot_data(dot.source)
    graph.write_png(f"graph_results\\taxonomy_pattern_examples\\n{data_set}_tax_pattern_{pattern_nr}.png")
    # graph.write_pdf(f"graph_results\\taxonomy_pattern_examples\\{data_set}_tax_pattern_{pattern_nr}.pdf")
