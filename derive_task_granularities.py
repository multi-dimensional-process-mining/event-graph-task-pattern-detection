from neo4j import GraphDatabase
import pandas as pd
import re
from graphviz import Digraph
from bpic2017_dictionaries import color_dict_application, color_dict_offer, color_dict_workflow
import pydot

# DATASET & FILTER SETTINGS
data_set = "bpic2017"
string_start = 5

# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", data_set))

granularity_output_directory = f"task-granularities\\{data_set}"
# granularity = "atomic"
granularity = "composite"


def get_all_task_paths(tx):
    q = f'''
        MATCH (c:Class) WHERE EXISTS(c.path)
        WITH c.path_length AS length, c.path AS path, c.rank AS rank
        RETURN length, path, rank ORDER BY length ASC
        '''
    result = tx.run(q)
    df_task_paths = pd.DataFrame([dict(record) for record in result]).set_index('rank')
    return df_task_paths


def get_task_granularities(df_task_paths):
    atomic_tasks = []
    atomic_task_variants = []
    composite_tasks = []

    for index, row in df_task_paths.iterrows():
        row['string_path'] = ''.join(map(str, row['path'])).replace("+", "").replace("(", "").replace(")", "")
        if len(row['path']) == 1:
            df_task_paths.loc[index, 'granularity'] = "atomic"
            atomic_tasks.append(row['string_path'])
        else:
            placeholder = "0" * len(row['string_path'])
            for task in atomic_tasks:
                string = row['string_path']
                positions = [match.start() for match in re.finditer(task, string)]
                if positions:
                    for position in positions:
                        placeholder = placeholder[:position] + ("1" * len(task)) + placeholder[(position + len(task)):]
                if placeholder.find("0") == -1:
                    break
            if placeholder.find("0") == -1:
                composite_tasks.append(row['string_path'])
                df_task_paths.loc[index, 'granularity'] = "composite"
            elif placeholder.find("0") != -1:
                atomic_tasks.append(row['string_path'])
                df_task_paths.at[index, 'granularity'] = "atomic"
                if placeholder.find("1") != -1:
                    atomic_task_variants.append(row['string_path'])
                    df_task_paths.loc[index, 'granularity'] = "atomic_variant"
    df_task_paths.sort_index(inplace=True)
    df_task_paths = df_task_paths[['granularity', 'length', 'path']]
    return df_task_paths


def analyze_granularity(df_task_paths):
    for length in range(df_task_paths['length'].max()):
        print(f"Length {length}: {len(df_task_paths[(df_task_paths['length'] == length) & ((df_task_paths['granularity'] == 'atomic') | (df_task_paths['granularity'] == 'atomic_variant'))])}")


def get_atomic_composite_tasks_per_length(df_task_paths):
    if granularity == "atomic":
        graph_rows = 10
        df_tasks = df_task_paths[((df_task_paths['granularity'] == 'atomic') | (df_task_paths['granularity'] == 'atomic_variant')) & (df_task_paths.index < 156)]
    else:
        graph_rows = 11
        df_tasks = df_task_paths[(df_task_paths['granularity'] == 'composite') & (df_task_paths.index < 156)]
    df_tasks.reset_index(level=0, inplace=True)
    lengths = df_tasks['length'].unique()
    for length in lengths:
        df_tasks_per_length = df_tasks[df_tasks['length'] == length]
        df_tasks_per_length.reset_index(drop=True, inplace=True)
        full_graph_columns, remainder = divmod(len(df_tasks_per_length), graph_rows)

        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", nodesep="0.25", ranksep="0.05")
        dot.attr("node", fixedsize="true", fontname="Helvetica", fontsize="11", margin="0")

        for g_row in reversed(range(remainder, graph_rows)):

            with dot.subgraph() as s:
                s.attr(newrank="True")
                last_node_id = ""

                for g_col in range(full_graph_columns):
                    last_node_id = get_graph_cell(s, df_tasks_per_length, graph_rows, g_col, g_row, last_node_id)

        for g_row in reversed(range(remainder)):

            with dot.subgraph() as s:
                s.attr(newrank="True")
                last_node_id = ""

                for g_col in range(full_graph_columns + 1):
                    last_node_id = get_graph_cell(s, df_tasks_per_length, graph_rows, g_col, g_row, last_node_id)

        (graph,) = pydot.graph_from_dot_data(dot.source)
        graph.write_png(f"{granularity_output_directory}\\{granularity}_tasks_length{length}.png")


def get_graph_cell(s, df_atomic_tasks_per_length, graph_rows, g_col, g_row, last_node_id):
    node_id = 1
    path = df_atomic_tasks_per_length.loc[(g_row + g_col * graph_rows), 'path']
    rank = df_atomic_tasks_per_length.loc[(g_row + g_col * graph_rows), 'rank']

    if g_col != 0:
        s.edge(last_node_id, f'{rank}_0', style="invis")
        s.node(f'{rank}_0', "", shape="rect", width="0.3", color="white", penwidth=str(0.5))
        s.edge(f'{rank}_0', str(rank), style="invis")

    s.node(str(rank), f'{rank}\l', shape="rect", width="0.3", color="white", penwidth=str(0.5))
    s.edge(str(rank), f'{rank}_{node_id}', style="invis")

    for event in path[:-1]:
        s.node(f'{rank}_{node_id}', "", style="filled", shape=get_node_properties(event)[0],
               width=get_node_properties(event)[1], height=get_node_properties(event)[2],
               fillcolor=get_node_properties(event)[3], fontcolor="black", penwidth=str(0.5))
        s.edge(f'{rank}_{node_id}', f'{rank}_{node_id + 1}', style="invis")
        node_id += 1
    s.node(f'{rank}_{node_id}', "", style="filled", shape=get_node_properties(path[-1])[0],
           width=get_node_properties(path[-1])[1], height=get_node_properties(path[-1])[2],
           fillcolor=get_node_properties(path[-1])[3], fontcolor="black", penwidth=str(0.5))

    last_node_id = f'{rank}_{node_id}'

    return last_node_id


def get_node_properties(event):
    if event[0] == "A":
        node_properties = ["square", "0.3", "0.3", color_dict_application.get(event)]
    elif event[0] == "W":
        node_properties = ["rect", "0.15", "0.3", color_dict_workflow.get(event)]
    elif event[0] == "O":
        node_properties = ["circle", "0.3", "0.3", color_dict_offer.get(event)]
    return node_properties


with driver.session() as session:
    task_paths = session.read_transaction(get_all_task_paths)
    task_paths = get_task_granularities(task_paths)
    # analyze_granularity(task_paths)
    get_atomic_composite_tasks_per_length(task_paths)
