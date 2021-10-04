from GraphConfigurator import GraphConfigurator
import PreprocessSelector
from constructors.EventGraphConstructor import EventGraphConstructor
from constructors.HighLevelEventConstructor import HighLevelEventConstructor
from GraphExplorer import GraphExplorer
from PatternMeasurer import PatternMeasurer

# -------------- BEGIN CONFIG -----------------

# TO START:
# specify the name of the graph:
graphs = ["bpic2017_single_ek",
          "bpic2014_single_ek",
          "operators"]

graph = graphs[0]
# and configure all the settings related to this graph name in "graph_confs.py"
gc = GraphConfigurator(graph)

# IF STARTING FROM SCRATCH (without event graph constructed in neo4j)
# (1) set "step_preprocess" and "step_create_event_graph" to true:
step_preprocess = False
step_create_event_graph = False
# (2) create graph in Neo4j (with same password as specified in "graph_confs.py")
#     and allocate enough memory: set dbms.memory.heap.max_size=20G
# (3) specify path to import directory of neo4j database:
path_to_neo4j_import_directory = 'C:\\Users\\s111402\\.Neo4jDesktop\\relate-data\dbmss\\' \
                                 'dbms-a78e4f94-921d-402d-b4b6-0bddc8d8e980\\import\\'

# IF STARTING FROM SCRATCH OR FROM AN EVENT GRAPH PRECONSTRUCTED:
# set "step_construct_high_level_events" to true to construct high level events:
step_construct_high_level_events = False

# IF EVENT GRAPH AND HIGH LEVEL EVENTS ARE ALREADY IN PLACE:
# set "step_explore_patterns" to True to explore the event graph for task executions and subgraphs of instances thereof:
step_explore_patterns = True
# set "step_explore_cases" to True to explore subgraphs of case-paths:
step_explore_cases = False
# set "step_measure_patterns" to measure various statistics of the task pattern executions in the event graph:
step_measure_patterns = False  # the list of measurements (which patterns, time constraints) can be configured in "graph_confs.py"

print_duration = False  # print duration on edges in subgraph
use_abbreviated_event_names = False  # print abbreviated activity names in subgraph (only possible for bpic2017 data)

# --------------- END CONFIG ------------------

if step_preprocess:
    PreprocessSelector.get_preprocessor(graph, gc.get_filename(), gc.get_column_names(), gc.get_separator(),
                                        gc.get_timestamp_format(), path_to_neo4j_import_directory).preprocess()

if step_create_event_graph:
    EventGraphConstructor(gc.get_password(), path_to_neo4j_import_directory, graph) \
        .construct_single()

if step_construct_high_level_events:
    HighLevelEventConstructor(gc.get_password(), graph, gc.get_entity_labels(), gc.get_action_lifecycle_labels()) \
        .construct_single()

if step_explore_patterns:
    GraphExplorer(graph, gc.get_password(), gc.get_name_data_set(), gc.get_entity_labels(),
                  gc.get_action_lifecycle_labels(), gc.get_timestamp_label(), print_duration,
                  use_abbreviated_event_names).explore_patterns()

if step_explore_cases:
    GraphExplorer(graph, gc.get_password(), gc.get_name_data_set(), gc.get_entity_labels(),
                  gc.get_action_lifecycle_labels(), gc.get_timestamp_label(), print_duration,
                  use_abbreviated_event_names).explore_cases()

if step_measure_patterns:
    PatternMeasurer(graph, gc.get_password(), gc.get_entity_labels(), gc.get_total_events()) \
        .get_all_pattern_measurements(gc.get_pm_selection())
