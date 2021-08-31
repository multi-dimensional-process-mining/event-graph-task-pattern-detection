# task-pattern-detecton

*Detecting and exploring task execution patterns in event graphs.*

 ## Description
 
This repository collects scripts for detecting, exploring and measuring task execution patterns (see [1]) in event graphs (see [2]).

[1] Klijn, E.L., Mannhardt, F., Fahland, D.: Classifying and Detecting Task Executions and Routines in Processes Using Event Graphs. In: Business Process Management Forum. pp. 212–229. (2021)

[2] Esser, S., Fahland, D.: Multi-Dimensional Event Data in Graph Databases. J Data Semant. 10, 109–141 (2021).

## Software

The following software is recommended to be used for experimental replication:
* Python 3.7
* Installation of the following python libraries: pandas 1.2.4, neo4j-python-driver 4.2.1, graphviz 2.38, tabulate 0.8.9 and numpy 1.19.2
* Neo4j Desktop with Neo4j Database version 3.5.4 or higher

## Quick start

*Pre-implemented for BPIC2014 and BPIC2017* 

##### 1. Constructing the event graph from scratch
1.	Create a graph in Neo4j Desktop. Set password = “bpic2017” or “bpic2014” depending on the dataset. Increase memory heap to 20GB in the settings of the graph.
2.	Specify the correct neo4j import folder in main.py.
3.	Select “bpic2017_single_ek” for BPIC2017 or “bpic2014_single_ek” for BPIC2014 in main.py and assign it to the variable “graph”.
4.	Set “step_preprocess”, “step_create_event_graph” and “step_construct_high_level_events” all to True (set these steps to False if you want to run the toolkit again for graph exploration but have already performed these steps for this neo4j graph).
5.	Set remaining steps to False.
6.	Run main.py.

##### 2. Exploring the graph for task execution patterns
1. Set “step_explore_patterns” to True. (And remember to select the graph you are currently running.)
2. Specify if you want to print durations and if you want to print the abbreviated activity names (latter only possible for BPIC2017).
3. Run main.py
4. You are asked to select a pattern type. Enter the pattern type you want to explore, e.g. “7p” (for P7’, see [1] for details on pattern types).
   * If you selected {1, 2, 3, 4}: You get the top 20 most frequent execution sequences (these are not yet instances, these are all distinct execution sequences). Select one of these execution sequences by entering their index.
      * Now you get 20 instances of this execution sequence (ordered by duration, ascending). Select one of these instances by entering their index. This visualizes the subgraph of that instance (and saves it to output/subgraphs/task_instances). You can go back to selecting a pattern type by entering “y”.
   * If you selected {7p, 8p}: You get 20 most frequent action sequences that are batched. Select one of these action sequences by entering their index.
      * Now you get the 20 most frequent execution sequences of the specified batch pattern type with that action sequence. Select one of these execution sequences by entering their index.
         * Now you get 20 instances of this execution sequences (ordered by duration, ascending). Select one of these instances by entering their index. This visualizes the subgraph of that instance (and saves it to output/subgraphs/task_instances). You can go back to selecting a pattern type by entering “y”.
    
##### 3. Exploring the graph for process executions
1. Set “step_explore_cases” to True. (And remember to select the graph you are currently running.)
2. Specify if you want to print durations and if you want to print the abbreviated activity names (latter only possible for BPIC2017).
3. Run main.py
4. Enter the case ids of the cases you want to visualize in the following format: *application_1213,application_73947*. This visualizes the subgraph of those cases (and saves it to output/subgraphs/from_cases). You can go back to selecting a new set of cases by entering “y”.

##### 4. Measuring patterns statistics
1. Set “step_measure_patterns” to True. (And remember to select the graph you are currently running.)
2. Run main.py

*To use this implementation on other data sets or build on an existing event graph constructed by the [implementation](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs) from [2], please follow the detailed comments in [main.py](main.py) and [graph_confs.py](graph_confs.py).*

## Toolkit breakdown
See [toolkit_breakdown.txt](toolkit_breakdown.txt) for a breakdown of the main executable and organization of the code.
