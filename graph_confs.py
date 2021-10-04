#####################################################
############# EXPLANATION OF SETTINGS ###############
#####################################################

# SETTING FOR PREPROCESSING
# if no preprocessing is done, empty strings can be assigned
filename = {}           # name of the csv file to build the graph from
column_names = {}       # names of the columns in the csv file for [case, activity, timestamp, resource(, lifecycle)] (in this order)
separator = {}          # separator used in csv file
timestamp_format = {}   # format of the timestamps recorded in the csv file
use_sample = {}  # specify to only use a sample when preprocessing (if True, sample needs to be specified in corresponding PreProcessor.py)
sample_cases = {}

# GRAPH SETTINGS
password = {}                   # password of neo4j database
entity_labels = {}              # labels used in the graph for: [[df_resource, node_resource], [df_case, node_case]]
action_lifecycle_labels = {}    # labels used in the graph for: [activity, lifecycle]
timestamp_label = {}            # label used for timestamp

# SETTINGS FOR VISUALIZING:
name_data_set = {}          # name of the data set, used for configuring the node labels when visualizing subgraphs (only available for bpic2014 and bpic2017)

# SETTINGS FOR MEASURING:
pm_selection = {}           # configure the list of measurements, for each measurement or row: the pattern (p),
                            # time constraint (t) and resource inclusion (r) is specified as [p, t, r]
total_events = {}           # total number of events in the dataset (or in the sample)


#####################################################
############ CONFIGURATION OF SETTINGS ##############
#####################################################

# -------------- BPIC 2017 SETTINGS -----------------

for graph in ["bpic2017_single_ek"]:

    filename[graph] = "bpic2017"
    name_data_set[graph] = "bpic2017"
    column_names[graph] = ["case", "event", "time", "org:resource", "lifecycle:transition"]
    separator[graph] = ","
    timestamp_format[graph] = "%Y/%m/%d %H:%M:%S.%f"
    total_events[graph] = 859705
    password[graph] = "bpic2017"

    entity_labels[graph] = [['resource', 'resource'],
                            ['case', 'case']]
    action_lifecycle_labels[graph] = ['activity', 'lifecycle']
    timestamp_label[graph] = "timestamp"

    pm_selection[graph] = [[1, 0, 1], [4, 0, 1], [2, 0, 1], [3, 0, 1], [2, 1, 1], [3, 1, 1],
                           [5, 0, 1], [8, 0, 1], [6, 0, 1], [7, 0, 1], [6, 1, 1], [7, 1, 1],
                           [9, 0, 1], [12, 0, 1], [10, 0, 1], [11, 0, 1], [10, 1, 1], [11, 1, 1],
                           [13, 0, 1], [16, 0, 1], [14, 0, 1], [15, 0, 1], [14, 1, 1], [15, 1, 1],
                           ["7p", 1, 1], ["8p", 1, 1],
                           [1, 0, 2], [4, 0, 2], [2, 0, 2], [3, 0, 2], [2, 1, 2], [3, 1, 2],
                           [5, 0, 2], [8, 0, 2], [6, 0, 2], [7, 0, 2], [6, 1, 2], [7, 1, 2],
                           [9, 0, 2], [12, 0, 2], [10, 0, 2], [11, 0, 2], [10, 1, 2], [11, 1, 2],
                           [13, 0, 2], [16, 0, 2], [14, 0, 2], [15, 0, 2], [14, 1, 2], [15, 1, 2],
                           ["7p", 1, 2], ["8p", 1, 2]]

    use_sample[graph] = False
    sample_cases[graph] = ['Application_1111458873',
                           'Application_1372864243',
                           'Application_206394826',
                           'Application_1877008365',
                           'Application_1992048266']

# -------------- BPIC 2014 SETTINGS -----------------

for graph in ["bpic2014_single_ek"]:

    filename[graph] = "bpic2014_incident_activity"
    name_data_set[graph] = "bpic2014"
    column_names[graph] = ["Incident ID", "IncidentActivity_Type", "DateStamp", "Assignment Group"]
    separator[graph] = ";"
    timestamp_format[graph] = "%d-%m-%Y %H:%M:%S"
    total_events[graph] = 466737

    password[graph] = "bpic2014"

    entity_labels[graph] = [['resource', 'resource'],
                            ['case', 'case']]
    action_lifecycle_labels[graph] = ['activity']
    timestamp_label[graph] = "timestamp"

    pm_selection[graph] = [[1, 0, 0], [4, 0, 0], [2, 0, 0], [3, 0, 0], [2, 1, 0], [3, 1, 0],
                           [5, 0, 0], [8, 0, 0], [6, 0, 0], [7, 0, 0], [6, 1, 0], [7, 1, 0],
                           [9, 0, 0], [12, 0, 0], [10, 0, 0], [11, 0, 0], [10, 1, 0], [11, 1, 0],
                           [13, 0, 0], [16, 0, 0], [14, 0, 0], [15, 0, 0], [14, 1, 0], [15, 1, 0],
                           ["7p", 1, 0], ["8p", 1, 0]]

    use_sample[graph] = False
    sample_cases[graph] = []

# -------------- OPERATORS SETTINGS -----------------

for graph in ["operators"]:
    filename[graph] = "operators"
    name_data_set[graph] = "operators"
    column_names[graph] = ["product_id", "event", "completeTime", "operator"]
    separator[graph] = ","
    timestamp_format[graph] = "%Y/%m/%d %H:%M:%S.%f"
    total_events[graph] = 109049
    password[graph] = "operators"

    entity_labels[graph] = [['resource', 'resource'],
                            ['case', 'case']]
    action_lifecycle_labels[graph] = ['activity']
    timestamp_label[graph] = "timestamp"

    pm_selection[graph] = [[1, 0, 1], [4, 0, 1], [2, 0, 1], [3, 0, 1], [2, 1, 1], [3, 1, 1],
                           [5, 0, 1], [8, 0, 1], [6, 0, 1], [7, 0, 1], [6, 1, 1], [7, 1, 1],
                           [9, 0, 1], [12, 0, 1], [10, 0, 1], [11, 0, 1], [10, 1, 1], [11, 1, 1],
                           [13, 0, 1], [16, 0, 1], [14, 0, 1], [15, 0, 1], [14, 1, 1], [15, 1, 1],
                           ["7p", 1, 1], ["8p", 1, 1],
                           [1, 0, 2], [4, 0, 2], [2, 0, 2], [3, 0, 2], [2, 1, 2], [3, 1, 2],
                           [5, 0, 2], [8, 0, 2], [6, 0, 2], [7, 0, 2], [6, 1, 2], [7, 1, 2],
                           [9, 0, 2], [12, 0, 2], [10, 0, 2], [11, 0, 2], [10, 1, 2], [11, 1, 2],
                           [13, 0, 2], [16, 0, 2], [14, 0, 2], [15, 0, 2], [14, 1, 2], [15, 1, 2],
                           ["7p", 1, 2], ["8p", 1, 2]]

    use_sample[graph] = False
    sample_cases[graph] = []
