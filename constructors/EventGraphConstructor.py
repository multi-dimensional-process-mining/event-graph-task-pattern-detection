import pandas as pd
from PerformanceRecorder import PerformanceRecorder
from neo4j import GraphDatabase


class EventGraphConstructor:

    def __init__(self, password, import_directory, filename):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.filename = filename
        self.file_name = f'{filename}.csv'
        self.csv_data_set = pd.read_csv(f'{import_directory}{filename}.csv')
        self.event_attributes = self.csv_data_set.columns
        self.data_entities = ['case', 'resource']

    def construct_single(self):
        pr = PerformanceRecorder(self.filename, 'constructing_event_graph')
        query_create_event_nodes = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS ' \
                                   f'FROM \"file:///{self.file_name}\" as line'
        for attr in self.event_attributes:
            if attr == 'idx':
                value = f'toInteger(line.{attr})'
            elif attr in ['timestamp', 'start', 'end']:
                value = f'datetime(line.{attr})'
            else:
                value = 'line.' + attr
            if self.event_attributes.get_loc(attr) == 0:
                new_line = f' CREATE (e:Event {{{attr}: {value},'
            elif self.event_attributes.get_loc(attr) == len(self.event_attributes) - 1:
                new_line = f' {attr}: {value}, LineNumber: linenumber()}})'
            else:
                new_line = f' {attr}: {value},'
            query_create_event_nodes = query_create_event_nodes + new_line
        run_query(self.driver, query_create_event_nodes)
        pr.record_performance("import_event_nodes")

        # query_filter_events = f'MATCH (e:Event) WHERE e.lifecycle in ["SUSPEND","RESUME", "ATE_ABORT", "SCHEDULE", "WITHDRAW"] DELETE e'
        query_filter_events = f'MATCH (e:Event) WHERE e.lifecycle in ["SUSPEND","RESUME"] DELETE e'
        run_query(self.driver, query_filter_events)
        pr.record_performance(f"filter_events_SUSPEND_RESUME")

        for entity in self.data_entities:
            query_create_entity_nodes = f'''
                MATCH (e:Event) 
                WITH DISTINCT e.{entity} AS id
                CREATE (n:Entity {{ID:id, EntityType:"{entity}"}})'''
            run_query(self.driver, query_create_entity_nodes)
            pr.record_performance(f"create_entity_nodes_({entity})")

            query_correlate_events_to_entity = f'''
                MATCH (e:Event) WHERE EXISTS(e.{entity})
                MATCH (n:Entity {{EntityType: "{entity}"}}) WHERE e.{entity} = n.ID
                CREATE (e)-[:CORR]->(n)'''
            run_query(self.driver, query_correlate_events_to_entity)
            pr.record_performance(f"correlate_events_to_{entity}s")

            query_create_directly_follows = f'''
                MATCH (n:Entity) WHERE n.EntityType="{entity}"
                MATCH (n)<-[:CORR]-(e)
                WITH n, e AS nodes ORDER BY e.timestamp, ID(e)
                WITH n, collect(nodes) AS event_node_list
                UNWIND range(0, size(event_node_list)-2) AS i
                WITH n, event_node_list[i] AS e1, event_node_list[i+1] AS e2
                MERGE (e1)-[df:DF {{EntityType:n.EntityType}}]->(e2)'''
            run_query(self.driver, query_create_directly_follows)
            pr.record_performance(f"create_directly_follows_({entity})")
        pr.record_total_performance()
        pr.save_to_file()

    def construct_multi(self, case_cols, resource_cols):
        self.case_cols = case_cols
        self.resource_cols = resource_cols
        pr = PerformanceRecorder(self.filename, 'constructing_event_graph')
        query_create_event_nodes = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS ' \
                                   f'FROM \"file:///{self.file_name}\" as line'

        resource_list_string = "["
        for resource in self.resource_cols:
            resource_list_string = resource_list_string + 'line.' + resource
            if self.resource_cols.index(resource) < len(self.resource_cols) - 1:
                resource_list_string = resource_list_string + ","
            else:
                resource_list_string = resource_list_string + "]"
        case_list_string = "["
        for case in self.case_cols:
            case_list_string = case_list_string + 'line.' + case
            if self.case_cols.index(case) < len(self.case_cols) - 1:
                case_list_string = case_list_string + ","
            else:
                case_list_string = case_list_string + "]"
        new_line = f" WITH line, COLLECT({resource_list_string}) as resources_list, COLLECT({case_list_string}) " \
                   f" AS cases_list " \
                   f" UNWIND resources_list AS resources " \
                   f" WITH line, cases_list, resources " \
                   f" UNWIND cases_list AS cases " \
                   f" WITH line, cases, resources"
        query_create_event_nodes = query_create_event_nodes + new_line

        case_list = []
        resource_list = []
        for attr in self.event_attributes:
            attr_name = attr
            if attr == 'idx':
                value = f'toInteger(line.{attr})'
                create_new_line = True
            elif attr in ['timestamp', 'start', 'end']:
                value = f'datetime(line.{attr})'
                create_new_line = True
            elif attr in self.case_cols:
                attr_name = "case"
                case_list.append(attr)
                if len(case_list) == len(self.case_cols):
                    value = "cases"
                    create_new_line = True
                else:
                    create_new_line = False
            elif attr in self.resource_cols:
                attr_name = "resource"
                resource_list.append(attr)
                if len(resource_list) == len(self.resource_cols):
                    value = "resources"
                    create_new_line = True
                else:
                    create_new_line = False
            else:
                value = 'line.' + attr
                create_new_line = True

            if create_new_line:
                if self.event_attributes.get_loc(attr) == 0:
                    new_line = f' CREATE (e:Event {{{attr_name}: {value},'
                elif self.event_attributes.get_loc(attr) == len(self.event_attributes) - 1:
                    new_line = f' {attr_name}: {value}, LineNumber: linenumber()}})'
                else:
                    new_line = f' {attr_name}: {value},'
                query_create_event_nodes = query_create_event_nodes + new_line
        run_query(self.driver, query_create_event_nodes)
        # print(query_create_event_nodes)
        pr.record_performance("import_event_nodes")



        query_filter_events = f'MATCH (e:Event) WHERE e.lifecycle in ["SUSPEND","RESUME"] DELETE e'
        run_query(self.driver, query_filter_events)
        pr.record_performance(f"filter_events_SUSPEND_RESUME")

        for entity in self.data_entities:
            query_sort_entities = f'''
                        MATCH (e:Event)
                        UNWIND e.{entity} AS entity_unw
                        WITH e, entity_unw ORDER BY entity_unw ASC
                        WITH e, COLLECT(entity_unw) AS entity_ordered
                        SET e.{entity} = entity_ordered
                        '''
            run_query(self.driver, query_sort_entities)
            pr.record_performance(f"sort_{entity}")

            query_create_entity_nodes = f'''
                MATCH (e:Event)
                UNWIND e.{entity} AS {entity}
                WITH DISTINCT {entity} AS id
                CREATE (n:Entity {{ID:id, EntityType:"{entity}"}})'''
            # print(query_create_entity_nodes)
            run_query(self.driver, query_create_entity_nodes)
            pr.record_performance(f"create_entity_nodes_({entity})")

            query_correlate_events_to_entity = f'''
                MATCH (e:Event)
                UNWIND e.{entity} AS {entity}_id
                WITH e, {entity}_id
                MATCH (n:Entity {{EntityType: "{entity}", ID:{entity}_id}})
                CREATE (e)-[:CORR]->(n)'''
            # print(query_correlate_events_to_entity)
            run_query(self.driver, query_correlate_events_to_entity)
            pr.record_performance(f"correlate_events_to_{entity}s")

            query_create_directly_follows = f'''
                MATCH (n:Entity) WHERE n.EntityType="{entity}"
                MATCH (n)<-[:CORR]-(e)
                WITH n, e AS nodes ORDER BY e.timestamp, ID(e)
                WITH n, collect(nodes) AS event_node_list
                UNWIND range(0, size(event_node_list)-2) AS i
                WITH n, event_node_list[i] AS e1, event_node_list[i+1] AS e2
                MERGE (e1)-[df:DF {{EntityType:n.EntityType, nID:n.ID}}]->(e2)'''
            run_query(self.driver, query_create_directly_follows)
            pr.record_performance(f"create_directly_follows_({entity})")
        pr.record_total_performance()
        pr.save_to_file()


def run_query(driver, query):
    with driver.session() as session:
        result = session.run(query).single()
        if result:
            return result.value()
        else:
            return None
