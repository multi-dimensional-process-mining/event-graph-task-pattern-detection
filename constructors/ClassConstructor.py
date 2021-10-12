import pandas as pd
from PerformanceRecorder import PerformanceRecorder
from neo4j import GraphDatabase


class ClassConstructor:

    def __init__(self, password, name_data_set, entity_labels, action_lifecycle_label):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.name_data_set = name_data_set
        self.entity_labels = entity_labels
        self.entity_labels[0].append('rID')
        self.entity_labels[1].append('cID')
        self.action_lifecycle_label = action_lifecycle_label

    def construct(self):
        # create performance recorder
        pr = PerformanceRecorder(self.name_data_set, 'constructing_high_level_event_nodes')
        # check if the transactional lifecycle is recorded
        if len(self.action_lifecycle_label) == 2:
            event_classifier = "activity+lifecycle"
            query_create_event_classes = f'''
                MATCH (e:Event) WITH DISTINCT e.{self.action_lifecycle_label[0]} AS action,
                e.{self.action_lifecycle_label[1]} AS lifecycle
                MERGE (c : Class {{Name:action, Lifecycle:lifecycle, Type:"activity+lifecycle", 
                ID: action+"+"+lifecycle}})'''
            run_query(self.driver, query_create_event_classes)
            query_link_events_to_classes = f'''
                MATCH ( c : Class ) WHERE c.Type = "activity+lifecycle"    
                MATCH ( e : Event ) where e.{self.action_lifecycle_label[0]} = c.Name 
                    AND e.{self.action_lifecycle_label[1]} = c.Lifecycle
                CREATE ( e ) -[:OBSERVED]-> ( c )'''
            run_query(self.driver, query_link_events_to_classes)
        else:
            event_classifier = "activity"
            query_create_event_classes = f'''
                MATCH ( e : Event ) WITH distinct e.{self.action_lifecycle_label[0]} AS action
                MERGE ( c : Class {{ Name:action, Type:"activity", ID: action}})'''
            run_query(self.driver, query_create_event_classes)
            query_link_events_to_classes = f'''
                MATCH ( c : Class ) WHERE c.Type = "Activity"
                MATCH ( e : Event ) WHERE c.Name = e.{self.action_lifecycle_label[0]}
                CREATE ( e ) -[:OBSERVED]-> ( c )'''
            run_query(self.driver, query_link_events_to_classes)

        for entity in self.entity_labels:
            # aggregate DF-relationships between classes
            query_aggregate_directly_follows_event_classes = f'''
                MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
                MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                WHERE n.EntityType = "{entity[0]}" AND df.EntityType = "{entity[1]}" AND c1.Type = "{event_classifier}" AND c2.Type="{event_classifier}"
                WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
                MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
            run_query(self.driver, query_aggregate_directly_follows_event_classes)

        # create task instance classes
        query_create_TI_classes = f'''
            MATCH (ti:TaskInstance)
            WITH DISTINCT ti.path AS path, count(*) AS count
            ORDER BY count DESC
            WITH collect(path) as paths, collect(count) as counts, collect(size(path)) as lengths
            UNWIND range(0, size(paths)-1) as pos
            WITH paths[pos] AS path, pos+1 AS rank, counts[pos] AS freq, lengths[pos] AS path_length
            CREATE (:Class {{Type:'TaskInstance', path:path, path_length:path_length, frequency:freq, rank:rank}})
            '''
        run_query(self.driver, query_create_TI_classes)
        query_link_TIs_to_classes = f'''
            MATCH (c:Class {{Type:'TaskInstance'}})
            MATCH (ti:TaskInstance) WHERE ti.path = c.path
            CREATE (ti)-[:TI_C]->(c)
            '''
        run_query(self.driver, query_link_TIs_to_classes)
        q_derive_TI_ID_from_class_rank = f'''
            MATCH (ti:TaskInstance)-[:TI_C]->(c:Class)
            SET ti.ID = c.rank
            '''
        run_query(self.driver, q_derive_TI_ID_from_class_rank)
        for entity in self.entity_labels:
            # aggregate DF_TI relationships to TI classes
            query_aggregate_directly_follows_TI_classes = f'''
                MATCH (c1:Class)<-[:TI_C]-(ti1:TaskInstance)-[df:DF_TI {{EntityType:"{entity[0]}"}}]->
                (ti2:TaskInstance)-[:TI_C]->(c2:Class)
                WITH c1, c2, count(df) AS df_freq
                MERGE (c1)-[r:DF_C {{EntityType:"{entity[0]}"}}]->(c2) ON CREATE SET r.count=df_freq
                '''
            run_query(self.driver, query_aggregate_directly_follows_TI_classes)


def run_query(driver, query):
    with driver.session() as session:
        result = session.run(query).single()
        if result:
            return result.value()
        else:
            return None
