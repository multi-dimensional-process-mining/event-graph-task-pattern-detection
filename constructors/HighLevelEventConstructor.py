from PerformanceRecorder import PerformanceRecorder
from neo4j import GraphDatabase


class HighLevelEventConstructor:

    def __init__(self, password, name_data_set, entity_labels, action_lifecycle_label):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.name_data_set = name_data_set
        self.entity_labels = entity_labels
        self.entity_labels[0].append('rID')
        self.entity_labels[1].append('cID')
        self.action_lifecycle_label = action_lifecycle_label
        self.max_cases = ""
        self.max_resources = ""

    def construct_single(self):
        # create performance recorder
        pr = PerformanceRecorder(self.name_data_set, 'constructing_high_level_event_nodes')
        # combine resource and case directly follows relationships
        query_combine_df_joint = f'''
            MATCH (e1:Event)-[:DF {{EntityType:'{self.entity_labels[0][0]}'}}]->(e2:Event)
            WHERE (e1)-[:DF {{EntityType:'{self.entity_labels[1][0]}'}}]->(e2)
            CREATE (e1)-[:DF {{EntityType:'joint'}}]->(e2)
            '''
        run_query(self.driver, query_combine_df_joint)
        pr.record_performance('combine_df_joint')

        # check if the transactional lifecycle is recorded and combine with activity classifier into single property
        if len(self.action_lifecycle_label) == 2:
            query_set_activity_lifecycle_property = f'''
                MATCH (e:Event)
                SET e.activity_lifecycle = e.{self.action_lifecycle_label[0]}+'+'+e.{self.action_lifecycle_label[1]}
                '''
            run_query(self.driver, query_set_activity_lifecycle_property)
            pr.record_performance('set_activity_lifecycle_property')
            self.action_lifecycle_label[0] = 'activity_lifecycle'

        # query and materialize task instances and relationships with events
        query_create_ti_nodes = f'''
            CALL {{
            MATCH (e1:Event)-[:DF {{EntityType:'joint'}}]->() WHERE NOT ()-[:DF {{EntityType:'joint'}}]->(e1)
            MATCH ()-[:DF {{EntityType:'joint'}}]->(e2:Event) WHERE NOT (e2)-[:DF {{EntityType:'joint'}}]->()
            MATCH p=(e1)-[:DF*]->(e2) WHERE all(r in relationships(p) WHERE (r.EntityType = 'joint'))
            RETURN p, e1, e2
            UNION
            MATCH (e:Event) WHERE exists(e.{self.entity_labels[0][1]})
            AND NOT ()-[:DF {{EntityType:'joint'}}]->(e) AND NOT (e)-[:DF {{EntityType:'joint'}}]->()
            MATCH p=(e) RETURN p, e AS e1, e AS e2
            }}
            WITH [event in nodes(p) | event.{self.action_lifecycle_label[0]}] AS path, 
                e1.{self.entity_labels[0][1]} AS resource, e1.{self.entity_labels[1][1]} AS case_id, 
                nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time
            CREATE (ti:TaskInstance {{path:path, rID:resource, cID:case_id, start_time:start_time,
                end_time:end_time, r_count: 1, c_count: 1}})
            WITH ti, events
            UNWIND events AS e
            CREATE (e)<-[:CONTAINS]-(ti)
            '''
        run_query(self.driver, query_create_ti_nodes)
        pr.record_performance('create_ti_nodes')

        for entity in self.entity_labels:
            # correlate task instances to entities
            query_correlate_ti_to_entity = f'''
                MATCH (ti:TaskInstance)
                MATCH (n:Entity {{EntityType:"{entity[0]}"}}) WHERE ti.{entity[2]} = n.ID
                CREATE (ti)-[:CORR]->(n)
                '''
            run_query(self.driver, query_correlate_ti_to_entity)
            pr.record_performance(f'correlate_ti_to_entity_({entity[0]})')

            # create DF-relationships between task instances
            query_create_df_ti = f'''
                MATCH (n:Entity) WHERE n.EntityType="{entity[0]}"
                MATCH (ti:TaskInstance)-[:CORR]->(n)
                WITH n, ti AS nodes ORDER BY ti.start_time, ID(ti)
                WITH n, COLLECT (nodes) as nodeList
                UNWIND range(0, size(nodeList)-2) AS i
                WITH n, nodeList[i] as ti_first, nodeList[i+1] as ti_second
                MERGE (ti_first)-[df:DF_TI {{EntityType:n.EntityType}}]->(ti_second)
                '''
            run_query(self.driver, query_create_df_ti)
            pr.record_performance(f'create_df_ti_({entity[0]})')

        pr.record_total_performance()
        pr.save_to_file()

    def construct_multi(self, max_cases, max_resources):
        self.max_cases = max_cases
        self.max_resources = max_resources
        pr = PerformanceRecorder(self.name_data_set, 'constructing_high_level_event_nodes')
        # aggregate resource DF-relationships and record the count
        query_combine_df_resource_joint = f'''
            MATCH (e1:Event)-[df_r:DF {{EntityType: "{self.entity_labels[0][0]}"}}]->(e2:Event)
            WITH count(df_r) AS df_resource_freq, e1, e2
            MERGE (e1)-[df:DF {{EntityType: "resource_joint"}}]->(e2) ON CREATE SET df.count = df_resource_freq
            '''
        run_query(self.driver, query_combine_df_resource_joint)
        # aggregate case DF-relationships and record the count
        query_combine_df_case_joint = f'''
            MATCH (e1:Event)-[df_c:DF {{EntityType: "{self.entity_labels[1][0]}"}}]->(e2:Event)
            WITH count(df_c) AS df_case_freq, e1, e2
            MERGE (e1)-[df:DF {{EntityType: "case_joint"}}]->(e2) ON CREATE SET df.count = df_case_freq
            '''
        run_query(self.driver, query_combine_df_case_joint)
        # combine aggregated case and resource DF-relationships and counts into joint DF-relationships
        query_combine_df_joint = f'''
            MATCH (e1:Event)-[]->(e2:Event) WHERE (e1)-[:DF {{EntityType: "{self.entity_labels[0][0]}"}}]->(e2) 
                AND (e1)-[:DF {{EntityType: "{self.entity_labels[1][0]}"}}]->(e2)
            WITH e1, e2
            MATCH (e1)-[df_r:DF {{EntityType: "resource_joint"}}]->(e2)
            WITH e1, e2, df_r
            MATCH (e1)-[df_c:DF {{EntityType: "case_joint"}}]->(e2)
            WITH e1, e2, df_r, df_c
            MERGE (e1)-[df:DF {{EntityType: "joint"}}]->(e2) 
                ON CREATE SET df.c_count = df_c.count, df.r_count = df_r.count
            '''
        run_query(self.driver, query_combine_df_joint)
        pr.record_performance('combine_df_joint')

        # check if the transactional lifecycle is recorded and combine with activity classifier into single property
        if len(self.action_lifecycle_label) == 2:
            query_set_activity_lifecycle_property = f'''
                MATCH (e:Event)
                SET e.activity_lifecycle = e.{self.action_lifecycle_label[0]}+'+'+e.{self.action_lifecycle_label[1]}
                '''
            run_query(self.driver, query_set_activity_lifecycle_property)
            pr.record_performance('set_activity_lifecycle_property')
            self.action_lifecycle_label[0] = 'activity_lifecycle'

        # for each combination of nr. of resources and nr. of cases, query and materialize task instances
        for r_count in range(1, self.max_resources + 1):
            for c_count in range(1, self.max_cases + 1):
                query_create_ti_nodes = f'''
                    CALL {{
                    MATCH (e1:Event)-[:DF {{EntityType:'joint', c_count: {c_count}, r_count: {r_count}}}]->() 
                        WHERE NOT ()-[:DF {{EntityType:'joint', c_count: {c_count}, r_count: {r_count}}}]->(e1)
                    MATCH ()-[:DF {{EntityType:'joint', c_count: {c_count}, r_count: {r_count}}}]->(e2:Event) 
                        WHERE NOT (e2)-[:DF {{EntityType:'joint', c_count: {c_count}, r_count: {r_count}}}]->()
                    MATCH p=(e1)-[:DF*]->(e2) WHERE all(r in relationships(p) 
                        WHERE (r.EntityType = 'joint' AND r.c_count = {c_count} AND r.r_count = {r_count}))
                        AND all(idx in range(0, size(nodes(p))-2) WHERE nodes(p)[idx].case = nodes(p)[idx + 1].case 
                        AND nodes(p)[idx].resource = nodes(p)[idx + 1].resource)
                    RETURN p, e1, e2
                    UNION
                    MATCH (e:Event) WHERE NOT ()-[:DF {{EntityType:'joint'}}]->(e) 
                        AND NOT (e)-[:DF {{EntityType:'joint'}}]->()
                        AND size(e.resource) = {r_count} AND size(e.case) = {c_count}
                    MATCH p=(e) RETURN p, e AS e1, e AS e2
                    }}
                    WITH [event in nodes(p) | event.activity_lifecycle] AS path, 
                        e1.resource AS resource, e1.case AS case_id, 
                        nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time
                    CREATE (ti:TaskInstance {{path:path, rID:resource, cID:case_id, start_time:start_time,
                        end_time:end_time, r_count: {r_count}, c_count: {c_count}}})
                    WITH ti, events
                    UNWIND events AS e
                    CREATE (e)<-[:CONTAINS]-(ti)
                    '''
                run_query(self.driver, query_create_ti_nodes)
                pr.record_performance('create_ti_nodes')

        # correlate task instances to entities
        query_correlate_ti_to_entities = f'''
            MATCH (ti:TaskInstance)
            MATCH (ti)-[:CONTAINS]->(:Event)-[:CORR]->(n:Entity)
            WITH DISTINCT ti, n
            CREATE (ti)-[:CORR]->(n)
            '''
        run_query(self.driver, query_correlate_ti_to_entities)
        pr.record_performance(f'correlate_ti_to_entities')

        for entity in self.entity_labels:
            # create DF-relationships between task instances
            query_create_df_ti = f'''
                MATCH (n:Entity) WHERE n.EntityType="{entity[0]}"
                MATCH (ti:TaskInstance)-[:CORR]->(n)
                WITH n, ti AS nodes ORDER BY ti.start_time, ID(ti)
                WITH n, COLLECT (nodes) as nodeList
                UNWIND range(0, size(nodeList)-2) AS i
                WITH n, nodeList[i] as ti_first, nodeList[i+1] as ti_second
                MERGE (ti_first)-[df:DF_TI {{EntityType:n.EntityType}}]->(ti_second)
                '''
            run_query(self.driver, query_create_df_ti)
            pr.record_performance(f'create_df_ti_({entity[0]})')

        pr.record_total_performance()
        pr.save_to_file()


def run_query(driver, query):
    with driver.session() as session:
        result = session.run(query).single()
        if result:
            return result.value()
        else:
            return None
