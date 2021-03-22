from neo4j import GraphDatabase
import pandas as pd
import matplotlib.pyplot as plt


### begin config
# data_set = "bpic2014"
data_set = "bpic2017"

task_ids = [4, 9, 25, 37]

# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", data_set))


def plot_task_trend(tx):
    q = f'''
        MATCH (ti:TaskInstance) WHERE ti.ID IN {task_ids}
            WITH date.truncate('week', ti.start_time) AS date, 
            ti.ID AS pattern, count(ti.ID) AS count
            RETURN date, pattern, count ORDER BY date ASC
        '''
    print(q)

    result = tx.run(q)
    df_task_per_date = pd.DataFrame([dict(record) for record in result]).pivot(index='date',
                                                                               columns='pattern').fillna(0)
    df_task_per_date.columns = df_task_per_date.columns.droplevel(0)
    df_task_per_date.index = pd.DatetimeIndex(df_task_per_date.index.astype(str))

    fig, axes = plt.subplots(nrows=1, ncols=4, squeeze=False, sharex=True, sharey=False, figsize=(30, 4))
    df_task_per_date.plot(subplots=True, ax=axes, color='tab:blue', xlabel="", fontsize=12, legend=False)
    plt.savefig(f"analysis-results\\{data_set}\\pattern_trend_{str(task_ids)}.png")


with driver.session() as session:
    session.read_transaction(plot_task_trend)
