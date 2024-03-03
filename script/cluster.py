
from databricks.sdk import WorkspaceClient


def start_cluster(cluster_id:str,db_host:str,db_token:str):
    
    try:
        print("Starting cluster")
        w = WorkspaceClient(host=db_host, token=db_token)
        # res = w.clusters.wait_get_cluster_running(cluster_id)
        res = w.clusters.start(cluster_id=cluster_id).result()

        print(f"{cluster_id} Cluster is running")
        
        return res
        
    except Exception as e:
        print(e)


DB_url = "https://agilisium.cloud.databricks.com"
DB_token = "dapic961af96fc16cf8e8c58618c5aeee123"
cluster_id = "0828-132635-71kqd24v"

res=start_cluster(
    cluster_id=cluster_id,
    db_host=DB_url,
    db_token=DB_token
)
print(res)