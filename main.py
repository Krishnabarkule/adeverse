import json
import configparser
from script.cluster import start_cluster
from script.local_to_s3 import copy_all_files_to_s3
from script.unpro_to_pro import copy_files_within_bucket, delete_files_in_folder
#from script.milvus import read_json_data_from_s3,create_embedding_dataframe, create_milvus_collection


CONFIG_FILE_PATH = '/Users/krishnasundarraobarkule/Desktop/adverse_event_detection/cred.ini'
JSON_FILE_PATH = '/Users/krishnasundarraobarkule/Desktop/adverse_event_detection/parameter.json'

def load_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def main():
    config = load_config(CONFIG_FILE_PATH)

    db_host = config['databricks']['DB_url']
    db_token = config['databricks']['DB_token']

    aws_access_key_id = config['aws']['aws_access_key_id']
    aws_secret_access_key = config['aws']['aws_secret_access_key']
    aws_session_token = config['aws']['aws_session_token']

    with open(JSON_FILE_PATH, 'r') as json_file:
        parameters = json.load(json_file)

    bucket_name = parameters['bucket_name']
    s3_source_folder = parameters['s3_source_folder']
    s3_target_folder = parameters['s3_target_folder']
    local_folder = parameters['local_folder']
    cluster_id = parameters['cluster_id']

    try:
        start_cluster(cluster_id=cluster_id,db_host=db_host,db_token=db_token)
        copy_all_files_to_s3(local_folder, bucket_name, s3_source_folder, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
        copy_files_within_bucket(bucket_name, s3_source_folder, s3_target_folder, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
        # combined_df = read_json_data_from_s3(bucket_name, folder_prefix=s3_source_folder, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
        # embedding_df = create_embedding_dataframe(combined_df)
        # create_milvus_collection(embedding_df)
        # delete_files_in_folder(bucket_name, s3_source_folder, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()