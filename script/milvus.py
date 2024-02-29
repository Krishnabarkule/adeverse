import boto3
import pandas as pd
import json
from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType

def read_json_data_from_s3(bucket_name, folder_prefix, aws_access_key_id, aws_secret_access_key, aws_session_token):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        dfs = []

        for obj in response.get('Contents', []):
            object_key = obj['Key']

            if object_key.lower().endswith('.json'):
                obj_response = s3.get_object(Bucket=bucket_name, Key=object_key)
                data = json.loads(obj_response['Body'].read().decode('utf-8'))
                df = pd.json_normalize(data)
                dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    except Exception as e:
        print(f"Error in read_json_data_from_s3: {str(e)}")
        return None

def create_embedding_dataframe(combined_df):
    try:
        model = SentenceTransformer("all-MiniLM-L6-v2")
        data = []

        for index, row in combined_df.iterrows():
            article_title = row.get("Article Title", "")
            article_abstract = row.get("Article Abstract", "")

            if article_title and article_abstract:
                embedding = model.encode([article_abstract])[0]
                data.append([article_title, article_abstract, embedding])

        columns = ["article_title", "article_abstract", "embedding"]
        df = pd.DataFrame(data, columns=columns)
        return df
    except Exception as e:
        print(f"Error in create_embedding_dataframe: {str(e)}")
        return None

def create_milvus_collection(df):
    try:
        connections.connect(host="54.186.110.51",
                            port='19530',
                            user="admin",
                            password="Agile@1234")

        fields = [
            FieldSchema(name="id", dtype=DataType.INT64,
                        is_primary=True, auto_id=True, max_length=100),
            FieldSchema(name="article_title", dtype=DataType.VARCHAR, max_length=1000),
            FieldSchema(name="article_abstract",
                        dtype=DataType.VARCHAR, max_length=10000),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384),
        ]
        schema = CollectionSchema(fields, "The schema for a medium news collection", enable_dynamic_field=False)
        collection = Collection("adverse_events", schema)

        index_params = {
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "params": {"nlist": 16384}
        }

        if not collection.has_index():
            collection.create_index(field_name="embedding", index_params=index_params)

        collection.load()

        df['embedding'] = df['embedding'].apply(lambda x: x + [0.0] * (768 - len(x)))

        collection.insert(df.to_dict(orient='records'))
        collection.flush()
        print("Entity counts:", collection.num_entities)
    except Exception as e:
        print(f"Error in create_milvus_collection: {str(e)}")

# # Example usage
# bucket_name = 'milvus-bucket-eks'
# folder_prefix = 'unprocessed/'
# aws_access_key_id = 'ASIAXWDKBRAV6TF5PQRN'
# aws_secret_access_key = 'UKA7J3xKUpRftVar3++AapAt6GZOUEiTOp2B39XP'
# aws_session_token = 'IQoJb3JpZ2luX2VjEP3//////////wEaCXVzLXdlc3QtMiJHMEUCIQD8XevDZezAQ5bk1cBaXQsQWZBmKyx6876ULPUn957rLgIgZhTM4NB9ADNAvAE46KLYzpft+xfpTK/Po6ZoybSBq+AqtAMI5v//////////ARAAGgw1Mjg1MDMzNzU5MTUiDHAji7Hd2hkVJu6qAiqIAyXN6GuMlc0mA84EsTcsqRgOrYH+boHvDUeSd9P+aSWF7GB9lrRaihFTVvadaoD6wgaQUhz8MI1C52jDHOedN+vyS59yD2UgJNTaTp/DAa70UXdZW++L/TPUcw4Hb38WSih97mrGtIY6NWeYXVqr7eX0usA3Ot5XHZ39zibmcDong547S2Qcu2KWhuA0DBekEiz9THMjggv1gjNxNy7HuryA3MYMj6r/UPfVDa8kPXPvyEw0LMPhFfoHeoVnHXrA5Og1fRXWhlFRX1HN+ISXW1zh6ioffeaQgheoGpcewP20/HOinpg7G5g+RsITJeR/rJTljhZkLIvztKMkh28aTZRlQBnaKWwkSJzF+xxzQQ7kKMKAPBewlBRBg6MPfrAWKgyLWvKB1iZKhh3fd7I6i5UKSOGHCadox0i1bBGjcDt0KiV6lP07c0qXEQjsjx+7g5Xiyw5zkBT62MxAvT2lBuB0zxBJ2OinStrkuBLgDwaXsmEyYm8BnDYJifq2nMFttaYURBrVaMkGMLLv+q4GOqYBS/MEqzHSZH1JUKYw6W0RkngxxHQIBYdcNCaVG9bv84zCI5viRYmU2Wlm2v9XS9dICdA9mgzRZagJVLFyUOg7UebEB1m1iDAIUCQ43pbH+lQMaAB3kPZ9wS2Cfy8EioydJMmdSyn35D6uAc8lXvZvf6mh4ISoGPUBiacsq6sxkcb00gGAWgHqED9knZxtsY5rB6ZR/ypR7V8IUCAVGPlNFzC3xvFwgQ=='

# combined_df = read_json_data_from_s3(bucket_name, folder_prefix, aws_access_key_id, aws_secret_access_key, aws_session_token)
# embedding_df = create_embedding_dataframe(combined_df)
# create_milvus_collection(embedding_df)