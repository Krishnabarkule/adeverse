import boto3
import os

aws_access_key_id="ASIAXWDKBRAVQANCA475"
aws_secret_access_key="AoWms/owX0zmaLDVtPJJSbdK44V1GQ8AYVkJ25MV"
aws_session_token="IQoJb3JpZ2luX2VjEC8aCXVzLXdlc3QtMiJIMEYCIQC99QaYj4gTeIgInJ13iHaqNy/un2alKufSWR3E/D8O/wIhALnEZDeCsAMwPeUnN/tYu6fEyP+0Sqojxl9/Qj6m4UFwKqsDCCgQABoMNTI4NTAzMzc1OTE1Igyq4DaZAclpmEYQeFYqiAM9SW70u2CU2+6MSc6IoYTlKr3waJRqT4Rz3ngSaTbOPlEKRerzHNo6z87w53B5XzsfzTGucGZEr94tp9gx+Z6f3VVaq/nUCjicwQVTTY3dGa3XnMo97I4VNn7xYC1YJ0y0z6DD/PYVgBMd4anvxIDw8KWAd1IWNKz0vXe0u6TdmfW+IB4RvmehQkOH4PPDpUlBtp5d70IPEpm64sPaL1Xl5kQ3pwNqnfGiOlgR7fM3vRJjBaXzC8sYLz4EmkMjh9Ac8CXmBColGCXehyRio1sa5hCnJE/hpWrFJ1mu3Yxt0f5hiiuSUsXMsbbeBxRWL2mmD0uv2jk31ydZp2VI6vWAB4/vINF3dj5n6dz4URhKtXr5ie7rTDVUjL9QKvQBhdVXSU/PhN6LOOTj+1EbOxwxVlRHP9omIbvEI2WsZtDiwsaZxj8MeOthbJSOckE3BK6CCFUmR3bJU1ygyf9j2uD6JGQsZveiumdGqgDIvIJMIsxWhufG1pAGBkFx9wtvZs0ydjMKGkqJlzCz8IWvBjqlAetouuM/o9BU4o4gXBKnWrY5LSWrE77MO6O0Ge3ES2Bxd0I/qh2KcFQFwpTbj7aR6oYDYCZY3SOpT5lfu39uPxWdIMa5r74psTmF5jujQMNVtuO90ANSNc9Bu3xOFkBqaimBF5Co/1Pky4a9nY5GVcrZcvToKfAHoGlVvPShGcXOWholKyfXxDw5/jQbCVct8/lTREXb6F3eVrgvbiv/oES/BjpqvQ=="

client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
s3_r = boto3.resource('s3')
processed = client.list_objects(
    Bucket='milvus-bucket-eks',
    Prefix='processed',)


# processed_keys = [content['Key'].split('/')[-1] for content in processed.get('Contents', []) if content['Key'].split('/')[-1]]

# Alternatively, you can use a filter to exclude empty strings
processed_keys = list(filter(None, [content['Key'].split('/')[-1] for content in processed.get('Contents', [])]))

print("process keys:",processed_keys)


Unprocessed = client.list_objects(
    Bucket='milvus-bucket-eks',
    Prefix='Unprocessed',)
Unprocessed_keys = [content['Key'].split('/')[-1] for content in Unprocessed.get('Contents', [])]

print("Unprocess keys:",Unprocessed_keys)

new_list = [item for item in Unprocessed_keys if item not in processed_keys]

print("new_list",new_list)

# for i in new_list:
#     copy_source = {
#         'Bucket': 'milvus-bucket-eks',
#         'Key': 'i'}
#     s3_r.meta.client.copy(copy_source, 'milvus-bucket-eks', f'processed/{i}')



s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)


copy_source = {
    'Bucket': 'milvus-bucket-eks',
    'Key': 'Unproccesed/demo.txt'
}
bucket = s3.Bucket('milvus-bucket-eks')
obj = bucket.Object('proccessed/demo.txt')
obj.copy(copy_source)
