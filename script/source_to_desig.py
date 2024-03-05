import boto3

# Function to delete files in a specified folder
def delete_files_in_folder(bucket_name, source_folder,aws_access_key_id,aws_secret_access_key,aws_session_token):
    s3=boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,aws_session_token=aws_session_token,)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)

    # Check if there are objects to delete
    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            
            # Make sure not to delete the folder placeholder
            if not file_key.endswith('/'):
                # Delete the object (file)
                s3.delete_object(Bucket=bucket_name, Key=file_key)
                print(f"Deleted {file_key}")

    print("Files deleted successfully.")


# aws_access_key_id="ASIAXWDKBRAVQDLYIG4B"
# aws_secret_access_key="N2OrCzKTnuN9akuaa4hphsQYeAuHsBdDK6dnac/W"
# aws_session_token="IQoJb3JpZ2luX2VjEI7//////////wEaCXVzLXdlc3QtMiJGMEQCIGzTWlVkKE8aOj1ZXzBlNoe7HTs8sefNO6xgQS2nEsl3AiB088E6ITxZhk5p9oN094kCyiYgwii3NZRxGynrCE2GeCq0AwiH//////////8BEAAaDDUyODUwMzM3NTkxNSIMN3ApZqTnsHPh0XTpKogDWWei1Mmwb0sLXPhWcauFKIHQ0fvJPORhoyilAtd05OHWM5SUUn8xw+hqSplvRBpdrvUKgQbI4oF7/RuIMEq227UH8jg7Bw1oHiBA0ZSqMV2okUY+AOWibh1O2sVhhY+g/MojEwYfSsb5hepKNzGj0xNE5ybecFBG/RUxSK1ygaQhkFhoJhmpdUQpRW0kleE0YAFgqlLxTwnBHyDjAzyh9dnQgxFix0txj4vX3QcGmeu6leI8q908nClFwXXY2faaAdP8ZR4aieKMy+tEaIbGo+F//76v6rjrTWuROhq0dUr9Wr3CROWl6iHF6e6/eXMp8uh0N4cCsbKjTxC3/C0anjhpGX+UcfIYbp+4/M1Qf+McgRjucSsI8tCtEvglMGlSrmUu1hMxPy7OeJHLQ8e8fp0fHrO3Vc+cjWCZ6wEkTHN+VGVYCUQCwEqbXPam8R0jh0t2jJg0fXVoxT0iQ7JE5eOgdRS7SU7SvjQEYVJcxpM/oGXO1GWaZBPiUtnS7n/pCZkPBhCttqYwz+CarwY6pwHnwlsEijxDeKRUI9MFTafqV+N8vMhagkM711QYvC99LY/M/x6rbJyyRmlgySaoQ90mrvwwTUsCk/WSRZxR5GJSLQNPz2K2hgMN375QUzPwHz2FxgRbRmYul/KkCQIXzDZglQkd0V9TYpYqWfq5wba/h/IWlPDqMDB+hGsuNHnx3+EIc/Og8GkXluwKm1SwXy5LF/8jVaLAaym1Q0he/RlmLqHXGxMLNg=="

# bucket_name = 'milvus-bucket-eks'
# folder_path = 'unproccessed/' 

# delete_files_in_folder(bucket_name, folder_path, aws_access_key_id, aws_secret_access_key, aws_session_token)

# import boto3

def copy_files_if_not_exists(bucket_name, source_folder, destination_folder, aws_access_key_id, aws_secret_access_key, aws_session_token):
    # Initialize S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
    
    # List objects in the source folder
    response_source = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
    
    # Extract source file names
    source_files = [obj['Key'] for obj in response_source.get('Contents', [])]
    
    # List objects in the destination folder
    response_destination = s3.list_objects_v2(Bucket=bucket_name, Prefix=destination_folder)
    
    # Extract destination file names
    destination_files = [obj['Key'] for obj in response_destination.get('Contents', [])]
    
    # Prepare the list of destination file basenames
    destination_files_basenames = [file.replace(destination_folder, '') for file in destination_files]
    
    # Iterate over source files and copy if they don't exist in destination
    for source_file in source_files:
        file_name = source_file.replace(source_folder, '')
        if file_name not in destination_files_basenames:
            # Define the copy source
            copy_source = {'Bucket': bucket_name, 'Key': source_file}
            
            # Define the destination key
            destination_key = destination_folder + file_name
            
            # Copy the object to the new destination
            s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
            print(f"Copied {file_name} to destination folder.")

    print("File copy operation completed.")

# # # AWS credentials
# aws_access_key_id = "ASIAXWDKBRAVQDLYIG4B"
# aws_secret_access_key = "N2OrCzKTnuN9akuaa4hphsQYeAuHsBdDK6dnac/W"
# aws_session_token = "IQoJb3JpZ2luX2VjEI7//////////wEaCXVzLXdlc3QtMiJGMEQCIGzTWlVkKE8aOj1ZXzBlNoe7HTs8sefNO6xgQS2nEsl3AiB088E6ITxZhk5p9oN094kCyiYgwii3NZRxGynrCE2GeCq0AwiH//////////8BEAAaDDUyODUwMzM3NTkxNSIMN3ApZqTnsHPh0XTpKogDWWei1Mmwb0sLXPhWcauFKIHQ0fvJPORhoyilAtd05OHWM5SUUn8xw+hqSplvRBpdrvUKgQbI4oF7/RuIMEq227UH8jg7Bw1oHiBA0ZSqMV2okUY+AOWibh1O2sVhhY+g/MojEwYfSsb5hepKNzGj0xNE5ybecFBG/RUxSK1ygaQhkFhoJhmpdUQpRW0kleE0YAFgqlLxTwnBHyDjAzyh9dnQgxFix0txj4vX3QcGmeu6leI8q908nClFwXXY2faaAdP8ZR4aieKMy+tEaIbGo+F//76v6rjrTWuROhq0dUr9Wr3CROWl6iHF6e6/eXMp8uh0N4cCsbKjTxC3/C0anjhpGX+UcfIYbp+4/M1Qf+McgRjucSsI8tCtEvglMGlSrmUu1hMxPy7OeJHLQ8e8fp0fHrO3Vc+cjWCZ6wEkTHN+VGVYCUQCwEqbXPam8R0jh0t2jJg0fXVoxT0iQ7JE5eOgdRS7SU7SvjQEYVJcxpM/oGXO1GWaZBPiUtnS7n/pCZkPBhCttqYwz+CarwY6pwHnwlsEijxDeKRUI9MFTafqV+N8vMhagkM711QYvC99LY/M/x6rbJyyRmlgySaoQ90mrvwwTUsCk/WSRZxR5GJSLQNPz2K2hgMN375QUzPwHz2FxgRbRmYul/KkCQIXzDZglQkd0V9TYpYqWfq5wba/h/IWlPDqMDB+hGsuNHnx3+EIc/Og8GkXluwKm1SwXy5LF/8jVaLAaym1Q0he/RlmLqHXGxMLNg=="

# # Bucket and folder information
# bucket_name = 'milvus-bucket-eks'
# source_folder = 'source/'
# destination_folder = 'destination/'

# # Call the function
# copy_files_if_not_exists(bucket_name, source_folder, destination_folder, aws_access_key_id, aws_secret_access_key, aws_session_token)
# delete_files_in_folder( bucket_name, source_folder,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,aws_session_token=aws_session_token,)

