
import boto3

# Replace these with your actual AWS credentials
# aws_access_key_id = 'ASIAXWDKBRAV6TF5PQRN'
# aws_secret_access_key = 'UKA7J3xKUpRftVar3++AapAt6GZOUEiTOp2B39XP'
# aws_session_token = 'IQoJb3JpZ2luX2VjEP3//////////wEaCXVzLXdlc3QtMiJHMEUCIQD8XevDZezAQ5bk1cBaXQsQWZBmKyx6876ULPUn957rLgIgZhTM4NB9ADNAvAE46KLYzpft+xfpTK/Po6ZoybSBq+AqtAMI5v//////////ARAAGgw1Mjg1MDMzNzU5MTUiDHAji7Hd2hkVJu6qAiqIAyXN6GuMlc0mA84EsTcsqRgOrYH+boHvDUeSd9P+aSWF7GB9lrRaihFTVvadaoD6wgaQUhz8MI1C52jDHOedN+vyS59yD2UgJNTaTp/DAa70UXdZW++L/TPUcw4Hb38WSih97mrGtIY6NWeYXVqr7eX0usA3Ot5XHZ39zibmcDong547S2Qcu2KWhuA0DBekEiz9THMjggv1gjNxNy7HuryA3MYMj6r/UPfVDa8kPXPvyEw0LMPhFfoHeoVnHXrA5Og1fRXWhlFRX1HN+ISXW1zh6ioffeaQgheoGpcewP20/HOinpg7G5g+RsITJeR/rJTljhZkLIvztKMkh28aTZRlQBnaKWwkSJzF+xxzQQ7kKMKAPBewlBRBg6MPfrAWKgyLWvKB1iZKhh3fd7I6i5UKSOGHCadox0i1bBGjcDt0KiV6lP07c0qXEQjsjx+7g5Xiyw5zkBT62MxAvT2lBuB0zxBJ2OinStrkuBLgDwaXsmEyYm8BnDYJifq2nMFttaYURBrVaMkGMLLv+q4GOqYBS/MEqzHSZH1JUKYw6W0RkngxxHQIBYdcNCaVG9bv84zCI5viRYmU2Wlm2v9XS9dICdA9mgzRZagJVLFyUOg7UebEB1m1iDAIUCQ43pbH+lQMaAB3kPZ9wS2Cfy8EioydJMmdSyn35D6uAc8lXvZvf6mh4ISoGPUBiacsq6sxkcb00gGAWgHqED9knZxtsY5rB6ZR/ypR7V8IUCAVGPlNFzC3xvFwgQ=='


def copy_files_within_bucket(bucket_name, s3_source_folder, s3_target_folder, aws_access_key_id, aws_secret_access_key, aws_session_token):
    # Create an S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    try:
        # List objects in the source folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_source_folder)

        # Iterate through the objects and copy them to the destination folder
        for obj in response.get('Contents', []):
            source_key = obj['Key']
            destination_key = f"{s3_target_folder}/{source_key.split('/')[-1]}"

            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)

            print(f"File {source_key} copied to {destination_key}")

    except Exception as e:
        print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     # Replace these values with your actual bucket name, source folder, and destination folder
#     bucket_name = 'milvus-bucket-eks'
#     source_folder = 'unprocessed'
#     destination_folder = 'processed'

#     copy_files_within_bucket(bucket_name, source_folder, destination_folder, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)






import boto3

def delete_files_in_folder(bucket_name, source_folder, aws_access_key_id, aws_secret_access_key, aws_session_token):
    # Create an S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

    try:
        # List objects in the source folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)

        # Delete files from the source folder
        for obj in response.get('Contents', []):
            source_key = obj['Key']
            s3.delete_object(Bucket=bucket_name, Key=source_key)
            print(f"File {source_key} deleted from {source_folder}")

    except Exception as e:
        print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     # Replace these values with your actual bucket name, source folder, and destination folder
#     bucket_name = 'milvus-bucket-eks'
#     source_folder = 'unprocessed'
#     destination_folder = 'processed'

#     delete_files_in_folder(bucket_name, source_folder, destination_folder, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
