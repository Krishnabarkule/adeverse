
import boto3
import os

# Replace with your actual AWS credentials and bucket name

# aws_access_key_id='ASIAXWDKBRAV6TF5PQRN'
# aws_secret_access_key='UKA7J3xKUpRftVar3++AapAt6GZOUEiTOp2B39XP'
# aws_session_token='IQoJb3JpZ2luX2VjEP3//////////wEaCXVzLXdlc3QtMiJHMEUCIQD8XevDZezAQ5bk1cBaXQsQWZBmKyx6876ULPUn957rLgIgZhTM4NB9ADNAvAE46KLYzpft+xfpTK/Po6ZoybSBq+AqtAMI5v//////////ARAAGgw1Mjg1MDMzNzU5MTUiDHAji7Hd2hkVJu6qAiqIAyXN6GuMlc0mA84EsTcsqRgOrYH+boHvDUeSd9P+aSWF7GB9lrRaihFTVvadaoD6wgaQUhz8MI1C52jDHOedN+vyS59yD2UgJNTaTp/DAa70UXdZW++L/TPUcw4Hb38WSih97mrGtIY6NWeYXVqr7eX0usA3Ot5XHZ39zibmcDong547S2Qcu2KWhuA0DBekEiz9THMjggv1gjNxNy7HuryA3MYMj6r/UPfVDa8kPXPvyEw0LMPhFfoHeoVnHXrA5Og1fRXWhlFRX1HN+ISXW1zh6ioffeaQgheoGpcewP20/HOinpg7G5g+RsITJeR/rJTljhZkLIvztKMkh28aTZRlQBnaKWwkSJzF+xxzQQ7kKMKAPBewlBRBg6MPfrAWKgyLWvKB1iZKhh3fd7I6i5UKSOGHCadox0i1bBGjcDt0KiV6lP07c0qXEQjsjx+7g5Xiyw5zkBT62MxAvT2lBuB0zxBJ2OinStrkuBLgDwaXsmEyYm8BnDYJifq2nMFttaYURBrVaMkGMLLv+q4GOqYBS/MEqzHSZH1JUKYw6W0RkngxxHQIBYdcNCaVG9bv84zCI5viRYmU2Wlm2v9XS9dICdA9mgzRZagJVLFyUOg7UebEB1m1iDAIUCQ43pbH+lQMaAB3kPZ9wS2Cfy8EioydJMmdSyn35D6uAc8lXvZvf6mh4ISoGPUBiacsq6sxkcb00gGAWgHqED9knZxtsY5rB6ZR/ypR7V8IUCAVGPlNFzC3xvFwgQ=='
# LOCAL_DIR='/Users/krishnasundarraobarkule/Desktop/files/'
# BUCKET_NAME='milvus-bucket-eks'
# S3_FOLDER_NAME='unprocessed'  # The folder within the S3 bucket

# # Specify the local directory containing files


def copy_all_files_to_s3(local_dir, bucket_name, s3_folder_name, aws_access_key_id, aws_secret_access_key, aws_session_token):
    """Copies all files from a local directory to a specific folder in an S3 bucket.

    Args:
        local_dir (str): Path to the local directory containing files.
        bucket_name (str): Name of the S3 bucket to upload to.
        s3_folder_name (str): Name of the folder within the S3 bucket.

    Raises:
        FileNotFoundError: If a file is not found in the local directory.
        Exception: If any other error occurs.
    """

    s3 = boto3.resource("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
    bucket = s3.Bucket(bucket_name)

    for filename in os.listdir(local_dir):
        local_file_path = os.path.join(local_dir, filename)
        try:
            s3_key = f"{s3_folder_name}/{filename}" if s3_folder_name else filename
            with open(local_file_path, "rb") as data:
                bucket.upload_fileobj(data, s3_key)
            print(f"Successfully uploaded {filename} to S3 bucket {bucket_name}/{s3_folder_name}")
        except FileNotFoundError:
            print(f"File not found: {filename}")
        except Exception as e:
            print(f"Error uploading {filename}: {e}")

# if __name__ == "__main__":
#     copy_all_files_to_s3(LOCAL_DIR, BUCKET_NAME, S3_FOLDER_NAME, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
