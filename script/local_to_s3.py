
import boto3
import os

# Replace with your actual AWS credentials and bucket name

# aws_access_key_id="ASIAXWDKBRAVQDLYIG4B"
# aws_secret_access_key="N2OrCzKTnuN9akuaa4hphsQYeAuHsBdDK6dnac/W"
# aws_session_token="IQoJb3JpZ2luX2VjEI7//////////wEaCXVzLXdlc3QtMiJGMEQCIGzTWlVkKE8aOj1ZXzBlNoe7HTs8sefNO6xgQS2nEsl3AiB088E6ITxZhk5p9oN094kCyiYgwii3NZRxGynrCE2GeCq0AwiH//////////8BEAAaDDUyODUwMzM3NTkxNSIMN3ApZqTnsHPh0XTpKogDWWei1Mmwb0sLXPhWcauFKIHQ0fvJPORhoyilAtd05OHWM5SUUn8xw+hqSplvRBpdrvUKgQbI4oF7/RuIMEq227UH8jg7Bw1oHiBA0ZSqMV2okUY+AOWibh1O2sVhhY+g/MojEwYfSsb5hepKNzGj0xNE5ybecFBG/RUxSK1ygaQhkFhoJhmpdUQpRW0kleE0YAFgqlLxTwnBHyDjAzyh9dnQgxFix0txj4vX3QcGmeu6leI8q908nClFwXXY2faaAdP8ZR4aieKMy+tEaIbGo+F//76v6rjrTWuROhq0dUr9Wr3CROWl6iHF6e6/eXMp8uh0N4cCsbKjTxC3/C0anjhpGX+UcfIYbp+4/M1Qf+McgRjucSsI8tCtEvglMGlSrmUu1hMxPy7OeJHLQ8e8fp0fHrO3Vc+cjWCZ6wEkTHN+VGVYCUQCwEqbXPam8R0jh0t2jJg0fXVoxT0iQ7JE5eOgdRS7SU7SvjQEYVJcxpM/oGXO1GWaZBPiUtnS7n/pCZkPBhCttqYwz+CarwY6pwHnwlsEijxDeKRUI9MFTafqV+N8vMhagkM711QYvC99LY/M/x6rbJyyRmlgySaoQ90mrvwwTUsCk/WSRZxR5GJSLQNPz2K2hgMN375QUzPwHz2FxgRbRmYul/KkCQIXzDZglQkd0V9TYpYqWfq5wba/h/IWlPDqMDB+hGsuNHnx3+EIc/Og8GkXluwKm1SwXy5LF/8jVaLAaym1Q0he/RlmLqHXGxMLNg=="
# LOCAL_DIR='/Users/krishnasundarraobarkule/Desktop/files/'
# BUCKET_NAME='milvus-bucket-eks'
# S3_FOLDER_NAME='source/'  # The folder within the S3 bucket

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
            s3_key = f"{s3_folder_name}{filename}" if s3_folder_name else filename
            with open(local_file_path, "rb") as data:
                bucket.upload_fileobj(data, s3_key)
            print(f"Successfully uploaded {filename} to S3 bucket {bucket_name}/{s3_folder_name}")
        except FileNotFoundError:
            print(f"File not found: {filename}")
        except Exception as e:
            print(f"Error uploading {filename}: {e}")

# if __name__ == "__main__":
#     copy_all_files_to_s3(LOCAL_DIR, BUCKET_NAME, S3_FOLDER_NAME, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
