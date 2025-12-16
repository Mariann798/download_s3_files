import argparse
import os
import logging
import boto3
import botocore
import concurrent.futures
from tqdm import tqdm

DEFAULT_FOLDER = os.getcwd()

parser = argparse.ArgumentParser(
    prog="download_s3_files",
    description="Download S3 files filter by prefix",
    epilog="by Mariann798"
)

parser.add_argument("-p", "--prefix", help="Prefix of files", required=True)
parser.add_argument("-d", "--download", help="Download folder", default=DEFAULT_FOLDER)
parser.add_argument("-b", "--bucket", help="Bucket name", required=True)

parser.add_argument("-r", "--region", help="AWS Region", default=None)
parser.add_argument("-c", "--parallelism", help="Number of downloads in parallelism", default=10)

parser = parser.parse_args()

def download_s3(bucket_session, key, download_folder):
    file_name = os.path.basename(key)
    f = f"{download_folder}/{file_name}"

    with open(f, 'wb') as data:
        bucket_session.download_fileobj(key, data)
        

def download(bucket, download_folder, prefix, region, parallelism=10) -> int:
    session = boto3.Session(region_name=region)
    s3 = session.resource("s3")

    my_bucket = s3.Bucket(bucket)

    files = my_bucket.objects.filter(Prefix=prefix)

    with tqdm(desc="Downloading", unit="file") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
            futures = [
                executor.submit(download_s3, my_bucket, file.key, download_folder)
                for file in files
            ]
            for future in concurrent.futures.as_completed(futures):
                pbar.update(1)

    return len(futures)

def main():
    total = download(
        parser.bucket,
        parser.download,
        parser.prefix,
        parser.region,
        int(parser.parallelism),
    )

    print(f"\n\n Downloaded {total} files")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        main()
    except KeyboardInterrupt:
        exit()
    except botocore.exceptions.ClientError as ex:
        logging.error(f"AWS client error: {ex}")
    except FileNotFoundError:
        logging.error("The download folder was not found")
    except Exception as ex:
        logging.error(f"An unexpected error occurred: {ex}")