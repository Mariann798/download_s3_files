import argparse
import os
import boto3
import concurrent.futures

from progress.bar import Bar

DEFAULT_FOLDER = os.getcwd()

ACCESS_KEY = os.getenv('ACCESS_KEY') 
AWS_SECRET = os.getenv('AWS_SECRET')

parser = argparse.ArgumentParser(
    prog="download_s3_files",
    description="Download S3 files filter by prefix",
    epilog="by Mariann798"
)

parser.add_argument("-p", "--prefix", help="Prefix of files", required=True)
parser.add_argument("-d", "--download", help="Download folder", default=DEFAULT_FOLDER)
parser.add_argument("-b", "--bucket", help="Bucket name", required=True)

parser.add_argument("-k", "--key", help="Access Key", default=ACCESS_KEY)
parser.add_argument("-s", "--secret", help="AWS Secret", default=AWS_SECRET)

parser.add_argument("-c", "--parallelism", help="Number of downloads in parallelism", default=1)

parser = parser.parse_args()

def download_s3(bucket_session, key, download_folder):
    file_name = os.path.basename(key)
    f = f"{download_folder}/{file_name}"

    with open(f, 'wb') as data:
        bucket_session.download_fileobj(key, data)
        

def download(bucket, download_folder, prefix, key, secret, parallelism = 1) -> int:
    session = boto3.Session(aws_access_key_id=key, aws_secret_access_key=secret)

    s3 = session.resource("s3")

    my_bucket = s3.Bucket(bucket)

    l = my_bucket.objects.filter(Prefix=prefix)
  
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = [executor.submit(download_s3, my_bucket, file.key, download_folder) for file in l]

        index = 0
        for _ in l: index+=1
        b = Bar("Downloading...", max=index)
        for _ in concurrent.futures.as_completed(futures):
            b.next() 

    return index

def main():
    total = download(parser.bucket, parser.download, parser.prefix, parser.key, parser.secret, int(parser.parallelism))

    print(f"\n\n Downloaded {total} files")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit()
    except Exception as ex:
        print(ex)