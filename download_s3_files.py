import argparse
import os
import boto3

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

parser = parser.parse_args()

def descargar(bucket, download_folder, prefix, key, secret) -> int:
    session = boto3.Session(aws_access_key_id=key, aws_secret_access_key=secret)

    s3 = session.resource("s3")

    my_bucket = s3.Bucket(bucket)

    l = my_bucket.objects.filter(Prefix=prefix)

    index = 0
    for a in l: index+=1

    b = Bar("Downloading...", max=index)

    for file in l:  
        file_name = os.path.basename(file.key)
        f = f"{download_folder}/{file_name}"

        with open(f, 'wb') as data:
            my_bucket.download_fileobj(file.key, data)
            b.next()
    return index

def main():
    total = descargar(parser.bucket, parser.download, parser.prefix, parser.key, parser.secret)

    print(f"\n\n Downloaded {total} files")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit()
    except Exception as ex:
        print(ex)