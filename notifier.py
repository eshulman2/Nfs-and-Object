import inotify.adapters
import boto3

access_key = 'K786GVIA52ASAYHT7FB8'
secret_key = 'vDQNf8WKxTClF1Ail8bSmMxMDys5PThO733lBGg5'
rgw = 'http://192.168.1.16:8000'
bucket = 'test1'
notify_path = '/tmp'

fileIndex = {}

# The sequence of calls that new file in nfs is making.
sequence = 'IN_CREATE,IN_ATTRIB,IN_ATTRIB,IN_MODIFY,IN_CLOSE_WRITE'


def CheckDirectory():
    notifier = inotify.adapters.Inotify()

    # which folder to watch
    notifier.add_watch(notify_path)

    for event in notifier.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event

        print('File: ' + filename + ' Type: ' + type_names[0])

        pathFile = path + '/' + filename

        # TODO: strictly monitor after the the types and not letting it reach 10 type call before
        #      deletion. Check if order is correspond to the the sequence and not just throw it into the array
        if pathFile not in fileIndex:
            fileIndex[pathFile] = []
            fileIndex[pathFile].append(type_names[0])
        else:
            fileIndex[pathFile].append(type_names[0])

        if (len(fileIndex[pathFile]) > 10):
            print('Deleted key')
            fileIndex.pop(pathFile, None)

        elif (sequence in ','.join(fileIndex[pathFile])):
            print('Upload')
            uploadToObject(path, filename)
            genPulicUrl(bucket, filename)
            fileIndex.pop(pathFile, None)
            print(fileIndex)


def uploadToObject(path, filename):
    s3 = boto3.client('s3', endpoint_url=rgw, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.upload_file(path + '/' + filename, bucket, filename)


def genPulicUrl(bucket, filename):
    s3 = boto3.client('s3', endpoint_url=rgw, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.put_object_acl(Key=filename, Bucket=bucket, ACL='public-read')
    print(rgw + '/' + bucket + '/' + filename)
    return rgw + '/' + bucket + '/' + filename


if __name__ == '__main__':
    CheckDirectory()
