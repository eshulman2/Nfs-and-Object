import inotify.adapters
import boto3

access_key = 'VC3KPVLT00J2M80Z1Y9G'
secret_key = 'O0Y9640BeCMuCwNEU6GsMKirgHXUAiGsDw4RqzuF'
rgw="http://192.168.14.26:8000"

fileIndex = {}

# The sequence of calls that new file in nfs is making.
sequence = "IN_CREATE,IN_ATTRIB,IN_ATTRIB,IN_MODIFY,IN_CLOSE_WRITE"

def CheckDirectory():
    i = inotify.adapters.Inotify()
    
    # which folder to watch
    i.add_watch('/nfs')
    
    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event
        
        print("File: " + filename + " Type: " + type_names[0])
        
        pathFile = path + "/" + filename
        
        #TODO: strictly monitor after the the types and not letting it reach 10 type call before
        #      deletion. Check if order is correspond to the the sequence and not just throw it into the array
        if(fileIndex.has_key(pathFile) == False):
            fileIndex[pathFile] = []
            fileIndex[pathFile].append(type_names[0])
        else:
            fileIndex[pathFile].append(type_names[0])
        
        if(len(fileIndex[pathFile]) > 10):
                print("Deleted key")
                fileIndex.pop(pathFile,None)
                
        elif(sequence in ",".join(fileIndex[pathFile])):
            print("Upload")
            uploadToObject(path,filename)
            fileIndex.pop(pathFile,None)
            print(fileIndex)
            
def uploadToObject(path,filename):
    
    s3 = boto3.client('s3',endpoint_url=rgw,aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    s3.upload_file(path + "/" + filename, "my-buc",filename)

if __name__ == '__main__':
    CheckDirectory()