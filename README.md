# Introduction
This python scripts detect a new file in nfs and then upload it to an s3 based object storage

## getting started
First you need to installed the requiered packages
```
pip install -r requierments.txt
```
Then run the script, it will look for changes in /nfs folder.
If the change was creating a new file then the script upload it to the object storage
