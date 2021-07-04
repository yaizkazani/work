import os, time, subprocess, pathlib

def get_media_server_list():
	media_server_list = subprocess.run([r"cat exec $(locate bp.conf | sed -n '1 p') | egrep '^MEDIA_SERVER'"], shell=True)

print(get_media_server_list())

#test
