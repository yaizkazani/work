import os, time, subprocess, pathlib

def get_media_server_list():
	media_server_list = subprocess.getoutput(r"cat exec $(locate bp.conf | sed -n '1 p') | egrep '^MEDIA_SERVER'")
	return [item.split("=")[1].strip() for item in media_server_list.split("\n") if "MEDIA_SERVER" in item]

print(get_media_server_list())


