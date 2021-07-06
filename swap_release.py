import os, time, subprocess, pathlib, sys, logging

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s -%(levelname)s -%(message)s')


def get_media_server_list():
	media_server_list = subprocess.getoutput(r"cat exec $(locate bp.conf | sed -n '1 p') | egrep '^MEDIA_SERVER'")
	filtered_media_server_list = [item.split("=")[1].strip() for item in media_server_list.split("\n") if "MEDIA_SERVER" in item]
	if not filtered_media_server_list:
		logging.critical("from: get_media_server_list()\nMedia server list is empty")
	return filtered_media_server_list

def get_media_server_data(type="swap", media_server=None):
	if not media_server:
		logging.error("from: get_media_server_data()\nMedia server is not specified")

	command = {"swap": "free -m"}.get(type, f"echo 'incorrect command provided: {type}' >> /root/Peter_swap_script_command_error.log")
	try:
		media_server_data = subprocess.getoutput(fr"ssh root@{media_server} {command}")

	except Exception as e:
		logging.error(f"from: get_media_server_data()\nError occured while connecting to media server {media_server}, Error: {e}")


print(get_media_server_data(type="swap", media_server=get_media_server_list()[0]))

