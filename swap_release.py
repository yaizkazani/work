import os, time, subprocess, pathlib, sys, logging

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s -%(levelname)s -%(message)s')


class Media_server():
	"""Class representing Netbackup Media Server"""

	def __init__(self, name):
		self.name = name
		self.free_swap_space = 101
		self.low_swap_condition = False

	def check_server_status(self):
		server_name = self.name
		if not server_name:
			logging.error("from: check_server_status()\nMedia server is not specified")
			return None

		# Getting swap data from server

		free_swap_space = get_media_server_data(data_type="swap", media_server=server_name)

		if free_swap_space < 100:
			self.low_swap_condition = True


def get_media_server_list():
	media_server_list = subprocess.getoutput(r"cat exec $(locate bp.conf | sed -n '1 p') | egrep '^MEDIA_SERVER'")
	filtered_media_server_list = [item.split("=")[1].strip() for item in media_server_list.split("\n") if "MEDIA_SERVER" in item]
	if not filtered_media_server_list:
		logging.critical("from: get_media_server_list()\nMedia server list is empty")
	return filtered_media_server_list


def get_media_server_data(data_type="swap", media_server=None):
	possible_data_types = ["swap"]  # ! add data types here

	if not media_server:
		logging.error("from: get_media_server_data()\nMedia server is not specified")
		return None
	elif data_type not in possible_data_types:
		logging.error(f"from: get_media_server_data()\nIncorrect type argument: {data_type}")
		return None

	data_type_command = {"swap": "free -m"}.get(data_type, None)
	try:
		getoutput_command = fr"ssh root@{media_server} {data_type_command}"
		logging.info(f"getoutput_command = {getoutput_command}")
		media_server_data = subprocess.getoutput(getoutput_command)

	except Exception as e:
		logging.error(f"from: get_media_server_data()\nError while connecting to media server {media_server}, Error: {e}")
		media_server_data = 0

	data_processing_command = {"swap": r"var = media_server_data.split('\n')[3].split()[3]"}.get(data_type,
	                                                                                             f'logging.error("from: get_media_server_data()\ndata_processing_command not found: {data_type}")')
	logging.info(f"data_processing_command = {data_processing_command}")
	exec_vars = {}
	exec(data_processing_command, locals(), exec_vars)

	return exec_vars["var"]


media_server_names = get_media_server_list()
media_servers = []

[media_servers.append(Media_server(name=media_server_name)) for media_server_name in media_server_names]

for media_server_exemplar in media_servers:
	media_server_exemplar.check_server_status()
	print(f"Media server name = {media_server_exemplar.name}\nMedia server free swap = {media_server_exemplar.free_swap_space}")
