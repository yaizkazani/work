import os, time, subprocess, pathlib, sys, logging, re

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s -%(levelname)s -%(message)s')

media_servers_with_MSDP = {"dk-prod-nbumed01" : False,
                           "dk-prod-nbumed02" : False,
                           "dk-prod-nbumed03" : True,
                           "dk-prod-nbumed04" : True,
                           "dk-prod-nbumed05" : True,
                           "dk-prod-nbumed06" : True,
                           "dk-prod-nbumed07" : True,
                           "dk-prod-nbumed08" : True,
                           "dk-prod-nbumed09" : True,
                           "dk-prod-nbumed10" : True,
                           "dk-prod-nbumed11" : True,
                           "dk-prod-nbumed12" : True,
                           "dk-prod-prismed01": True,
                           "dk-prod-prismed02": True,
                           "dk-prod-prismed03": True,
                           "dk-prod-prismed04": True,
                           "dk-prod-prismed05": True,
                           "dk-prod-prismed06": True}

general_nb_processes = ["vnetd", "bpcd", "nbdisco", "nbrmms", "nbsl", "nbsvcmon"]
nb_msdp_processes = ["spad", "spoold"]


class Media_server():
	"""Class representing Netbackup Media Server"""

	def __init__(self, name):
		"""Each Media_server has a name, system info (only swap so far), and some conditions"""
		self.name = name
		self.free_swap_space = 101
		self.low_swap_condition = False
		self.running_backups_condition = True
		self.msdp_server = media_servers_with_MSDP[self.name.split(".")[0]]
		self.netbackup_processes_running = False

	def check_server_status(self) -> None:
		"""Get self swap status, determine low swap and running backups conditions"""
		server_name = self.name
		if not server_name:
			logging.error("from: check_server_status()\nMedia server name is not specified")
			return None

		# Getting swap data from server

		self.free_swap_space = get_media_server_data(data_type="swap", media_server=server_name)

		if 0 <= self.free_swap_space < 300:
			self.low_swap_condition = True
		elif self.free_swap_space < 0:
			logging.error(f"Error detected while getting parameters for media server: \n\tMedia server name: {self.name}, parameter: free_swap_space")

		# Checking if backups are running

		self.running_backups_condition = check_running_backups(server_name)

	def release_swap(self) -> [None, str]:
		"""Connect to server, stop Netbackup services, disable-enable swap, start Netbackup services"""
		server_name = self.name
		if not server_name:
			logging.error("from: release_swap()\nMedia server name is not specified")
			return None
		try:
			subprocess.getoutput(f"ssh root@{server_name} service netbackup stop && swapoff -a && swapon -a && service netbackup start")
		except Exception as e:
			logging.error(f"from: release_swap()\n{e}")
			return f"{e}"
		return "OK"

	def check_netbackup_processes(self) -> [list, None]:
		"""Connect to server and check if all Netbackup processes are running"""
		server_name = self.name
		if not server_name:
			logging.error("from: check_netbackup_processes()\nMedia server name is not specified")
			return None
		try:
			nb_processes = subprocess.getoutput(f"ssh root@{server_name} eval $(locate bpps | sed -n '2 p') -a").split("/")
			print(nb_processes)
		except Exception as e:
			logging.error(f"from: release_swap()\n{e}")
			return None

		missing_nb_processes = []

		for process in general_nb_processes:
			if process not in nb_processes:
				missing_nb_processes.append(process)
		self.msdp_server = media_servers_with_MSDP[server_name.split(".")[0]]

		if self.msdp_server:
			for process in general_nb_processes:
				if process not in nb_msdp_processes:
					missing_nb_processes.append(process)
		self.netbackup_processes_running = True if not missing_nb_processes else False
		return missing_nb_processes


def get_media_server_list() -> list:
	"""Read bp.conf, extract media server list, filter it and return as list"""
	media_server_list = subprocess.getoutput(r"cat exec $(locate bp.conf | sed -n '1 p') | egrep '^MEDIA_SERVER'")
	filtered_media_server_list = [item.split("=")[1].strip() for item in media_server_list.split("\n") if "MEDIA_SERVER" in item]
	if not filtered_media_server_list:
		logging.critical("from: get_media_server_list()\nMedia server list is empty")
		raise ValueError("get_media_server_list: No media servers found")
	return filtered_media_server_list


def get_media_server_data(data_type="swap", media_server=None) -> [int, None]:
	"""connect to server, get free swap amount and return as int MB"""
	possible_data_types = ["swap"]  # ! add data types here

	if not media_server:
		logging.error("from: get_media_server_data()\nMedia server is not specified")
		return -1
	elif data_type not in possible_data_types:
		logging.error(f"from: get_media_server_data()\nIncorrect type argument: {data_type}")
		return -1

	data_type_command = {"swap": "free -m"}.get(data_type, None)
	try:
		getoutput_command = fr"ssh root@{media_server} {data_type_command}"
		logging.info(f"get data: getoutput_command = {getoutput_command}")
		media_server_data = subprocess.getoutput(getoutput_command)

	except Exception as e:
		logging.error(f"from: get_media_server_data()\nError while connecting to media server {media_server}, Error: {e}")
		return -1

	data_processing_command = {"swap": r"var = media_server_data.split('\n')[3].split()[3]"}.get(data_type,
	                                                                                             f'logging.error("from: get_media_server_data()\ndata_processing_command not found: {data_type}")')
	logging.info(f"data_processing_command = {data_processing_command}")
	exec_vars = {}
	exec(data_processing_command, locals(), exec_vars)

	return int(exec_vars["var"])


def check_running_backups(media_server=None):
	if not media_server:
		logging.error("from: get_media_server_data()\nMedia server is not specified")
		return None

	try:
		linux_command = r"eval $(locate bpps | sed -n '2 p') -a"
		getoutput_command = fr"ssh root@{media_server} {linux_command}"
		logging.info(f"check backups: getoutput_command = {getoutput_command}")
		running_backups_raw = subprocess.getoutput(getoutput_command)

	except Exception as e:
		logging.error(f"from: check_running_backups()\nError while connecting to media server {media_server}, Error: {e}")
		running_backups_raw = True

	return True if re.search(r"\s-backup\s", running_backups_raw) else False


media_server_names = get_media_server_list()
media_servers = []

[media_servers.append(Media_server(name=media_server_name)) for media_server_name in media_server_names]

for media_server_exemplar in media_servers:
	media_server_exemplar.check_server_status()
	print(f"Media server name = {media_server_exemplar.name}\n"
	      f"Media server free swap = {media_server_exemplar.free_swap_space}\n"
	      f"Media server running backups condition = {media_server_exemplar.running_backups_condition}\n"
	      f"Media server low swap condition = {media_server_exemplar.low_swap_condition}")
	print(f"Checking Netbackup processes for {media_server_exemplar.name}, status: {media_server_exemplar.check_netbackup_processes()}")
	if media_server_exemplar.low_swap_condition and not media_server_exemplar.running_backups_condition:
		logging.warning(f"Swap release attempt done for {media_server_exemplar.name}, status: {media_server_exemplar.release_swap()}")
		logging.warning(f"Checking Netbackup processes for {media_server_exemplar.name}, status: {media_server_exemplar.check_netbackup_processes()}")

#TODO
# 1. Add Logging: a) Write log each time we check media server conditions
#                 b) Each day if there are < than 1.5h until midnight - run daily report: read all previous report files, zip them, move to archive folder
#				  c) Email daily report after b)
#                 d) Each 1st day of the month delete old archives - keep for 3 months
#                 e) Track attempts for each server per day to see where it is hard to release swap. Include this in daily letter
# 2. Add swap release: a) Run ssh root@media1 "service netbackup stop && swapoff -a && swapon -a && service netbackup start  - OK
#					   b) Check if services are running, add new condition - services_running                                - OK
#                      c) If services are not running - start them again, check again if won't start - email an alert to the team
#
