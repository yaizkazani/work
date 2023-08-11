import os, datetime, subprocess, pathlib, sys, logging, re, calendar
import time

from crontab import CronTab

# nb Paths + folders creation
temp_logs_folder_path = pathlib.Path.joinpath(pathlib.Path.cwd(), "swap_release_temp_logs")  # We keep logs for runs through day here
temp_logs_path = pathlib.Path.joinpath(temp_logs_folder_path,
                                       f"temp_log_{datetime.datetime.now().strftime('%H-%M')}.log")  # Pointer to the current temp log file
temp_logs_archive_folder_path = pathlib.Path.joinpath(pathlib.Path.cwd(),
                                                      "swap_release_temp_logs_archive")  # We keep temp logs archives that we make daily after daily log was created here

daily_logs_folder_path = pathlib.Path.joinpath(pathlib.Path.cwd(),
                                               "swap_release_daily_logs")  # We keep daily logs that contain all temp log data for a single day here
daily_log_path = pathlib.Path.joinpath(daily_logs_folder_path,
                                       f"daily_log_{datetime.datetime.now().strftime('%m-%d-%H-%M')}.log")  # Pointer to the current daily log file

daily_logs_archive_folder_path = pathlib.Path.joinpath(pathlib.Path.cwd(),
                                                       "swap_release_daily_logs_archive")  # We keep daily log archives here, archiving runs each 10 days

os.makedirs(f"{temp_logs_folder_path.__str__()}", exist_ok=True)
os.makedirs(f"{daily_logs_folder_path.__str__()}", exist_ok=True)
os.makedirs(f"{daily_logs_archive_folder_path.__str__()}", exist_ok=True)
os.makedirs(f"{temp_logs_archive_folder_path.__str__()}", exist_ok=True)

# ! logging to file
logging.basicConfig(filename=f"{temp_logs_path.__str__()}", filemode="a", level=logging.DEBUG,
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s')

# ! logging to stdout
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

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

general_nb_processes = ["vnetd", "bpcd", "nbdisco", "nbrmms", "nbsl", "nbsvcmon"]  # processes that should be running on each media server
nb_msdp_processes = ["spad", "spoold"]  # processes that run only on media servers with MSDP connected


class Media_server:
	"""Class representing Netbackup Media Server"""

	def __init__(self, name):
		"""Each Media_server has a name, system info (only swap so far), and some conditions"""
		self.name = name
		self.free_swap_space = 101
		self.low_swap_condition = False
		self.running_backups_condition = True
		self.running_restore_condition = True
		self.msdp_server = media_servers_with_MSDP[self.name.split(".")[0]]  # get True/False from dict ↑
		if not self.name.split(".")[0] in media_servers_with_MSDP:
			logging.critical(f"Server name is not correct, current name is full: {self.name}, short: {self.name.split('.')[0]}")
			raise ValueError(f"Server name is not correct, current name is full: {self.name}, short: {self.name.split('.')[0]}")

	def check_server_status(self) -> None:
		"""Get self swap status, determine low swap and running backups conditions"""

		self.free_swap_space = self.get_media_server_data()

		if 0 <= self.free_swap_space < 500:
			self.low_swap_condition = True
		elif not self.free_swap_space:
			logging.error(f"Error detected while getting parameters for media server: \n\tMedia server name: {self.name}, parameter: free_swap_space\n\tValue={self.free_swap_space}")

		# note Checking if backups are running

		self.running_backups_condition = self.check_running_backups()

	def get_media_server_data(self, data_type="swap") -> [int, None]:
		"""connect to server, get free swap amount and return as int MB"""
		possible_data_types = ["swap"]  # ! add data types here
		media_server = self.name

		if data_type not in possible_data_types:
			logging.error(f"from: get_media_server_data()\nIncorrect type argument: {data_type}")
			return None

		data_type_command = {"swap": "free -m"}.get(data_type, None)
		try:
			getoutput_command = fr"ssh root@{media_server} {data_type_command}"
			media_server_data = subprocess.getoutput(getoutput_command)

		except Exception as e:
			logging.error(f"from: get_media_server_data()\nError while connecting to media server {media_server}, Error: {e}")
			return None


		data_processing_command = {"swap": r"var = ''.join([string.split()[3] for string in media_server_data.split('\n') if 'Swap' in string])"}[data_type]


		exec_vars = {}

		try:
			exec(data_processing_command, locals(), exec_vars)
		except Exception as e:
			logging.error(f'logging.error("from: get_media_server_data()\nwhile running exec, error = {e}")')

		return int(exec_vars["var"])

	def check_running_backups(self) -> [True, False]:
		r"""Connect to server, run eval /usr/openv/netbackup/bin/bpps -a, parse with re.search(r"\s-backup\s", running_backups_raw)
		And restores with re.search(r"\s-restore\s", running_backups_raw), duplication with r"\s-dup\s" and  r"\s-copy\s" """
		server_name = self.name

		try:
			getoutput_command = fr"ssh root@{server_name} /usr/openv/netbackup/bin/bpps -a"
			running_backups_raw = subprocess.getoutput(getoutput_command)

		except Exception as e:
			logging.error(f"from: check_running_backups()\nError while connecting to media server {server_name}, Error: {e}")
			return True

		if re.search(r"\s-backup\s", running_backups_raw):
			return "Backups are running"
		elif re.search(r"\s-restore\s", running_backups_raw):
			return "Restore is running"
		elif re.search(r"\s-dup\s", running_backups_raw):
			return "Duplication is running"
		elif re.search(r"\s-copy\s", running_backups_raw):
			return "Duplication (copy) is running"
		else:
			return False

	def release_swap(self) -> [None, str]:
		"""Connect to server, stop Netbackup services, disable-enable swap, start Netbackup services"""
		server_name = self.name

		try:
			# ! This command ↓ sometimes leave Netbackup services in down state so we double check that
			subprocess.getoutput(f"ssh root@{server_name} service netbackup stop && swapoff -a && swapon -a && service netbackup start")
		except Exception as e:
			logging.error(f"from: release_swap()\n{e}")
			return f"{e}"
		return "OK"

	def check_netbackup_processes(self, recursion=False) -> [list, None]:
		"""Connect to server and check if all Netbackup processes are running
		If not: try to start them, wait 5 second, make a recursive call to itself to check once more"""
		server_name = self.name

		try:
			nb_processes = subprocess.getoutput(f"ssh root@{server_name} eval /usr/openv/netbackup/bin/bpps -a")
			logging.info(f"Got processes from {server_name}")
		except Exception as e:
			logging.error(f"from: check_netbackup_processes()\n{e}")
			return None

		missing_nb_processes = []

		# note we are going through all Netbackup processes and check if they are present on server
		for process in general_nb_processes:
			if not re.search(f"/{process}", nb_processes):
				missing_nb_processes.append(process)
		# note checking if our server has MSDP connected
		self.msdp_server = media_servers_with_MSDP[server_name.split(".")[0]]

		if self.msdp_server:
			for process in nb_msdp_processes:
				if not re.search(f"/{process}", nb_processes):
					missing_nb_processes.append(process)
		if recursion and "No missing processes found!" in missing_nb_processes:  # nb to process recursive call since our list wont be empty and will have "No missing processes found!" in it
			missing_nb_processes = []

		if missing_nb_processes:
			logging.info(f"from: check_netbackup_processes, MISSING SERVICES: {missing_nb_processes}\nServer: {self.name}\nStarting services... ")
			self.start_netbackup_services()
			time.sleep(5)
			if not recursion:
				missing_nb_processes = self.check_netbackup_processes(recursion=True)

		return missing_nb_processes if missing_nb_processes else "No missing processes found!"

	def start_netbackup_services(self) -> None:
		"""Connect to server and start Netbackup processes"""
		server_name = self.name

		try:
			subprocess.getoutput(f"ssh root@{server_name} service netbackup start")
			logging.info(f"Netbackup services started on {server_name}")
		except Exception as e:
			logging.error(f"from: start_netbackup_services()\n{e}")
			return None


def get_media_server_list() -> list:
	"""Read bp.conf, extract media server list, filter it and return as list, must be run on master server"""
	media_server_list = subprocess.getoutput(r"cat /usr/openv/netbackup/bp.conf | egrep '^MEDIA_SERVER'")
	filtered_media_server_list = [item.split("=")[1].strip() for item in media_server_list.split("\n") if "MEDIA_SERVER" in item]
	if not filtered_media_server_list:
		logging.critical("from: get_media_server_list()\nMedia server list is empty")
		raise ValueError("from: get_media_server_list: No media servers found")
	return filtered_media_server_list


def prepare_environment():
	"""Look for PyPy as we mostly use it as interpreter, if not found try to use python3"""
	pypy_location = subprocess.check_output('locate pypy3.6 | egrep "pypy3.6$" | sed -n "1 p"', shell=True).decode()
	if not pypy_location:
		print("PyPy location was not found, trying to updatedb and search again")
		subprocess.run("updatedb", shell=True)
		pypy_location = subprocess.check_output('locate pypy3.6 | egrep "pypy3.6$" | sed -n "1 p"').decode()
		if not pypy_location:
			logging.info("Warning. PyPy was not found - cron jobs might fail. Trying to search for Python3")
			python3_location = subprocess.check_output('which python3', shell=True).decode()
			if not python3_location:
				logging.critical("from: prepare_environment, PyPy and Python3 locations were not found")
				sys.exit("Python3 was not found, PyPy was not found, exiting.")
			else:
				return python3_location.strip("\n")
		else:
			return pypy_location.strip("\n")
	else:
		return pypy_location.strip("\n")


def prepare_cron_jobs() -> None:
	"""
	Checks if cron jobs used for our script are in place. If not they are created
	First job runs at 00:01 AM, they run each hour
	"""
	cron = CronTab(user="root")
	interpreter_location = prepare_environment()
	for cron_job_comment in ["swap_release_job"]:
		if not [i for i in cron.find_comment(cron_job_comment)]:  # Thing returns iterable so we have to go through it to check if empty
			logging.info("Cron jobs for running swap_release.py not found. Trying to create them")
			job = cron.new(command=f"cd {pathlib.Path.cwd()} && {interpreter_location} {pathlib.Path.joinpath(pathlib.Path.cwd(), __file__)}",
			               comment=cron_job_comment)  # create job
			if "swap_release_job" in cron_job_comment:
				job.hour.every(1)
				job.minute.on(1)
			job.enable()  # enable job
			cron.write()  # close cron and write changes


def create_daily_log() -> None:
	"""Read all temp logs, collect data, create daily log, put data into it"""
	daily_log_data = []
	try:
		for file in temp_logs_folder_path.glob("temp_log_*"):
			daily_log_data.append(file.open(mode="r", encoding="utf8").readlines())
		with open(daily_log_path, mode="w", encoding="utf8") as daily_log_file:
			for log in daily_log_data:
				daily_log_file.writelines(log)
				daily_log_file.write("\n\n")
	except Exception as e:
		logging.error(f"from: create_daily_log, error while creating daily log: {e}")
	else:
		compress_temp_logs_move_to_archive()


def compress_temp_logs_move_to_archive() -> None:
	"""Compress all temp logs and move them to archive. If successful - delete them"""
	try:
		# subprocess.run(f"cd {temp_logs_folder_path} && tar -cf {temp_logs_archive_folder_path}/{datetime.datetime.now().strftime('%d-%m')}.tar temp_log_*",
		#                shell=True)
		subprocess.run(
			f"cd {temp_logs_folder_path} && tar -cf {pathlib.Path.joinpath(temp_logs_archive_folder_path, datetime.datetime.now().strftime('%d-%m')).__str__()}_temp_logs.tar temp_log_*",
			shell=True)
		logging.info("from: compress_temp_logs_move_to_archive. Temp logs were compressed")
	except Exception as e:
		logging.error(f"from: compress_temp_logs_move_to_archive (compress), error: {e}")
	else:
		for file in pathlib.Path(temp_logs_folder_path).glob("temp_log_*"):
			try:
				subprocess.run(f"rm -f {file}", shell=True)
			except Exception as e:
				logging.error(f"from: compress_temp_logs_move_to_archive(delete), error: {e}")
		logging.info("from: compress_temp_logs_move_to_archive. Temp logs were deleted")


def compress_daily_logs_move_to_archive() -> None:
	"""Each 10 days we compress daily logs, archive them then delete"""
	now = datetime.datetime.now()
	last_day_of_month = calendar.monthrange(now.year, now.month)[1]
	if now.day % 10 == 0 or (now.month == 2 and now.day == last_day_of_month):  # Should also work on 28.02
		try:
			# subprocess.run(f"cd {daily_logs_folder_path} && tar -cf {daily_logs_archive_folder_path}/{now.strftime('%d-%m')}.tar daily_log_*",
			#                shell=True)
			subprocess.run(
				f"cd {daily_logs_folder_path} && tar -cf {pathlib.Path.joinpath(daily_logs_archive_folder_path, now.strftime('%d-%m')).__str__()}_daily_logs.tar daily_log_*",
				shell=True)
			logging.info("from: compress_daily_logs_move_to_archive. Daily logs were compressed")
		except Exception as e:
			logging.error(f"from: compress_daily_logs_move_to_archive (compress), error: {e}")
		else:
			for file in pathlib.Path(daily_logs_folder_path).glob("daily_log_*"):
				try:
					subprocess.run(f"rm -f {file}", shell=True)
				except Exception as e:
					logging.error(f"from: compress_daily_logs_move_to_archive(delete), error: {e}")
			logging.info("from: compress_daily_logs_move_to_archive. Daily logs were deleted")


def remove_old_archives():
	"""Each 6 days we delete old archives for temp logs (keep 5 newest)
	And each 1st day of the month we delete old archives for daily logs (keep 3 newest = for 3 months)"""

	# Temp logs
	if datetime.datetime.now().day % 6 == 0:
		archives = list(pathlib.Path(temp_logs_archive_folder_path).glob("*"))
		archives.sort(key=lambda x: os.path.getmtime(x), reverse=True)  # sort list of archives by modification date
		to_delete = [archives[i] for i in range(len(archives)) if i > 4]  # get 5 newest archives to keep
		try:
			[subprocess.run(f"rm -f {archive}") for archive in to_delete]
			logging.info("from: remove_old_archives. Old temp log archives were deleted")
		except Exception as e:
			logging.error(f"from: remove_old_archives(temp). error: {e}")

	# Daily logs
	if datetime.datetime.now().day == 1:
		archives = list(pathlib.Path(daily_logs_archive_folder_path).glob("*"))
		archives.sort(key=lambda x: os.path.getmtime(x), reverse=True)  # sort list of archives by modification date
		to_delete = [archives[i] for i in range(len(archives)) if i > 2]  # get 3 newest archives to keep
		try:
			[subprocess.run(f"rm -f {archive}") for archive in to_delete]
			logging.info("from: remove_old_archives. Old daily log archives were deleted")
		except Exception as e:
			logging.error(f"from: remove_old_archives(daily). error: {e}")
	else:
		return


prepare_cron_jobs()
media_server_names = get_media_server_list()
media_servers = []

[media_servers.append(Media_server(name=media_server_name)) for media_server_name in media_server_names if
 (media_server_name.split(".")[0] in media_servers_with_MSDP or media_server_name in media_servers_with_MSDP)]
# We are checking both long and short name to be present in media server list.

# nb hourly swap check
for media_server_exemplar in media_servers:
	media_server_exemplar.check_server_status()
	logging.info(f"Media server NAME = {media_server_exemplar.name}\n"
	             f"Media server FREE SWAP = {media_server_exemplar.free_swap_space}MB\n"
	             f"Media server RUNNING BACKUPS = {media_server_exemplar.running_backups_condition}\n"
	             f"Media server LOW SWAP = {media_server_exemplar.low_swap_condition}")
	logging.info(f"Checking Netbackup services on {media_server_exemplar.name}, status: {media_server_exemplar.check_netbackup_processes()}")
	if media_server_exemplar.low_swap_condition and not media_server_exemplar.running_backups_condition:
		logging.warning(f"Swap release attempt done for {media_server_exemplar.name}, status: {media_server_exemplar.release_swap()}")
		logging.warning(f"Checking Netbackup services for {media_server_exemplar.name}, status: {media_server_exemplar.check_netbackup_processes()}")

# nb daily log creation
if datetime.datetime.now().hour == 23:
	create_daily_log()
	compress_daily_logs_move_to_archive()
	remove_old_archives()
