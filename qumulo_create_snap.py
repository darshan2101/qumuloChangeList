from pprint import pprint
import os
import requests
import subprocess
import json
import platform
import time
import sys
import time    
import argparse
from configparser import ConfigParser
import plistlib

QUMULO_CLUSTER_IP = "QumuloClusterIP"
QUMULO_CLUSTER_PORT = "QumuloClusterPort"
QUMULO_USERNAME = "QumuloUsername"
QUMULO_PASSWORD = "QumuloPassword"

def get_qumulo_config_dictionary():

    is_linux=0
    if platform.system() == "Linux":
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
        is_linux=1
    elif platform.system() == "Darwin":
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    config_dictionary = {}

    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General'):
            section_info = config_parser['General']
            if config_parser.has_option('General',QUMULO_CLUSTER_IP):
                config_dictionary[QUMULO_CLUSTER_IP] = section_info[QUMULO_CLUSTER_IP]
            if config_parser.has_option('General',QUMULO_CLUSTER_PORT):
                config_dictionary[QUMULO_CLUSTER_PORT] = section_info[QUMULO_CLUSTER_PORT]
            if config_parser.has_option('General',QUMULO_USERNAME):
                config_dictionary[QUMULO_USERNAME] = section_info[QUMULO_USERNAME]
            if config_parser.has_option('General', QUMULO_PASSWORD):
                config_dictionary[QUMULO_PASSWORD] = section_info[QUMULO_PASSWORD]
 
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            config_dictionary[QUMULO_CLUSTER_IP] = my_plist[QUMULO_CLUSTER_IP]
            config_dictionary[QUMULO_CLUSTER_PORT] = my_plist[QUMULO_CLUSTER_PORT]
            config_dictionary[QUMULO_USERNAME] = my_plist[QUMULO_USERNAME]
            config_dictionary[QUMULO_PASSWORD] = my_plist[QUMULO_PASSWORD]

    if len(config_dictionary[QUMULO_CLUSTER_IP]) == 0 or len(config_dictionary[QUMULO_CLUSTER_PORT]) == 0 or len(config_dictionary[QUMULO_USERNAME]) == 0 or len(config_dictionary[QUMULO_PASSWORD]) == 0:
        print("Missing qumulo settings, please check DNAClientServices for " + " " + QUMULO_CLUSTER_IP + " " + QUMULO_CLUSTER_PORT + " " + QUMULO_USERNAME + " " + QUMULO_PASSWORD)
        sys.exit(23)

    return config_dictionary

def run_qumulo_qq_process(action, config, params, output):

    # Build up parameter list
    args = ["qq"]
    args.append("--host")
    args.append(config[QUMULO_CLUSTER_IP])
    args.append("--port")
    args.append(config[QUMULO_CLUSTER_PORT])
    args.append(action)
    for x in params:
        args.append(x)
        args.append(params[x])
    p = subprocess.run(args, capture_output=True )
    output["exitcode"]  = p.returncode
    if p.returncode != 0:
         output["result"] = p.stderr.decode()
         return False
    else:
         output["result"] = p.stdout.decode()
         return True

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--projectname', required = True, help = 'Project we are performing scan for.')
    parser.add_argument('-s', '--qumulosourcepath', required = True, help = 'Source path to scan')
    args = parser.parse_args()

    config_dict = get_qumulo_config_dictionary()
    params_dict = {"-u":config_dict[QUMULO_USERNAME],"--p":config_dict[QUMULO_PASSWORD]}
    output_dict = {}

    if run_qumulo_qq_process("login", config_dict, params_dict, output_dict) == False:
        print("Unable to login " + output["result"])
        exit(output_dict["exitcode"])
        
    epoch_time = int(time.time())
    new_snapshot_name = args.projectname + "-" + str(epoch_time)

   ### Get Jobs List
    new_snapshot_path = args.qumulosourcepath

    # Now create the new snapshot
    
    params_dict = {"--name":new_snapshot_name,"--path":new_snapshot_path}
    output_dict = {}

    if run_qumulo_qq_process("snapshot_create_snapshot", config_dict, params_dict, output_dict) == False:
        print("Unable to login " + output_dict["result"])
        exit(output_dict["exitcode"])

    json_response = json.loads(output_dict["result"])
    new_snapshot_id = json_response["id"]

    print(new_snapshot_id)
    exit(0)

