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
import stat

QUMULO_CLUSTER_IP = "QumuloClusterIP"
QUMULO_CLUSTER_PORT = "QumuloClusterPort"
QUMULO_USERNAME = "QumuloUsername"
QUMULO_PASSWORD = "QumuloPassword"

global_path_list = []
global_data_map = { }
global_file_counts = { }
global_walkhrough_error = False

def escape( str ):
    str = str.replace("&", "&amp;")
    str = str.replace("<", "&lt;")
    str = str.replace(">", "&gt;")
    str = str.replace("\"", "&quot;")
    return str

def walktree(top, base_path, mapped_path):
    
    local_path = top
    if top.startswith(base_path):
        local_path = top.replace(base_path, mapped_path)

    for root, dirs, files in os.walk(local_path):
        for file in files:
            pathname = os.path.join(root, file)
            try:
                rel_path = pathname[len(mapped_path):]
                file_map = get_stat_file_obj(pathname, rel_path, "CREATE", base_path, mapped_path)
                global_data_map[rel_path] = file_map
                global_path_list.append(rel_path)
                if file_map["type"] == 'file':
                     global_file_counts["total_size"] = global_file_counts["total_size"] + file_map["size"]
                     global_file_counts["total_count"] =  global_file_counts["total_count"] + 1

            except OSError:
                file_map = {}
                file_map["type"] = 'dir'
                if os.path.isfile(pathname):
                    file_map["type"] = 'file'
                file_map["action"] = "BADDIR"
                file_map["entry"] = path
                global_file_counts["bad_dir_count"] = global_file_counts["bad_dir_count"] + 1


def get_stat_file_obj(given_path, rel_path, action, base, mapped):
    
    file_map = { }

    try:
        if given_path.startswith(base):
            given_path = given_path.replace(base, mapped)
        stat_info = os.lstat(given_path)
        if stat.S_ISDIR(stat_info.st_mode):
            file_map["type"] = 'dir'
        elif stat.S_ISREG(stat_info.st_mode):
            file_map["type"] = 'file'
        else:
            return file_map

        file_map["uid"] = stat_info.st_uid
        file_map["gid"] = stat_info.st_gid
        file_map["action"] = action
        file_map["mtime"] = stat_info.st_mtime
        file_map["atime"] = stat_info.st_atime
        file_map["size"] = stat_info.st_size
        file_map["mode"] = stat_info.st_mode
        file_map["path"] = rel_path

    except OSError:
            
        file_map = {}
        file_map["type"] = 'dir'
        if os.isfile(pathname):
            file_map["type"] = 'file'
            
        file_map["action"] = "BADDIR"
        file_map["entry"] = given_pathh
        global_file_counts["bad_dir_count"] = global_file_counts["bad_dir_count"] +  1
        file_map["action"] = "baddir"
        file_map["path"] = given_path
       
        global_walkhrough_error = True 

    return file_map

def generate_deleted_file_map(rel_path, full_path):
    del_file_map = {}
    del_file_map["action"] = "DELETE"
    if full_path.endswith("/"):
         del_file_map["type"] = 'dir'
    else:
         del_file_map["type"] = 'file'

    del_file_map["rel_path"] = rel_path
    del_file_map["size"] = 0
    del_file_map["uid"] = 0
    del_file_map["gid"] = 0
    del_file_map["mtime"] = int(time.time())
    del_file_map["atime"] = int(time.time())
    del_file_map["mode"] = "0x0"
    del_file_map["path"] = full_path
    return del_file_map

def get_file_maps(json_string, index,  deletes_on,  base_path, mapped_path):

    total_size = 0
    total_count = 0
    delete_count = 0

    new_content = "[" + json_string.replace("}\n{", "},\n{") + "]"
    changes_list = json.loads(new_content)

    for change_list in changes_list:
        entries = change_list["entries"]
        for entry in entries:
             path = entry["path"]
             rel_path = path[len(base_path):]
             action = entry["op"]
             if rel_path in global_data_map:
                 prev_data = global_data_map[rel_path]
                 if prev_data["type"] == "file":
                      if prev_data["action"] == "DELETE":
                          delete_count = delete_count - 1
                      else:
                          total_count = total_count - 1
                          total_size = total_size - prev_data["size"]

             if action == "DELETE":
                  if deletes_on == True:
                      file_map = generate_deleted_file_map(rel_path, path)
                      global_data_map[rel_path] = file_map
                      global_path_list.append(rel_path)
                  if deletes_on == True and not path.endswith("/"):
                      delete_count = delete_count + 1
             else:
                  file_map = get_stat_file_obj(entry["path"], rel_path, entry["op"], base_path, mapped_path)
                  if file_map["type"] == 'dir' and action == 'CREATE':
                      walktree(path, base_path, mapped_path)
                  else:
                      if file_map["type"] == 'file':
                          total_size = total_size +  file_map['size']
                          total_count = total_count + 1
                      global_data_map[rel_path] = file_map
                      global_path_list.append(rel_path)
                      
    global_file_counts["total_size"] = global_file_counts["total_size"] + total_size
    global_file_counts["total_count"] = global_file_counts["total_count"] + total_count
    global_file_counts["delete_count"] = global_file_counts["delete_count"] + delete_count

    return True


def write_xml_result(xml_file, index):

    total_count = global_file_counts["total_count"]
    total_size = global_file_counts["total_size"]
    delete_count = global_file_counts["delete_count"]
    bad_dir_count = global_file_counts["bad_dir_count"]

    xml_file.write("<files scanned=\"" + str(total_count) + "\" selected=\"" + str(total_count) + "\" size=\"" + str(total_size) + "\" bad_dir_count=\"" + str(bad_dir_count)+ "\" delete_count=\"" + str(delete_count) + "\">\n");
 
    global_path_list.sort()

    for list_entry in reversed(global_path_list):

        entry = global_data_map[list_entry]
        if entry['type'] == 'file' and (entry['action'] == 'CREATE' or entry['action'] == 'MODIFY'):
                 xml_file.write("    <file name=\"" + escape(entry['path']) + "\" size=\"" + str(entry['size'])  + "\" mode=\"0x777\"  type=\"F_REG\" mtime=\"" + str(int(entry['mtime'])) + "\" atime=\"" + str(int(entry['atime'])) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")
          
        elif entry['type'] == 'dir':
                 dir_path = entry['path']
                 xml_file.write("    <file name=\"" + escape(dir_path[:-1]) + "\" size=\"" + str(entry['size'])  + "\" mode=\"0x777\"  type=\"F_DIR\" mtime=\"" + str(int(entry['mtime'])) + "\" atime=\"" + str(int(entry['atime'])) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")
        elif entry['action'] == 'DELETE':
            xml_file.write("    <delete-file name=\"" + escape(entry['path']) + "\" size=\"" + str(entry['size']) + "\" mode=\"0x777\"  type=\"F_REG\" mtime=\"" + str(int(entry['mtime'])) + "\" atime=\"" + str(int(entry['atime'])) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")
        elif entry['action'] == 'BADDIR':
            xml_file.write("    <bad-dir name=\"" + escape(entry['path']) + "\" size=\"" + str(entry['size']) + "\" mode=\"0x777\"  type=\"F_REG\" mtime=\"" + str(entry['mtime']) + "\" atime=\"" + str(entry['atime']) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")

    xml_file.write("</files>\n")
    xml_file.close()

def get_scan_folder_output_folder(project_name, project_guid):

    is_linux=0
    if platform.system() == "Linux":
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
        is_linux=1
    elif platform.system() == "Darwin":
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    fastScanWorkFolder = ""

    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','FastScanWorkFolder'):
            section_info = config_parser['General']
            fastScanWorkFolder = section_info['FastScanWorkFolder']
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            fastScanWorkFolder  = my_plist["FastScanWorkFolder"]
    
    if (len(fastScanWorkFolder) == 0):
        fastScanWorkFolder = "/tmp"
     
    fastScanWorkFile = fastScanWorkFolder + '/sdna-scan-files/' + project_guid
    return fastScanWorkFile


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
        args.append(str(params[x]))
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
    parser.add_argument('-b', '--basepath', required = True, help = 'Project guid we are performing scan for.')
    parser.add_argument('-m', '--mappedpath', required = True, help = 'Project guid we are performing scan for.')
    parser.add_argument('-g', '--projectguid', required = True, help = 'Project guid we are performing scan for.')
    parser.add_argument('-i', '--sourceindex', required = True, help = 'Numeric index of source folders')
    parser.add_argument('--deletes', required = False, action='store_true', help = 'Required prev snapshot')
    parser.add_argument('--prevsnapshotid', required = True, help = 'Required prev snapshot')
    parser.add_argument('--newsnapshotid', required = True, help = 'Required new snapshot')

    args = parser.parse_args()

    config_dict = get_qumulo_config_dictionary()
    params_dict = {"-u":config_dict[QUMULO_USERNAME],"--p":config_dict[QUMULO_PASSWORD]}
    output_dict = {}

    if run_qumulo_qq_process("login", config_dict, params_dict, output_dict) == False:
        print("Unable to login " + output["result"])
        exit(output_dict["exitcode"])

    output_folder = get_scan_folder_output_folder(args.projectname, args.projectguid)
    if not os.path.isdir(output_folder):
        os.makedirs(output_folder, exist_ok=True)
    if not os.path.isdir(output_folder):
        pathlib.Path(output_folder).mkdir(parents=True, exist_ok=True)
        print("Unable to create output folder")
        exit(4)

    prev_snapshot_id = int(args.prevsnapshotid)
    new_snapshot_id = int(args.newsnapshotid)

    params_dict = {"--older-snapshot":prev_snapshot_id,"--newer-snapshot":new_snapshot_id}
    output_dict = {}

    if run_qumulo_qq_process("snapshot_diff", config_dict, params_dict, output_dict) == False:
        print("Unable to generate snapshot difference" + output_dict["result"])
        exit(output_dict["exitcode"])
    
    qumulo_output = output_dict["result"]

    global_file_counts["total_size"] = 0
    global_file_counts["total_count"] = 0
    global_file_counts["delete_count"] = 0
    global_file_counts["bad_dir_count"] = 0
   
    get_file_maps(qumulo_output, args.sourceindex, args.deletes, args.basepath, args.mappedpath)

    try:
        output_file = output_folder + "/" + str(args.sourceindex) + "-files.xml"
        xml_file = open(output_file, "w") 
        write_xml_result(xml_file, args.sourceindex)

    except OSError:
        print("Could not open/read file:" + xml_filename)
        sys.exit(4)

    if global_walkhrough_error == True:
        exit(1)
    else:
        exit(0)

