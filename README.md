# Qumulo Python Wrapper

A Python-based wrapper for interacting with Qumulo storage systems to manage snapshots and generate change lists.

## Prerequisites

- Python 3.6 or higher
- Qumulo Command Line Tool (`qq`) installed and accessible from PATH
- Access to a Qumulo cluster
- Proper configuration in DNAClientServices file

## Installation

1. Clone the repository
2. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The wrapper requires configuration settings in one of these locations:

- Linux: `/etc/StorageDNA/DNAClientServices.conf`
- macOS: `/Library/Preferences/com.storagedna.DNAClientServices.plist`

Required configuration parameters:
- QumuloClusterIP
- QumuloClusterPort
- QumuloUsername
- QumuloPassword

### Example Linux Configuration
```ini
[General]
QumuloClusterIP=192.168.1.100
QumuloClusterPort=8000
QumuloUsername=admin
QumuloPassword=password
FastScanWorkFolder=/tmp/fastscan
```

## Usage

### Create Snapshot
Creates a new snapshot of a specified path on the Qumulo cluster.

```bash
python qumulo_create_snap.py -p <project_name> -s <qumulo_source_path>
```

Parameters:
- `-p, --projectname`: Name of the project
- `-s, --qumulosourcepath`: Source path on Qumulo to snapshot

Output:
- Prints the snapshot ID on success
- Exits with error code on failure

### Generate Change List
Generates an XML file containing changes between two snapshots.

```bash
python qumulo_generate_changelist.py \
    -p <project_name> \
    -b <base_path> \
    -m <mapped_path> \
    -g <project_guid> \
    -i <source_index> \
    --prevsnapshotid <prev_snapshot_id> \
    --newsnapshotid <new_snapshot_id> \
    [--deletes]
```

Parameters:
- `-p, --projectname`: Project name
- `-b, --basepath`: Base path to compare
- `-m, --mappedpath`: Mapped path for comparison
- `-g, --projectguid`: Project GUID
- `-i, --sourceindex`: Numeric index of source folders
- `--prevsnapshotid`: Previous snapshot ID to compare
- `--newsnapshotid`: New snapshot ID to compare
- `--deletes`: Optional flag to include deleted files

Output:
- Generates an XML file at `<FastScanWorkFolder>/sdna-scan-files/<project_guid>/<source_index>-files.xml`
- Contains file changes, additions, and deletions between snapshots

## Error Codes

- 0: Success
- 1: Walkthrough error
- 4: Output folder creation error
- 23: Missing Qumulo configuration
- Other: Various Qumulo command execution errors

## File Structure

- `qumulo_create_snap.py`: Handles creation of new snapshots
- `qumulo_generate_changelist.py`: Generates change lists between snapshots

## Examples

Create a new snapshot:
```bash
python qumulo_create_snap.py -p MyProject -s /path/to/source
```

Generate change list:
```bash
python qumulo_generate_changelist.py \
    -p MyProject \
    -b /original/path \
    -m /mapped/path \
    -g project-guid-123 \
    -i 1 \
    --prevsnapshotid 100 \
    --newsnapshotid 101 \
    --deletes
```