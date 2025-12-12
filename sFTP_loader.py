"""
sFTP Loader v5:

Manipulates sFTP buckets for school district use.  

Purposes: File Transfer over sFTP, Handling Permissions and manipulating 
directories/listing files.* 

*Main as listed for a district in mSupport = /home/ as main path in sFTP
**Type exit at any point to terminate program. 

"""

import os
import sys
import stat
import time
import csv
from contextlib import contextmanager
from typing import List, Union, Tuple
from pathlib import Path as path
from pathlib import PurePosixPath as posix_path
import paramiko as pm
import logging 

## Log setup
log_dir = path('logs')
log_dir.mkdir(parents = True, exist_ok = True)
logging.basicConfig(
    level = os.getenv(
        'LOG_LEVEL', 'INFO'), 
        format = '%(asctime)s - %(levelname)s - %(message)s'
                    )
log = logging.getLogger(__name__)


def ask(prompt: str) -> str:
    """
    Prompts the user through inputs, and if they 
    type "exit", this function will terminate the loop.
    """
    response = input(prompt).strip()
    if response.lower() == "exit":
        log.info("Goodbye!")
        sys.exit(0)
    return response

@contextmanager
def sftp_connection(host: str, port: int,
                    username: str, password: str):
    transport = pm.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = pm.SFTPClient.from_transport(transport)
    try:
        yield sftp
    finally:
        sftp.close()
        transport.close()
    

def upload_file(local_path: path, 
                remote_dir: posix_path, 
                hostname: str, 
                port: int,
                username: str, 
                password: str
                ) -> None:
    """
    The function needed to upload a file directly to sFTP
    """
    
    # Establish file and destination paths: 

    local_path = local_path.expanduser().resolve()
    
    if not local_path.exists() or not local_path.is_file(): 
        log.error(f"File path is not valid or isn’t a file: {local_path}")
        raise FileNotFoundError(f"{local_path} not found or not a file")
    else:
        log.info("Paths found and valid, starting upload.")
    
    remote_file = remote_dir / local_path.name
    remote_str = str(remote_file)
    
    # Put file into the sFTP:
    try:
        with sftp_connection(hostname, port, username, password) as sftp:
            log.info(f"Uploading {local_path} → {remote_dir}")
            sftp.put(str(local_path), str(remote_str), confirm=True)
            log.info("Upload completed successfully.")
    except(pm.PasswordRequiredException, 
            pm.AuthenticationException, 
            pm.BadHostKeyException, 
            pm.ChannelException) as e:
        log.error(f"An exception has occured! Error {e}")
        raise

def create_dir(remote_dir: posix_path, 
                hostname: str, 
                port: int,
                username: str, 
                password: str) -> None:
    """
    Creates folders in home/main directory

    """
    remote_str = remote_dir.as_posix()
    
    with sftp_connection(hostname, port, username, password) as sftp:
        try:
            sftp.mkdir(str(remote_dir))
            sftp.close()
            print(f"Folder '{remote_str}' created successfully on {hostname}")
        except Exception as e:
            print(f"Error creating folder: {e}")

def delete(remote_dir: posix_path, 
                hostname: str, 
                port: int,
                username: str, 
                password: str,
                deletion_type: str) -> None:
    """
    Function to delete files from an sFTP. 
    
    """
    
    # Initiate sFTP session: 
    
    remote_str = remote_dir.as_posix()

    with sftp_connection(hostname, port, username, password) as sftp:
        try:
            if deletion_type == "file":
                try:
                    attrs = sftp.stat(remote_str)
                except IOError:
                    if deletion_type == "file":
                        log.error(f"Remote file not found: {remote_str}")
                        raise FileNotFoundError(f"{remote_str} not found!")
                if stat.S_ISDIR(attrs.st_mode):
                    log.error(f"Remote path is a directory: {remote_str}")
                    raise IsADirectoryError(f"{remote_str} is a directory!")
                sftp.remove(remote_str)
                log.info(f"File '{remote_str}' deleted successfully.")
            
            elif deletion_type == "folder":
                sftp.rmdir(remote_str)
                log.info(f"Folder '{remote_str}' deleted successfully.")
        except(
            pm.SSHException, 
                pm.AuthenticationException, 
                ) as e:
            log.error(f"sFTP error: {e}")
            raise

def view_contents(
                remote_dir: posix_path, 
                hostname: str,
                port: int, 
                username: str,
                password: str 
                ) -> List[Tuple[str, str]]:
    """
    Returns a list of (filename, human_readable_mtime) in the remote_dir.
    """
    
    remote_str = remote_dir.as_posix()
    out = []
    
    with sftp_connection(hostname, port, username, password) as sftp:
        for name in sorted(sftp.listdir(remote_str)):
            full_path = f"{remote_str}/{name}"
            attrs = sftp.stat(full_path)
            mtime = time.ctime(attrs.st_mtime)
            out.append((name, mtime))
    return out

def output_csv(
    rows: List[Tuple[str, str]],
    output_path: path
) -> None:
    """
    Writes the contents of 'list' mode within view as a .csv file.
    """
    # Ensure parent folders exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # header
        writer.writerow(["File", "Last Modified"])
        # data rows
        writer.writerows(rows)

    log.info(f"Wrote {len(rows)} entries to {output_path}")
            
def stfp_formatter(
    hostname: str,
    port:     int,
    username: str,
    password: str,
    remote_dir: Union[str, posix_path]
                ) -> None:
    """
    Connect to the SFTP server and print a GitHub‐style tree of remote_dir.

    Used only with the view_contents() function exclusively.
    """
    # Convert to a forward‐slash string
    root = str(remote_dir).rstrip("/")

    # Establish transport & client
    with sftp_connection(hostname, port, username, password) as sftp:
        try:
            print(root + "/")
            # inner recursive helper
            def _walk(path: str, prefix: str = ""):
                entries = sorted(sftp.listdir(path))
                for idx, name in enumerate(entries):
                    full  = f"{path.rstrip('/')}/{name}"
                    is_dir = stat.S_ISDIR(sftp.stat(full).st_mode)
                    branch = "└── " if idx == len(entries)-1 else "├── "
                    suffix  = "/" if is_dir else ""
                    print(f"{prefix}{branch}{name}{suffix}")
                    if is_dir:
                        extension = "    " if idx == len(entries) - 1 else "│   "
                        _walk(full, prefix + extension)
            _walk(root)
        finally:
            if sftp:
                sftp.close()

    
def main():
    """
    Provides execution order for embedded functions: 

    Upload, Delete, View Contents (list, tree)

    Includes logic to restart loop and provide new creds if needed. 
    """
    
    # Inputs for function execution

    print(r"""
******************************************************************************************
 
                                ┏┓┏┓┏┳┓┏┓  ┓      ┓    
                                ┗┓┣  ┃ ┃┃  ┃ ┏┓┏┓┏┫┏┓┏┓
                                ┗┛┻  ┻ ┣┛  ┗┛┗┛┗┻┗┻┗ ┛ 
                
                                        v.5
                                sFTP File Manipulator
                                Author: Bruce A. Lee 
                    Type exit at any point to terminate this program.
          
******************************************************************************************
""")

    host = ask("Host: ").strip()
    username = ask("Username: ")
    password = ask("Password: ").strip()
    port = 22
    
    # Input for selecting to view or upload files
    while True:
        choice = ask(
            "Type 'upload' to send a file, " \
            "'folder' to make a new folder, " \
            "'delete' to remove either or 'view' "  
            " to list all files within the server: ").strip().lower()       
        
        if choice == "upload":
            src = path(ask("Local file to upload: "))
            dst_dir = posix_path(ask("Remote directory: ").strip())
            try:
                upload_file(src, dst_dir, host, port, username, password)
            except Exception as e:
                log.error(f"Upload failed: {e}")
                sys.exit(1)

        elif choice == "folder":
            new_dir = posix_path(ask("New remote directory: ").strip())
            try:
                create_dir(new_dir, host, port, username, password)
            except Exception as e:
                log.error(f"Folder creation failed: {e}")
                sys.exit(1)
        
        elif choice == "delete":
            deletion_type = input("File or Folder? ")
            dst_dir = posix_path(ask("Remote directory: ").strip())
            try: 
                delete(dst_dir, host, port, username, password, deletion_type)
            except Exception as e:
                log.error(f"Deletion failed: {e}")
                sys.exit(1)

        elif choice == "view":
            remote_dir = posix_path(ask("Remote directory to list: ").strip())
            view_mode = ask(
                "Type 'list' for flat listing, 'csv' for the lists' contents as a .csv, or 'tree' for full directory tree: ") \
                            .strip().lower()
            if view_mode == "list":
                try:
                    files = view_contents(remote_dir, host, port, username, password)
                    max_name = max(len(name) for name, _ in files + [("File", "")])
                    header = f"{'File'.ljust(max_name)}   Last Modified"
                    sep    = "-" * len(header)
                    print(header)
                    print(sep)
                    for name, mtime in files:
                        print(f"{name.ljust(max_name)}   {mtime}")
                except Exception as e:
                    log.error(f"Failed to list directory: {e}")
                    sys.exit(1)
            elif view_mode == "tree":
                try:
                    stfp_formatter(host, port, username, password, remote_dir)
                except Exception as e:
                    log.error(f"Failed to print tree: {e}")
                    sys.exit(1)
            elif view_mode == "csv":
                files = view_contents(remote_dir, host, port, username, password) 
                out_file = path(ask("Output csv file path: "))
                try:
                    output_csv(files, out_file)
                except Exception as e:
                    log.error(f"Failed to write csv: {e}")
                    sys.exit(1)
            else:
                log.error("Invalid view mode—please enter 'list', 'tree', or 'csv'.")
                sys.exit(1)
        else:
            log.error(f"Invalid choice {choice!r}; please enter 'upload', 'delete', 'folder' or 'view'.")
            sys.exit(1)
        
        # Restart loop logic
        restart = ask("Continue? (yes/no): ").strip().lower()
        if restart == "no":
            log.info("Goodbye!")
            break
        same_creds = ask("Proceed with the same sFTP credentials? (yes/no): ").strip().lower()
        if same_creds == "yes":
            continue
        elif same_creds == "no":
            log.info("Please reenter sFTP credentials:")
            host = ask("Host: ").strip()
            username = ask("Username: ")
            password = ask("Password: ").strip()
            continue
        else:   
            log.error("Invalid answer-please enter 'yes', or 'no'.")
            continue

if __name__ == "__main__":
    main()
        