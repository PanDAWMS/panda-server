import re
import requests
import json
import socket
import subprocess

def get_repo_info() -> object:
    # Get the current remote URL of the repository
    repo_url = subprocess.check_output(['git', 'config', '--get', 'remote.origin.url']).strip().decode()

    # Get the repo and branch name
    match = re.match(r'https://github.com/(.*).git@(.*)', repo_url)

    if match:
        repo_name = match.group(1)
        branch_name = match.group(2)
    else:
        repo_name = repo_url.rstrip('.git')
        branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode()

    # Commit hash
    commit_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode()

    return repo_name, branch_name, commit_hash


def mm_communication_script(repo_name, branch_name, commit_hash):
    #Get Server Name
    server_name = socket.gethostname()

    # TODO: decide on the best path for the hook URL
    file_path = os.path.expanduser('~/mm_webhook_url.txt')
    with open(file_path, 'r') as file:
        mm_webhook_url = file.read().strip()
        if not mm_webhook_url:
            return

    mm_message = {
        "text": f"Install Information:\n - Server Name: {server_name} \n - Commit: {commit_hash}\n - Branch: {branch_name}\n - Package: {repo_name} \n "
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(mm_webhook_url, data=json.dumps(mm_message), headers=headers)
        if response.status_code == 200:
            print("Message sent successfully to Mattermost")
        else:
            print(f"Failed to send message: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")