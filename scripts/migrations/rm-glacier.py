import json, subprocess

input_file_name = 'inventory.json'
vault_name = 'scraper-place-prod'

with open(input_file_name, 'r') as f:
    inv = json.load(f)
archive_list = inv['ArchiveList']

for i, archive in enumerate(archive_list):
    if i % 1000 == 0:
        print(i)
    command = "aws glacier delete-archive --archive-id='" + archive['ArchiveId'] + "' --vault-name " + vault_name + " --account-id -"
    subprocess.run(command, shell=True, check=True)
