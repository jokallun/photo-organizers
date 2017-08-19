from invoke import task
import json
import glacier_inventory as glacier
import archive_to_glacier as archive

CONFIG = json.load(open('config.json'))

@task
def initiate_vault_inventory(ctx, account_id=None, vault_name=None):
    glacier.initiate_vault_inventory(account_id, vault_name)

@task
def get_vault_inventory(ctx, job_file, account_id=None, vault_name=None, region=None):
    job_info = json.load(open(job_file))
    glacier.get_vault_inventory(job_info['id'], account_id, vault_name, region)

@task
def list_vaults(ctx, region_name=None):
    vaults = glacier.list_vaults(region_name)
    print(json.dumps(vaults, indent=2))

@task
def archive_dirs(ctx, rootdir):
    archive.archive_tree(rootdir)

@task
def create_vault(ctx, vault_name):
    vault = archive.create_vault(vault_name)
    print(vault)

@task
def upload_archive(ctx, archive_name, account_id=None, vault_name=None):
    archive.upload_archive(archive_name)
