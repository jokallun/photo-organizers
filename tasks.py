from invoke import task
import json
import glacier_inventory as glacier
import archive_to_glacier as archive


@task
def initiate_vault_inventory(ctx, account_id=None, vault_name=None):
    glacier.initiate_vault_inventory(ctx, account_id, vault_name)

@task
def get_vault_inventory(ctx, job_file, account_id=None, vault_name=None, region=None):
    job_info = json.load(open(job_file))
    glacier.get_vault_inventory(ctx, job_info['id'], account_id, vault_name, region)

@task
def list_vaults(ctx, region=None):
    vaults = glacier.list_vaults(ctx, region)
    print(json.dumps(vaults, indent=2))

@task
def archive_dirs(ctx, rootdir):
    archive.archive_tree(rootdir)

@task
def create_vault(ctx, vault_name):
    vault = archive.create_vault(vault_name)
    print(vault)

@task
def upload_archive(ctx, archive_name, vault_name=None, account_id=None, region=None,
                   description=None, add_checksum=True):
    archive.upload_archive(ctx, archive_name, account_id, vault_name, region, description, add_checksum)

@task
def initiate_archive_download(ctx, archive_info, vault_name=None,
                              account_id=None, region=None):
    archive.initiate_archive_download(
        ctx,
        archive_info=json.load(open(archive_info)),
        vault_name=vault_name,
        account_id=account_id,
        region=region
    )

@task
def download_archive(ctx, job_info=None, job_id=None, account_id=None,
                     vault_name=None, region=None):
    if job_info is not None:
        job_info = json.load(open(job_info))
    archive.download_archive(ctx, job_info, job_id, account_id, vault_name, region)
