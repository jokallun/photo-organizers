from invoke import task
import json
import glacier_inventory as glacier


@task
def initiate_vault_inventory(ctx, account_id=glacier.ACCOUNT_ID, vault_name=glacier.VAULT_NAME):
    glacier.initiate_vault_inventory(account_id, vault_name)

@task
def get_vault_inventory(ctx, job_file, account_id=None, vault_name=None, region=None):
    job_info = json.load(open(job_file))
    glacier.get_vault_inventory(job_info['id'], account_id, vault_name, region)

@task
def list_vaults(ctx, region_name=glacier.REGION):
    vaults = glacier.list_vaults(region_name)
    print(json.dumps(vaults, indent=2))
