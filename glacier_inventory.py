import boto3
import json
from datetime import datetime
import logging

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG = json.load(open('config.json'))

def list_vaults(region_name=CONFIG['REGION']):
    client = boto3.client('glacier', region_name)
    return client.list_vaults()

def get_vault_inventory(job_id, account_id=None, vault_name=None, region=None):
    account_id = account_id or CONFIG['ACCOUNT_ID']
    vault_name = vault_name or CONFIG['VAULT_NAME']
    region = region or CONFIG['REGION']
    glacier = boto3.resource('glacier', region_name=region)
    job = glacier.Job(account_id=account_id, vault_name=vault_name, id=job_id)
    job.reload()
    if job.completed:
        inventory_response = job.get_output()
        inventory_json = inventory_response['body'].read()
        inventory = json.loads(inventory_json)
        dt_str = datetime.today().strftime('%Y-%m-%d')
        fname = 'inventory-{}.json'.format(dt_str)
        logger.info('Writing inventory to file {}'.format(fname))
        with open(fname, 'w') as f:
            json.dump(inventory, f, indent=2)
        return inventory
    else:
        logger.info('Job {} not completed'.format(job_id))

def initiate_vault_inventory(account_id=None, vault_name=None, region=None):
    account_id = account_id or CONFIG['ACCOUNT_ID']
    vault_name = vault_name or CONFIG['VAULT_NAME']
    region = region or CONFIG['REGION']
    logger.info('Initiating inventory for {}'.format(vault_name))
    glacier = boto3.resource('glacier', region_name=region)
    print(vault_name)
    vault = glacier.Vault(account_id, vault_name)
    job = vault.initiate_inventory_retrieval()
    dt_str = datetime.today().strftime('%Y-%m-%d')
    fname = 'inventory-job-{}.json'.format(dt_str)
    with open(fname, 'w') as f:
        json.dump({'account_id': job.account_id,
                   'vault_name': job.vault_name, 'id': job.id}, f)
    logger.info('Writing vault inventory job info to {}'.format(fname))
    return job
