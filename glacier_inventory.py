import boto3
import json
from datetime import datetime
import argparse
import logging

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

VAULT_NAME = 'new-photos'
ACCOUNT_ID = '-'
REGION = 'eu-west-1'

def list_vaults(region_name=REGION):
    client = boto3.client('glacier', region_name)
    return client.list_vaults()

def get_vault_inventory(job_id, account_id=None, vault_name=None, region=None):
    account_id = account_id or ACCOUNT_ID
    vault_name = vault_name or VAULT_NAME
    region = region or REGION
    glacier = boto3.resource('glacier', region_name=region)
    job = glacier.Job(account_id=account_id, vault_name=vault_name, id=job_id)
    job.reload()
    if job.completed:
        inventory_response = job.get_output()
        inventory_json = inventory_response['body'].read()
        dt_str = datetime.today().strftime('%Y-%m-%d')
        fname = 'inventory-{}.json'.format(dt_str)
        with open(fname, 'w') as f:
            f.write(inventory_json)
        logger('Writing inventory to file {}'.format(fname))
        return json.loads(inventory_json)
    else:
        logger.info('Job {} not completed'.format(job_id))

def initiate_vault_inventory(account_id=ACCOUNT_ID, vault_name=VAULT_NAME):
    logger.info('Initiating inventory for {}'.format(vault_name))
    glacier = boto3.resource('glacier', region_name=REGION)
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rename photos')
    parser.add_argument('-v', '--vault_name',
                        help='name of aws galcier vault',
                        default=VAULT_NAME)
    parser.add_argument('-i', '--initiate',
                        help='initiate inventory job',
                        action='store_true')
    args = parser.parse_args()
    if args.initiate:
        initiate_vault_inventory(vault_name=args.vault_name)
