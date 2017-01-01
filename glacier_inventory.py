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

def list_vaults():
    client = boto3.client('glacier', region_name=REGION)
    return client.list_vaults()

def get_vault_inventory(job_id, account_id=ACCOUNT_ID, vault_name=VAULT_NAME):
    glacier = boto3.resource('glacier', region_name=REGION)
    job = glacier.Job(account_id=account_id, vault_name=vault_name, id=job_id)
    job.reload()
    if job.completed:
        inventory_response = job.get_output()
        inventory_json = inventory_response['body'].read()
        dt_str = datetime.today().strftime('%Y-%m-%d')
        with open('inventory-{}.json'.format(dt_str), 'w') as f:
            f.write(inventory_json)
        return json.loads(inventory_json)

def initiate_vault_inventory(account_id=ACCOUNT_ID, vault_name=VAULT_NAME):
    logger.info('Initiating inventory for {}'.format(vault_name))
    glacier = boto3.resource('glacier', region_name=REGION)
    vault = glacier.Vault(account_id, vault_name)
    job = vault.initiate_inventory_retrieval()
    dt_str = datetime.today().strftime('%Y-%m-%d')
    with open('inventory-job-{}.json'.format(dt_str), 'w') as f:
        json.dump({'account_id': job.account_id,
                   'vault_name': job.vault_name, 'id': job.id}, f)
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
