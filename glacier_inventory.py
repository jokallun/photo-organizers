import json
from invoke import task
from datetime import datetime
import logging
from aws_contexts import GlacierCtx, VaultCtx, JobCtx


logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


@task
def list_vaults(ctx, region=None):
    """List vaults in given region

    region argument is optional and defaults to value
    in photo-organizer.json
    """
    with GlacierCtx(ctx, region) as glacier:
        gclient = glacier.meta.client
        vaults = gclient.list_vaults()
        print(json.dumps(vaults, indent=2))


@task
def get_vault_inventory(ctx, job_file, account_id=None, vault_name=None, region=None):
    """Download the vault inventory.

    This can be run after the initiate-vault-inventory task
    has ran.

    job-file is the output of initiate-vault-inventory task,
    other argments are optional and default to values in photo-organizer.json
    """
    job_info = json.load(open(job_file))
    job_info['ctx'] = ctx
    with JobCtx(**job_info) as job:
        job.reload()
        if job.completed:
            inventory_response = job.get_output()
            inventory_json = inventory_response['body'].read()
            inventory = json.loads(inventory_json)
            dt_str = datetime.today().strftime('%Y%m%d-%H%M')
            fname = 'inventory-{}.json'.format(dt_str)
            logger.info('Writing inventory to file {}'.format(fname))
            with open(fname, 'w') as f:
                json.dump(inventory, f, indent=2)
            return inventory
        else:
            logger.info('Job {} not completed'.format(job.id))


@task
def initiate_vault_inventory(ctx, account_id=None, vault_name=None, region=None):
    """Initiate the vault inventory job.

    After the job has completed (3-5h), the inventory
    can be downloaded with get-vault-inventory task.
    Output file of this task is the input of get-vault-inventory

    All argments are optional and default to values in photo-organizer.json
    """
    with VaultCtx(ctx, vault_name, account_id, region) as vault:
        job = vault.initiate_inventory_retrieval()
        dt_str = datetime.today().strftime('%Y%m%d-%H%M')
        fname = 'inventory-job-{}.json'.format(dt_str)
        with open(fname, 'w') as f:
            json.dump(
                {'account_id': job.account_id, 'vault_name': job.vault_name, 'id': job.id},
                f,
                indent=2,
            )
            logger.info('Writing vault inventory job info to {}'.format(fname))
            return job


@task
def delete_archives(ctx, file_list, account_id=None, vault_name=None, region=None):
    """Delete archives listed in file_list

    file-list is an json file returned by get-vault-inventory
    or with similar structure

    Other  argments are optional and default to values in photo-organizer.json
    """
    archive_infos = json.load(open(file_list))
    with VaultCtx(ctx, vault_name, account_id, region) as vault:
        for n, info in enumerate(archive_infos.get('ArchiveList')):
            archive = vault.Archive(info['ArchiveId'])
            archive.delete()
            logger.info('deleted {}'.format(info['ArchiveId']))
            if n % 100 == 0:
                logger.info('Deleted {} archives'.format())
