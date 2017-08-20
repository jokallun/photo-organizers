import boto3
import os
import json
from datetime import datetime
from zipfile import ZipFile, ZIP_STORED
import logging
import hashlib


logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

BLOCKSIZE = 65536

def get_checksum(fname):
    hasher = hashlib.md5()
    with open(fname, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

def leaf_dirs(rootdir):
    for root, dirs, fnames in os.walk(rootdir):
        if len(dirs) == 0:
            yield root, fnames

def create_vault(vault_name):
    glacier = boto3.resource('glacier')
    vault = glacier.create_vault(vaultName=vault_name)
    return vault

def upload_archive(ctx, archive_name, vault_name=None, account_id=None, region=None, description=None, add_checksum=True):
    account_id = account_id or ctx.config.glacier.account_id
    vault_name = vault_name or ctx.config.glacier.vault_name
    region = region or ctx.config.glacier.region
    logger.info('Uploading archive {} to vault {}'.format(archive_name, vault_name))
    glacier = boto3.resource('glacier', region_name=region)
    vault = glacier.Vault(account_id, vault_name)
    description = description or archive_name.split('/')[-1]
    if add_checksum:
        checksum = get_checksum(archive_name)
        description = '{} {}'.format(description, checksum)
    archive = vault.upload_archive(
        archiveDescription=description,
        body=open(archive_name, 'rb')
    )
    splitted = archive_name.split('/')
    outdir = '/'.join(splitted[:-1]) + '/glacier-meta'
    info = {
        'account_id': archive.account_id,
        'vault_name': archive.vault_name,
        'archive_name': splitted[-1],
        'id': archive.id,
        'description': description,
        'datetime': datetime.today().strftime('%Y-%m-%d:%H:%M:%S')
    }
    if not os.path.isdir(outdir):
        logger.info('creating {}'.format(outdir))
        os.makedirs(outdir)
    with open('{}/glacier-archive-{}.json'.format(outdir, splitted[-1]), 'w') as f:
        json.dump(info, f)
    return archive

def initiate_archive_download(ctx, archive_info, vault_name=None, account_id=None, region=None):
    account_id = account_id or ctx.config.glacier.account_id
    vault_name = vault_name or ctx.config.glacier.vault_name
    region = region or ctx.config.glacier.region
    logger.info('Initiating archive {} download'.format(archive_info['id']))
    glacier = boto3.resource('glacier', region_name=region)
    vault = glacier.Vault(account_id, vault_name)
    archive = vault.Archive(archive_info['id'])
    job = archive.initiate_archive_retrieval()
    dt_str = datetime.today().strftime('%Y-%m-%d')
    fname = 'archive-download-job-{}.json'.format(dt_str)
    with open(fname, 'w') as f:
        json.dump({
            'account_id': job.account_id,
            'vault_name': job.vault_name,
            'id': job.id
        }, f)
    logger.info('Writing archive download job info to {}'.format(fname))
    return job

def download_archive(ctx, job_info=None, job_id=None, account_id=None,
                     vault_name=None, region=None):
    account_id = account_id or ctx.config.glacier.account_id
    vault_name = vault_name or ctx.config.glacier.vault_name
    region = region or ctx.config.glacier.region
    if job_id is None and job_info is None:
        logger.error('Must give either job_id or job_info!')
        return None
    glacier = boto3.resource('glacier', region_name=region)
    if job_info is not None:
        job = glacier.Job(**job_info)
    else:
        job = glacier.Job(account_id=account_id, vault_name=vault_name, id=job_id)
    job.reload()
    if job.completed:
        download_response = job.get_output()
        checksum = download_response['checksum']
        body = download_response['body']
        description = download_response['archiveDescription']
        fname = description.split(' ')[0]
        logger.info('Writing downloaded archive to file {}'.format(fname))
        with open(fname, 'wb') as f:
            f.write(body.read())
    else:
        logger.info('Job {} not completed'.format(job_id))

def create_archive(path, fnames):
    current_dir = os.getcwd()
    archive_name = '-'.join(path.split('/')[-2:]) + '.zip'
    zip_root_dir = '/'.join(path.split('/')[:-2])
    os.chdir(zip_root_dir)
    logger.info('Creating archive {}/{}'.format(zip_root_dir, archive_name))
    with ZipFile(archive_name, 'w', ZIP_STORED) as f:
        for input_fname in fnames:
            relative_path = '/'.join(path.split('/')[-2:])
            f.write('{}/{}'.format(relative_path, input_fname))
    os.chdir(current_dir)

def archive_tree(rootdir):
    logger.info('Creating archives from rootdir {}'.format(rootdir))
    for path, fnames in leaf_dirs(rootdir):
        create_archive(path, fnames)
