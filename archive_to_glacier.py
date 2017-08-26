import boto3
import os
import json
from datetime import datetime
from zipfile import ZipFile, ZIP_STORED
from invoke import task
import logging
import hashlib
from aws_contexts import GlacierCtx, VaultCtx, JobCtx


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

@task
def create_vault(ctx, vault_name):
    """Create a new vault.
    """
    with GlacierCtx(ctx) as glacier:
        vault = glacier.create_vault(vaultName=vault_name)
        print(vault)

@task
def upload_archive(ctx, archive_name, vault_name=None, account_id=None,
                   region=None, description=None, add_checksum=True):
    """Upload an archive (file) to AWS glacier.
    archive-name is a mandatory argument, others are optional.
    vault-name, account-id and region default to values defined in photo-organizer.json file.
    description defaults to name of file + md5 checksum
    """
    with VaultCtx(ctx, vault_name, account_id, region) as vault:
        logger.info('Uploading archive {} to vault {}'.format(archive_name, vault.name))
        description = description or archive_name.split('/')[-1]
        if add_checksum:
            checksum = get_checksum(archive_name)
            description = '{} {}'.format(description, checksum)
        archive = vault.upload_archive(
            archiveDescription=description,
            body=open(archive_name, 'rb')
        )
        write_archive_info(archive_name, archive, description)

def write_archive_info(archive_name, archive, description):
    splitted = archive_name.split('/')
    if len(splitted) > 1:
        outdir = '/'.join(splitted[:-1]) + '/glacier-meta'
    else:
        outdir ='./glacier-meta'
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
        json.dump(info, f, indent=2)

@task
def initiate_archive_download(ctx, archive_info, vault_name=None, account_id=None, region=None):
    """Initiate the archive donwload job.

    After the job has completed (3-5h), the archive
    can be downloaded with download-archive task.
    Output file of this task is the input of download-archive

    archive-info is a json file containing the id of the archive.
    Other argments are optional and default to values in photo-organizer.json
    """
    with VaultCtx(ctx, vault_name, account_id, region) as vault:
        archive_info = json.load(open(archive_info)),
        logger.info('Initiating archive {} download'.format(archive_info['id']))
        archive = vault.Archive(archive_info['id'])
        job = archive.initiate_archive_retrieval()
        dt_str = datetime.today().strftime('%Y-%m-%d')
        fname = 'archive-download-job-{}.json'.format(dt_str)
        with open(fname, 'w') as f:
            json.dump({
                'account_id': job.account_id,
                'vault_name': job.vault_name,
                'id': job.id
            }, f, indent=2)
        logger.info('Writing archive download job info to {}'.format(fname))

@task
def download_archive(ctx, job_file=None, job_id=None, account_id=None,
                     vault_name=None, region=None):
    """Download an archive.

    This can be run after the initiate-archive-download task
    has ran.

    job-file is the output of initiate-archive-download task,
    other argments are optional and default to values in photo-organizer.json
    """
    if job_id is None and job_file is None:
        logger.error('Must give either job_id or job_file!')
        return None
    if job_file is None:
        job_info = {
            'account_id': account_id,
            'vault_name': vault_name,
            'id': job_id
        }
    else:
        job_info = json.load(open(job_file))
    job_info['ctx'] = ctx
    with JobCtx(**job_info) as job:
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

@task
def archive_tree(ctx, rootdir):
    """Create zip archive of rootdir content
    """
    logger.info('Creating archives from rootdir {}'.format(rootdir))
    for path, fnames in leaf_dirs(rootdir):
        create_archive(path, fnames)
