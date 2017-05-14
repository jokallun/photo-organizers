import boto3
import os
import json
from datetime import datetime
from zipfile import ZipFile, ZIP_STORED
import argparse
import logging
import hashlib

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

VAULT_NAME = 'new-photos'
ACCOUNT_ID = '-'
REGION = 'eu-west-1'
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

def upload_archive(archive_name, description=None, add_checksum=True,
                   vault_name=VAULT_NAME, account_id=ACCOUNT_ID):
    logger.info('Uploading archive {} to vault {}'.format(archive_name, vault_name))
    glacier = boto3.resource('glacier', region_name=REGION)
    vault = glacier.Vault(account_id, vault_name)
    description = description or archive_name
    if add_checksum:
        checksum = get_checksum(archive_name)
        description = '{} {}'.format(description, checksum)
    archive = vault.upload_archive(archiveDescription=description, body=archive_name)
    info = {
        'account_id': archive.account_id,
        'vault_name': archive.vault_name,
        'archive_name': archive_name,
        'id': archive.id,
        'description': description,
        'datetime': datetime.today().strftime('%Y-%m-%d:%H:%M:%S')
    }
    with open('glacier-archive-{}.json'.format(archive_name), 'w') as f:
        json.dump(info, f)
    return archive

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rename photos')
    parser.add_argument('-r', '--rootdir', help='root dir for archiving')
    parser.add_argument('-v', '--vault_name',
                        help='name of aws galcier vault',
                        default=VAULT_NAME)
    args = parser.parse_args()
    if args.rootdir:
        archive_tree(args.rootdir)
