import exifread
from invoke import task
from os import walk, path, renames
from hachoir_metadata import extractMetadata
from hachoir_parser import createParser
from hachoir_core.cmd_line import unicodeFilename
import hashlib
import logging

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


def file_md5(fpath):
    with open(fpath, 'rb') as ofile:
        data = ofile.read()
    return hashlib.md5(data).hexdigest()


def get_video_date(fname):
    try:
        parser = createParser(unicodeFilename(fname))
        metadata = extractMetadata(parser)
        v = metadata.getItem('creation_date', 0)
    except:
        return None
    if v is None:
        return None
    else:
        return str(v.text).replace('-', ':')


def get_image_date(fname):
    with open(fname, 'rb') as f:
        tags = exifread.process_file(f, stop_tag='DateTimeOriginal')
        dttag = tags.get('EXIF DateTimeOriginal')
    if dttag is None:
        return None
    else:
        return dttag.values


def find_vid_img_files(topdir):
    video_fnames = []
    image_fnames = []
    for root, dirs, fnames in walk(topdir):
        for fname in fnames:
            fpath = path.join(root, fname)
            exif_date = get_image_date(fpath)
            if exif_date is not None:  # image file
                image_fnames.append((fpath, exif_date))
            else:
                exif_date = get_video_date(fpath)
                if exif_date is not None:  # video file
                    video_fnames.append((fpath, exif_date))
    return (image_fnames, video_fnames)


def get_new_fpath(rootpath, exif_date, prefix='photo_', suffix='jpg'):
    ymd = exif_date.split(' ')[0].split(':')
    if len(ymd) != 3 or int(ymd[0]) < 1990:
        return None
    # 2013:09:28 14:56:31
    ts_string = exif_date.replace(' ', 'T').replace(':', '-')
    fname = prefix + ts_string + '.' + suffix
    return path.join(rootpath, ymd[0], ymd[1], fname)


def shift_path(fpath):
    x, suffix = fpath.split('.')
    if x[-3] == '_':
        idx = int(x[-2:]) + 1
    else:
        idx = 1
    return x[:-3] + '_{:02}.'.format(idx) + suffix


def move_iv_file(fpath, exif_date, rootpath, prefix='photo_'):
    suffix = fpath.split('.')[-1].lower()
    new_fpath = get_new_fpath(rootpath, exif_date, prefix, suffix)
    if new_fpath is None:
        logger.info("No valid new path for file {}".format(fpath))
        return fpath
    while path.exists(new_fpath):
        # check if duplicate
        if file_md5(new_fpath) == file_md5(fpath):
            logger.info('Duplicate files: {}\t{}'.format(fpath, new_fpath))
            return None
        new_fpath = shift_path(new_fpath)
    renames(fpath, new_fpath)
    return None


def move_failed(fpath, failed_dir):
    fname = fpath.split('/')[-1]
    new_fpath = path.join(failed_dir, fname)
    renames(fpath, new_fpath)


def get_ts_year(x):
    try:
        return int(x[1].split(':')[0])
    except:
        return None

@task
def rename_photos(ctx, input_dir, output_dir, failed_dir='./fail'):
    """Rename photos found under input-dir
    according to their timestamp and move them
    to by-date organized folders under output-dir.

    If rename fails, the photo is moved to failed-dir,
    which defaults to ./fail.

    If renamed file exists, a md5 checksum is calculated
    to check for duplicate file.
    """
    logger.info("Starting photo rename")
    logger.info('input path: {}'.format(input_dir))
    logger.info('output path: {}'.format(output_dir))
    logger.info('failed path: {}'.format(failed_dir))

    image_paths, video_paths = find_vid_img_files(input_dir)
    failed = []

    logger.info('renaming {} photos'.format(len(image_paths)))
    for fpath, exif_date in image_paths:
        fail = move_iv_file(fpath, exif_date, output_dir, prefix='photo_')
        if fail is not None:
            failed.append(fail)

    logger.info('renaming {} videos'.format(len(video_paths)))
    for fpath, exif_date in video_paths:
        fail = move_iv_file(fpath, exif_date, output_dir, prefix='video_')
        if fail is not None:
            failed.append(fail)

    logger.info('failed to rename {} files'.format(len(failed)))
    logger.info('moving failed files to {}'.format(failed_dir))
    for fpath in failed:
        move_failed(fpath, failed_dir)
    logger.info('done!')
