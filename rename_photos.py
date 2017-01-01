#!./env/bin/python
import exifread
import re
from os import walk, path, renames
from hachoir_metadata import extractMetadata
from hachoir_parser import createParser
from hachoir_core.cmd_line import unicodeFilename
import hashlib
import argparse
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


def move_failed(fpath, failed_folder):
    fname = fpath.split('/')[-1]
    new_fpath = path.join(failed_folder, fname)
    renames(fpath, new_fpath)


def get_ts_year(x):
    try:
        return int(x[1].split(':')[0])
    except:
        return None


def get_jounia920_ts(x):
    fname = x[0].split('/')[-1]
    # ptrn = r'Jounia920_\d{8}_\d*\.mp4'
    ptrn = r'Windows Phone_\d{8}_\d*\.mp4'
    m = re.match(ptrn, fname)

    if m is None:
        return None

    dt = re.search(r'\d{8}', fname).group()
    y = dt[:4]
    m = dt[4:6]
    d = dt[6:8]

    sec = fname.split('_')[-1].split('.')[0][-2:]
    return '{}:{}:{} 00:00:{}'.format(y, m, d, sec)


def fix_jounia920_video_ts(vpath):

    if get_ts_year(vpath) > 2000:
        return vpath

    new_ts = get_jounia920_ts(vpath)
    if new_ts is None:
        return vpath

    return(vpath[0], new_ts)


def main():
    logger.info("Starting photo rename")
    parser = argparse.ArgumentParser(description="Rename photos")
    parser.add_argument("-i", "--input", help="input photo directory")
    parser.add_argument("-o", "--output", help="output photo directory")
    parser.add_argument("-f", "--failed", help="failed renaming directory")
    args = parser.parse_args()

    new_rootpath = args.output
    old_rootpath = args.input
    failed_folder = args.failed or './fail'
    logger.info('input path: {}'.format(old_rootpath))
    logger.info('output path: {}'.format(new_rootpath))
    logger.info('failed path: {}'.format(failed_folder))

    image_paths, video_paths = find_vid_img_files(old_rootpath)
    video_paths = [fix_jounia920_video_ts(x) for x in video_paths]
    failed = []

    logger.info('renaming {} photos'.format(len(image_paths)))
    for fpath, exif_date in image_paths:
        fail = move_iv_file(fpath, exif_date, new_rootpath, prefix='photo_')
        if fail is not None:
            failed.append(fail)

    logger.info('renaming {} videos'.format(len(video_paths)))
    for fpath, exif_date in video_paths:
        fail = move_iv_file(fpath, exif_date, new_rootpath, prefix='video_')
        if fail is not None:
            failed.append(fail)

    logger.info('failed to rename {} files'.format(len(failed)))
    logger.info('moving failed files to {}'.format(failed_folder))
    for fpath in failed:
        move_failed(fpath, failed_folder)
    logger.info('done!')

if __name__ == '__main__':
    main()
    # new_rootpath = '/Users/jkallunk/Pictures/Personal'
    # old_rootpath = '/Users/jkallunk/Pictures/iPhoto Library.photolibrary/Masters/2015'
    # failed_folder = '/Users/jkallunk/Pictures/failedpics'
