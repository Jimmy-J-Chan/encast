import os
import shutil
from conf.config import *

def get_dir_all_paths(dir_path=None):
    # TODO: get all files paths given parent directory

    return file_paths


def replicate_metadata():
    fn_dest_dir = r"U:\NEM
    fn_src_dir = config.project.folder + '/src/nem_data'

    ## current
    fnsrc = fn_src_dir + '/metadata_current.pkl'
    fndest = fn_dest_dir + '/metadata/metadata_current.pkl'
    shutil.copy(fnsrc, fndest)

    tmp_src_dir = os.listdir(fn_src_dir)
    src_dir_cur_archived = [c for c in tmp_src_dir if c.startswith('metadata_current_')]
    dest_dir_cur_archived = os.listdir(fn_dest_dir + '/metadata/CURRENT')
    tmpdiff = [c for c in src_dir_cur_archived if c not in dest_dir_cur_archived]

    for c in tmpdiff:
        # copy over missing files
        fnsrc = fn_src_dir + f'/{c}'
        fndest = fn_dest_dir + f'/metadata/CURRENT/{c}'
        shutil.copy(fnsrc, fndest)

    ## archive
    fnsrc = fn_src_dir + '/metadata_archive.pkl'
    fndest = fn_dest_dir + '/metadata/metadata_archive.pkl'
    shutil.copy(fnsrc, fndest)

    src_dir_arc_archived = [c for c in tmp_src_dir if c.startswith('metadata_archive_')]
    dest_dir_arc_archived = os.listdir(fn_dest_dir + '/metadata/ARCHIVE')
    tmpdiff = [c for c in src_dir_arc_archived if c not in dest_dir_arc_archived]

    for c in tmpdiff:
        # copy over missing files
        fnsrc = fn_src_dir + f'/{c}'
        fndest = fn_dest_dir + f'/metadata/ARCHIVE/{c}'
        shutil.copy(fnsrc, fndest)

    pass

def replicate_archive():
    fn_src_dir = r"C:\Users\Jimmy\Documents\NEM\ARCHIVE"
    fn_dest_dir = r"U:\NEM\ARCHIVE"

    folder_names_src = os.listdir(fn_src_dir)
    # recursively



    # # get all file paths from src and dest
    # src_file_paths = os.listdir(fn_src_dir)
    # dest_file_paths = os.listdir(fn_dest_dir)
    # tmpdiff = [c for c in src_file_paths if c not in dest_file_paths]
    #
    # for c in tmpdiff:
    #     # copy over missing files
    #     fnsrc = fn_src_dir + f'/{c}'
    #     fndest = fn_dest_dir + f'/{c}'
    #     shutil.copy(fnsrc, fndest)



    pass

def pc2rdss():
    # copy metadata
    replicate_metadata()

    # copy nemdata
    replicate_archive()

    pass


if __name__ == '__main__':
    pc2rdss()
