import os
import shutil
from conf.config import *

def get_dir_all_paths(dir_path=None):
    file_paths = []
    for dirpath, _, filenames in os.walk(dir_path):
        for filename in filenames:
            file_paths.append(os.path.join(dirpath, filename))

    # remove parent path
    str_cut = len(dir_path)+1
    file_paths_cut = [c[str_cut:] for c in file_paths]
    return file_paths_cut


def replicate_metadata():
    fn_dest_dir = r"U:\NEM"
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

def replicate_files(parent_folder=None):
    fn_src_dir = r"C:\Users\Jimmy\Documents\NEM\{}"
    fn_dest_dir = r"U:\NEM\{}"

    src_filepaths = get_dir_all_paths(fn_src_dir.format(parent_folder))
    dest_filepaths = get_dir_all_paths(fn_dest_dir.format(parent_folder))

    df = Struct()
    df['src'] = src_filepaths
    df['dest'] = dest_filepaths
    df.to_pickle('filepaths.pkl')

    tmpdiff = [c for c in src_filepaths if c not in dest_filepaths]


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
    replicate_files('CURRENT')
