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

def search_by_folder(dir_path=None, file_paths=[]):
    #file_paths = []
    #folders = os.listdir(dir_path)#[-3:] #5:15

    # use scandir #
    with os.scandir(dir_path) as it:
        folders = [f.name for f in it]

    if all(os.path.isdir(f"{dir_path}/{c}") for c in folders):
        # if all folders -> goto next level
        for folder in folders:
            tmp_file_paths = search_by_folder(f"{dir_path}/{folder}", file_paths)
            file_paths.append(tmp_file_paths)
    else:
        # if all files
        tmpfiles = [f"{dir_path}/{c}" for c in folders]
        return tmpfiles
    file_paths = sum(file_paths, [])
    return file_paths

def fast_get_dir_all_paths(dir_path=None):
    file_paths = []
    for dirpath, _, filenames in os.walk(dir_path):
        for filename in filenames:
            file_paths.append(os.path.join(dirpath, filename))

    # remove parent path
    str_cut = len(dir_path) + 1
    file_paths_cut = [c[str_cut:] for c in file_paths]
    return file_paths_cut


def replicate_metadata(src_parent_path=None, dest_parent_path=None):
    # from local to shared location

    fn_src_dir = src_parent_path + '/src/nem_data'
    fn_dest_dir = dest_parent_path

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

def replicate_data_files(parent_folder=None, src_parent_path=None, dest_parent_path=None):
    # replicate files from local to shared location

    # get all file paths at src and dest
    fn_src_dir = f"{src_parent_path}/{parent_folder}"
    fn_dest_dir = f"{dest_parent_path}/{parent_folder}"
    src_filepaths = get_dir_all_paths(fn_src_dir)
    dest_filepaths = get_dir_all_paths(fn_dest_dir)

    # save for testing
    test_save = False
    if test_save:
        df = Struct()
        df['src'] = src_filepaths
        df['dest'] = dest_filepaths
        df.to_pickle('filepaths.pkl')
    else:
        tmp = pd.read_pickle('filepaths.pkl')
        src_filepaths = tmp['src']
        dest_filepaths = tmp['dest']

    # copy files in src but not in dest
    tmpdiff = list(set(src_filepaths) - set(dest_filepaths))  # quick diff
    for f in tmpdiff:
        tmpsrc = f"{fn_src_dir}/{f}"
        tmpdest = f"{fn_dest_dir}/{f}"
        shutil.copy(tmpsrc, tmpdest)
    pass

def pc2schd():
    # metadata
    src_parent_path = config.project.folder
    # dest_parent_path = r"U:\NEM" # off campus
    dest_parent_path = r'U:\Research\Projects\sef\encast\NEM' # on campus
    replicate_metadata(src_parent_path, dest_parent_path)

    # nemdata
    src_parent_path = r"C:\Users\Jimmy\Documents\NEM"
    replicate_data_files('CURRENT', src_parent_path, dest_parent_path)
    replicate_data_files('ARCHIVE', src_parent_path, dest_parent_path)
    pass


if __name__ == '__main__':
    ### SHOULD ONLY RUN THIS INFREQUENTLY as it takes along time

    # metadata
    src_parent_path = config.project.folder
    # dest_parent_path = r"U:\NEM" # off campus
    dest_parent_path = r'U:\Research\Projects\sef\encast\NEM' # on campus
    # replicate_metadata(src_parent_path, dest_parent_path)

    # nemdata
    src_parent_path = r"C:\Users\Jimmy\Documents\NEM"
    replicate_data_files('CURRENT', src_parent_path, dest_parent_path)
    # replicate_data_files('ARCHIVE', src_parent_path, dest_parent_path)
