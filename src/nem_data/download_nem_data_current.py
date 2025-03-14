import pandas as pd
from conf.config import *
import requests
import bs4
import time
import os
import shutil

def download_save_zip_file(url=None, fn_save=None):
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open a local file in binary write mode
        with open(fn_save, 'wb') as file:
            # Write the content of the response to the file
            file.write(response.content)
    # else:
    #     print('Failed to download the file. Status code:', response.status_code)
    return response.status_code
def get_page_level_metadata(url=None):
    soup = bs4.BeautifulSoup(requests.get(url).content, features="lxml")
    timestamp_size = [c.next_element.text.strip() for c in soup.find_all('br')]
    timestamp_size = [c for c in timestamp_size if len(c)>0]
    metadata = pd.DataFrame()
    if len(timestamp_size)>0:
        timestamp = [c.rsplit(None,1)[0].strip() for c in timestamp_size]
        size = [c.rsplit(None,1)[1].strip() for c in timestamp_size]
        file_name = [c.text for c in soup.find('pre').find_all('a')]
        file_name = [c.strip() for c in file_name if c not in ['[To Parent Directory]']]
        href = [c['href'] for c in soup.find('pre').find_all('a')][1:]
        metadata = pd.DataFrame(zip(timestamp, size, file_name, href), columns=['update_datetime', 'file_size', 'file_name', 'url_path'])
        assert len(timestamp_size)==len(file_name)
        assert len(timestamp_size)==len(href)
    return metadata
def get_metadata_current(update=False, save=False):
    fn_save = 'metadata_current.pkl'
    if not update:
        return pd.read_pickle(fn_save)

    # get links to zip files to download
    current_page_level = 0
    base_url = r'https://nemweb.com.au'
    url_l0 = r'https://nemweb.com.au/Reports/CURRENT/'
    print(f'Level {current_page_level} - Getting page metadata')
    metadata = get_page_level_metadata(url_l0)
    metadata['page_level'] = current_page_level
    metadata_current_level = metadata.loc[metadata['page_level']==current_page_level]
    metadata_current_level_dirs = metadata_current_level.loc[metadata_current_level['file_size']=='<dir>']
    more_dirs = len(metadata_current_level_dirs)>0

    while more_dirs:
        current_page_level +=1

        # goto each dir - download zip or goto next dir down
        for ix, row in metadata_current_level_dirs.iterrows():
            url_path = row['url_path']
            dir_name = row['file_name']
            tmp_url = f"{base_url}/{url_path}"
            print(f'Level {current_page_level} - Getting page metadata - {dir_name}')
            tmp_metadata = get_page_level_metadata(tmp_url)
            tmp_metadata['parent_file_name'] = dir_name
            tmp_metadata['page_level'] = current_page_level
            metadata = cc(metadata, tmp_metadata, axis=0)
            pass

        # check next level
        metadata_current_level = metadata.loc[metadata['page_level'] == current_page_level]
        metadata_current_level_dirs = metadata_current_level.loc[metadata_current_level['file_size'] == '<dir>']
        more_dirs = len(metadata_current_level_dirs) > 0

    metadata = metadata.reset_index(drop=True)
    if save:
        print('Saving metadata current')
        metadata.to_pickle(fn_save)
    return metadata
def check_folders_exists(base_fn_save, folder_path):
    tmp_path = base_fn_save
    tmp_path = f"{tmp_path}/{folder_path}"
    if not os.path.isdir(tmp_path):
        os.makedirs(tmp_path)
    pass

def download_nem_current(base_fn_save=None, update_metadata=False, save_metadata=False, last_updated=None):
    base_url = r'https://nemweb.com.au'
    metadata = get_metadata_current(update=update_metadata, save=save_metadata)
    file2dl = metadata.loc[metadata['file_size']!='<dir>'].reset_index(drop=True)
    if last_updated is not None:
        file2dl['update_datetime'] = pd.to_datetime(file2dl['update_datetime'], format='%A, %B %d, %Y %I:%M %p')
        file2dl = file2dl.loc[file2dl['update_datetime']>=last_updated].reset_index(drop=True)

    n_files = len(file2dl)
    if n_files>0:
        for ix, row in file2dl.iterrows():
            url_path = row['url_path']
            file_name = row['file_name']

            path_elements = [c for c in url_path.split('/')]
            path_elements = [c for c in path_elements if len(c)>0]
            path_elements = [c for c in path_elements if c!='Reports'][:-1]
            folder_path = '/'.join(path_elements)
            tmp_url = f"{base_url}/{url_path}"
            tmp_fn_save = f"{base_fn_save}/{folder_path}/{file_name}"

            # check if we have d/l file already
            if not os.path.isfile(tmp_fn_save):
                # check we have folders for path elements else create
                check_folders_exists(base_fn_save, folder_path)

                print(f"{ix}/{n_files-1} - downloaded file: {folder_path}/{file_name}", end=' - ')
                status_code = download_save_zip_file(tmp_url, tmp_fn_save)
                print('SUCCESS' if status_code==200 else 'FAILED')
            else:
                print(f"{ix}/{n_files-1} - file already downloaded: {folder_path}/{file_name}")
            pass
    else:
        print('no files to download today')
    pass

def download_nem_current_hist(base_fn_save=None, update_metadata=False, save_metadata=False):
    download_nem_current(base_fn_save, update_metadata, save_metadata)
    pass

def download_nem_current_update(base_fn_save=None, update_metadata=False, save_metadata=False):
    fn_metadata = 'metadata_current.pkl'
    fn_metadata_archive = f'metadata_current_{pd.Timestamp.today():%Y_%m_%d}.pkl'
    last_updated = pd.to_datetime(os.path.getmtime(fn_metadata), unit='s') - pd.offsets.Day(3)
    if not os.path.isfile(fn_metadata_archive):
        shutil.copy(fn_metadata, fn_metadata_archive)
    download_nem_current(base_fn_save, update_metadata, save_metadata, last_updated)
    pass

if __name__ == '__main__':
    base_fn_save = r"C:\Users\Jimmy\Documents\NEM"
    #download_nem_current_hist(base_fn_save, update_metadata=False, save_metadata=False)
    download_nem_current_update(base_fn_save, update_metadata=True, save_metadata=True)

    pass