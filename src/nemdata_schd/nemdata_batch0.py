import pandas as pd

from conf.config import *
import zipfile
import shutil

staging_fpath = r"C:\staging"
onedrive_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\data\NEM"
portable_fpath = r"D:\nemdata"



#TODO: check missed files between portable and batched zips

def rebuild_data_files():
    root_fpath = r"U:\Research\Projects\sef\encast\NEM\metadata"

    #CURRENT
    cur_fpath = root_fpath + '/CURRENT'
    cur_files = os.listdir(cur_fpath)
    cur_files = pd.DataFrame(cur_files, columns=['filename'])
    cur_files['date'] = cur_files['filename'].str.split('_', n=2, expand=True)[2]
    cur_files['date'] = pd.to_datetime(cur_files['date'].str.split('.pkl', expand=True)[0], format='%Y_%m_%d')
    cur_files = cur_files.sort_values(by='date', ascending=True)
    cur_files['prev_filename'] = cur_files['filename'].shift()

    for ix, row in cur_files.iterrows():
        tmpfn = cur_fpath + f'/{row["filename"]}'
        tmpmeta = pd.read_pickle(tmpfn)

        tmpmeta['metadata_update_datetime'] = row['date']
        tmpmeta['prev_metadata_filename'] = row['prev_filename']
        tmpmeta['file2download'] = False
        tmpmeta['downloaded'] = False
        tmpmeta['file_path'] = tmpmeta['url_path'].str[len('/Reports'):]

        if row['prev_filename'] is None:
            # batch files, no diffing
            tmpmeta.loc[(tmpmeta['file_size']!='<dir>'), 'file2download'] = True

            # get files to batch
            tmpmeta_batch = tmpmeta.loc[tmpmeta['file2download']].copy()

            # create all directories
            print(f"Creating directories")
            tmpmeta_dirs = tmpmeta.loc[(tmpmeta['file_size']=='<dir>')]
            for fpath in tmpmeta_dirs['file_path']:
                tmpfpath = staging_fpath + fpath
                os.makedirs(tmpfpath, exist_ok=False)

            # move files to local staging folder
            _n = len(tmpmeta_batch)
            print(f"Copying files to staging")
            #for ix, rowb in tmpmeta_batch.sample(200).iterrows():
            for ix, rowb in tmpmeta_batch.iterrows():
                print(f" -> {ix}/{_n} - {rowb['file_path']}")
                src = portable_fpath + rowb['file_path']
                dest = staging_fpath + rowb['file_path']
                if os.path.isfile(src):
                    shutil.copy2(src, dest)
                    tmpmeta.loc[ix, 'downloaded'] = True
                else:
                    print('file does not exist')
                    tmpmeta.loc[ix, 'downloaded'] = 'missing'
                    pass
                pass
            pass

            # zip directory and move to onedrive
            print('ARCHIVING files')
            src = staging_fpath + '/CURRENT'
            dest = onedrive_fpath + f"/CURRENT/CURRENT_{row['date']:%Y_%m_%d}.zip"
            with zipfile.ZipFile(dest, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                for folder_name, subfolders, filenames in os.walk(src):
                    for filename in filenames:
                        file_path = os.path.join(folder_name, filename)
                        zip_ref.write(file_path, arcname=os.path.relpath(file_path, src))

            # save metadata
            print('Saving metdata to onedrive')
            tmpfn = onedrive_fpath + f"/metadata/CURRENT/{row['filename']}"
            tmpmeta.to_pickle(tmpfn)

            # rename staging
            src = staging_fpath + '/CURRENT'
            dest = staging_fpath + f"/CURRENT_{row['date']:%Y_%m_%d}"
            os.rename(src, dest)
        pass


    pass




def get_folder_size(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total

def print_subfolder_sizes(base_folder):
    df = pd.DataFrame(columns=['folder','size'])
    ix = 0
    for root, dirs, _ in os.walk(base_folder):
        for d in dirs:
            folder_path = os.path.join(root, d)
            size = get_folder_size(folder_path)

            df.loc[ix, 'folder'] = folder_path
            df.loc[ix, 'size'] = format_size(size)
            ix +=1

    pass

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024




if __name__ == '__main__':
    print_subfolder_sizes(r"C:\staging\CURRENT")



    rebuild_data_files()


    pass