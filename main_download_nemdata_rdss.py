from src.nemdata_schd.download_nemdata_archive_schd import download_nem_archive_update
from src.nemdata_schd.download_nemdata_current_schd import download_nem_current_update
from conf.config import *
#from src.nem_data.dl_nem_data_sample import download_sample_file

log = setup_logger(__name__) # setup logging

def download_nemdata(root_path=None):
    # root_path = r"C:\Users\Jimmy\Documents\NEM" if root_path is None else root_path
    root_path = r'U:\Research\Projects\sef\encast\NEM' if root_path is None else root_path
    download_nem_archive_update(root_path, update_metadata=True, save_metadata=True)
    download_nem_current_update(root_path, update_metadata=True, save_metadata=True)
    pass



if __name__ == '__main__':
    root_path = r'U:\Research\Projects\sef\encast\NEM'
    download_nemdata(root_path)
    pass