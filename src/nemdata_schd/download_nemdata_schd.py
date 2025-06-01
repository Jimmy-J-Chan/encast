from src.nemdata_schd.download_nemdata_archive_schd import download_nem_archive_update
from src.nemdata_schd.download_nemdata_current_schd import download_nem_current_update




if __name__ == '__main__':
    base_fn_save = ''
    download_nem_archive_update(base_fn_save, update_metadata=True, save_metadata=True)
    download_nem_current_update(base_fn_save, update_metadata=True, save_metadata=True)
    pass