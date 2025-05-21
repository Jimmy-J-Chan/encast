from src.nem_data.download_nem_data_archive import download_nem_archive_update
from src.nem_data.download_nem_data_current import download_nem_current_update


if __name__ == '__main__':
    base_fn_save = r"C:\Users\Jimmy\Documents\NEM"
    download_nem_archive_update(base_fn_save, update_metadata=True, save_metadata=True)
    download_nem_current_update(base_fn_save, update_metadata=True, save_metadata=True)
    pass