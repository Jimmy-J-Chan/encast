from conf.config import *

def parse_region_data():
    """
    extend version of aggregated data collected here
    - https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data
    - we want to include UIGF generation as well
    - along with some other information
    - use Dispatch_IS_Report file to compile
    - 5min data
    - region and interconnectors
    """

    base_fpath = r'D:\nemdata\CURRENT\DispatchIS_Reports'
    file_names = os.listdir(base_fpath)
    for f in file_names:
        tmpfn = base_fpath + f"/{f}"
        df = pd.read_csv(tmpfn)

        pass














    pass



if __name__ == '__main__':
    parse_region_data()
    pass