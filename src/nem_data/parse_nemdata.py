import pandas as pd
import zipfile
from conf.config import *
import io

"""
# DispatchIS_Reports, 
- Price
- Demand
 
# Dispatch_SCADA
- generator production 

# Next_Day_Actual_Gen

# Public_Prices

# Short_Term_PASA_Reports
# predipsatch 5m 
"""

def read_inner_csv(base_fpath, dir, f, csv_params={}):
    """
    Assumes only one csv file nested inside zip file path
    """

    # read outer zip
    outer_fpath = base_fpath + f"/{dir}"
    with zipfile.ZipFile(outer_fpath, 'r') as outer_zip:
        # read inner zip
        with outer_zip.open(f) as inner_file:
            inner_bytes = inner_file.read()
            inner_zip = zipfile.ZipFile(io.BytesIO(inner_bytes))
            fname = inner_zip.namelist()[0]

            # read csv
            with inner_zip.open(fname) as f:
                df = pd.read_csv(f, **csv_params, engine='python')
    return df

def _get_diff_file_names(file_names, data_fpath, regions=None, file_type=None):
    if regions is None:
        df_fpath = data_fpath
    else:
        df_fpath = data_fpath.format(regions[0])

    if os.path.isfile(df_fpath):
        # get strip str
        if file_type in ['dispatchIS_reports']:
            str_strip = 'PUBLIC_DISPATHIS_.zip'
        elif file_type in ['dispatch_scada']:
            str_strip = 'PUBLIC_DISPATCHSCADA_.zip'

        df = pd.read_pickle(df_fpath)
        file_names_df = pd.Series(file_names).to_frame('file_names')
        file_names_df['datetime'] = pd.to_datetime(
            file_names_df['file_names'].str.strip(str_strip).str.split('_', expand=True)[0])
        mask = file_names_df['datetime'].isin(df.index)
        file_names_diff = file_names_df.loc[~mask, 'file_names'].to_list()
        return file_names_diff
    else:
        return file_names

########################################################################################################################

def parse_dispatchIS_reports_deeparchive():
    """
    Data stored in cold storage
     - https://visualisations.aemo.com.au/aemo/nemweb/index.html#mms-data-model
     - 2009 -> 2025
     - monthly
    """

    # flags
    update_price = False
    update_demand = True

    # fpaths
    base_urlp = r'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{}/MMSDM_{}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DISPATCHPRICE_{}0000.zip'
    base_urld = r'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{}/MMSDM_{}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DISPATCHREGIONSUM_{}0000.zip'
    root_save_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data"
    price_fpath = root_save_fpath + "/NEM/historical/price_{}.pkl"
    demand_fpath = root_save_fpath + "/NEM/historical/demand_{}.pkl"

    # params
    regions = ['NSW1','QLD1','SA1','TAS1','VIC1']
    start_date = '2014-12-01' #'2009-07-01'
    end_date = '2024-02-01'
    dr = pd.date_range(start_date, end_date, freq='MS')
    dr5m = pd.date_range(start_date, end_date, freq='5min')

    if update_price:
        # collect and save by month
        print(f' -> Updating prices')
        lenDR = len(dr)
        dfprices = {k: pd.read_pickle(price_fpath.format(k)) for k in regions}

        # get diff dr
        dr_diff = dr5m.difference(dfprices[regions[0]].index)
        dr_diff = pd.date_range(dr_diff.min(), dr_diff.max(), freq='MS').sort_values(ascending=False) # collect in reverse
        for ix, d in enumerate(dr_diff):
            print(f" ->> price - {ix}/{lenDR} - {d}")

            # get data
            data_url = base_urlp.format(d.year, f'{d:%Y_%m}', f'{d:%Y%m%d}')
            df = pd.read_csv(data_url, skiprows=1, skipfooter=1, engine='python')

            # parse
            df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
            df['LASTCHANGED'] = pd.to_datetime(df['LASTCHANGED'])
            cols2f = ['LASTCHANGED', 'SETTLEMENTDATE']
            df = df[cols2f + [col for col in df.columns if col not in cols2f]]
            df = df.set_index('SETTLEMENTDATE', drop=False)

            # check for diff cols
            xcols = ['RAISE1SECRRP', 'RAISE1SECROP', 'RAISE1SECAPCFLAG', 'LOWER1SECRRP', 'LOWER1SECROP',
                     'LOWER1SECAPCFLAG', 'PRE_AP_RAISE1_PRICE', 'PRE_AP_LOWER1_PRICE', 'CUMUL_PRE_AP_RAISE1_PRICE',
                     'CUMUL_PRE_AP_LOWER1_PRICE', 'OCD_STATUS', 'MII_STATUS']
            diffcols = [c for c in dfprices[regions[0]] if c not in df.columns.to_list()+xcols]
            if len(diffcols)>0:
                print(f" -> missing columns: {diffcols}")

            for region in regions:
                print(f' -> {region} - concat and saving data ', end='')
                mask = df['REGIONID'] == region
                df_region = df.loc[mask]

                # drop overlapping
                mask = df_region.index.isin(dfprices[region].index)
                df_region = df_region.loc[~mask]

                if not df_region.empty:
                    dfprices[region] = cc(dfprices[region], df_region, axis=0).sort_index()
                    # drop some cols
                    cols2drop = ['5','4','1']
                    dfprices[region] = dfprices[region][[c for c in dfprices[region].columns if c not in cols2drop]]
                    dfprices[region].to_pickle(price_fpath.format(region))  # save
                    print(' - done')
                else:
                    print(' - no data to concat...skip')

            pass

        # check missing 5min interval for each region
        for region in regions:
            start_date = dfprices[region].index.min()
            end_date = dfprices[region].index.max()
            dr = pd.date_range(start_date, end_date, freq='5min')

            miss_5m = dr.difference(dfprices[region].index)
            if len(miss_5m)>0:
                print(f" -> {region} - sdate: {start_date} - edate: {end_date} -  missing 5m: {[c for c in miss_5m]}")

    if update_demand:
        # collect and save by month
        print(f' -> Updating demand')
        lenDR = len(dr)
        dfdemands = {k: pd.read_pickle(demand_fpath.format(k)) for k in regions}

        # get diff dr
        dr_diff = dr5m.difference(dfdemands[regions[0]].index)
        dr_diff = pd.date_range(dr_diff.min(), dr_diff.max(), freq='MS').sort_values(ascending=False) # collect in reverse
        for ix, d in enumerate(dr_diff):
            print(f" ->> demand - {ix}/{lenDR} - {d}")

            # get data
            data_url = base_urld.format(d.year, f'{d:%Y_%m}', f'{d:%Y%m%d}')
            df = pd.read_csv(data_url, skiprows=1, skipfooter=1, engine='python')

            # parse
            df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
            df['LASTCHANGED'] = pd.to_datetime(df['LASTCHANGED'])
            cols2f = ['LASTCHANGED', 'SETTLEMENTDATE']
            df = df[cols2f + [col for col in df.columns if col not in cols2f]]
            df = df.set_index('SETTLEMENTDATE', drop=False)

            # check for diff cols
            xcols = ['8', 'RAISE1SECLOCALDISPATCH', 'LOWER1SECLOCALDISPATCH', 'RAISE1SECACTUALAVAILABILITY',
                     'LOWER1SECACTUALAVAILABILITY', 'SS_SOLAR_AVAILABILITY', 'SS_WIND_AVAILABILITY',
                     'BDU_ENERGY_STORAGE', 'BDU_MIN_AVAIL', 'BDU_MAX_AVAIL', 'BDU_CLEAREDMW_GEN', 'BDU_CLEAREDMW_LOAD','7']
            diffcols = [c for c in dfdemands[regions[0]] if c not in df.columns.to_list()+xcols]
            if len(diffcols)>0:
                print(f" -> missing columns: {diffcols}")

            for region in regions:
                print(f' -> {region} - concat and saving data ', end='')
                mask = df['REGIONID'] == region
                df_region = df.loc[mask]

                # drop overlapping
                mask = df_region.index.isin(dfdemands[region].index)
                df_region = df_region.loc[~mask]

                if not df_region.empty:
                    dfdemands[region] = cc(dfdemands[region], df_region, axis=0).sort_index()
                    # drop some cols
                    cols2drop = ['5','4','1','7','8']
                    dfdemands[region] = dfdemands[region][[c for c in dfdemands[region].columns if c not in cols2drop]]
                    dfdemands[region].to_pickle(demand_fpath.format(region))  # save
                    print(' - done')
                else:
                    print(' - no data to concat...skip')

            pass

        # check missing 5min interval for each region
        for region in regions:
            start_date = dfdemands[region].index.min()
            end_date = dfdemands[region].index.max()
            dr = pd.date_range(start_date, end_date, freq='5min')

            miss_5m = dr.difference(dfdemands[region].index)
            if len(miss_5m)>0:
                print(f" -> {region} - sdate: {start_date} - edate: {end_date} -  missing 5m: {[c for c in miss_5m]}")

    pass

def parse_dispatchIS_reports_archive():
    """
    populates data from archive
    """
    update_price = False
    update_demand = True

    regions = ['NSW1','QLD1','SA1','TAS1','VIC1']

    root_save_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data"
    price_fpath = root_save_fpath + "/NEM/historical/price_{}.pkl"
    demand_fpath = root_save_fpath + "/NEM/historical/demand_{}.pkl"

    # list all zip files
    base_fpath = r'D:\nemdata\ARCHIVE\DispatchIS_Reports'
    file_names = {}
    file_names_l1 = os.listdir(base_fpath) [:] # layer1
    for f in file_names_l1:
        with zipfile.ZipFile(base_fpath + f"/{f}", 'r') as zip_ref:
            file_names[f] = zip_ref.namelist()
    all_file_names = pd.DataFrame(file_names).melt()
    all_file_names_lst = all_file_names['value'].to_list()

    # # check for missing dates
    # str_strip = 'PUBLIC_DISPATHIS_.zip'
    # file_names_df = pd.Series(all_file_names_lst).to_frame('file_names')
    # file_names_df['datetime'] = pd.to_datetime(file_names_df['file_names'].str.strip(str_strip).str.split('_', expand=True)[0])
    # file_names_df['date'] = pd.to_datetime(file_names_df['datetime'].dt.date)
    # dr = pd.date_range(file_names_df['date'].min(), file_names_df['date'].max(), freq='D')
    # mask = file_names_df['date'].isin(dr)
    # file_names_diff = file_names_df.loc[~mask, 'file_names'].to_list()
    # if len(file_names_diff)>0:
    #     raise Exception

    # price
    if update_price:
        print(f' -> Updating prices')
        dfprice_hold = []

        file_names_diff = _get_diff_file_names(all_file_names_lst, price_fpath, regions, 'dispatchIS_reports')
        all_file_names_diff = all_file_names.loc[all_file_names['value'].isin(file_names_diff)].reset_index(drop=True)
        all_file_names_diff = all_file_names_diff.iloc[:].reset_index(drop=True) # in chunks of 25k
        lenFN = len(all_file_names_diff)
        if lenFN>0:
            for ix, row in all_file_names_diff.iterrows():
                dir = row['variable'] # 2nd zip
                f = row['value'] # csv file
                print(f" ->> {ix}/{lenFN} - {f}")

                # get tables indexes - I=header, D=data rows
                csv_params = {'usecols': [0, 2], 'index_col': False, 'header': None}
                dfidx = read_inner_csv(base_fpath, dir, f, csv_params)

                # price
                hidx = dfidx.loc[(dfidx[0]=='I') & (dfidx[2]=='PRICE')].index[0]
                didx = dfidx.loc[(dfidx[0] == 'D') & (dfidx[2] == 'PRICE')].index
                csv_params = {'skiprows': hidx, 'nrows': len(didx)}
                tmpdfp = read_inner_csv(base_fpath, dir, f, csv_params)

                # remove some cols
                # tmpdfp = tmpdfp.drop(columns=['I','DISPATCH', 'PRICE','5','RUNNO'])

                dfprice_hold.append(tmpdfp)
                pass

            # agg and parse
            dfprice_hold_agg = pd.concat(dfprice_hold, axis=0)
            dfprice_hold_agg['SETTLEMENTDATE'] = pd.to_datetime(dfprice_hold_agg['SETTLEMENTDATE'])
            #dfprice_hold_agg['update_datetime'] = dfprice_hold_agg['SETTLEMENTDATE'] - pd.DateOffset(minutes=5)

            cols2f = ['LASTCHANGED','SETTLEMENTDATE']
            dfprice_hold_agg = dfprice_hold_agg[cols2f + [col for col in dfprice_hold_agg.columns if col not in cols2f]]
            dfprice_hold_agg = dfprice_hold_agg.set_index('SETTLEMENTDATE', drop=False)
            dfprice_hold_agg['LASTCHANGED'] = pd.to_datetime(dfprice_hold_agg['LASTCHANGED'])

            # merge and save by region
            for ix, region in chunks(regions, enum=True):
                print(f' -> {region} - concat and saving data ', end='')
                mask = dfprice_hold_agg['REGIONID']==region
                dfprice_agg_region = dfprice_hold_agg.loc[mask]

                tmp_fpath = price_fpath.format(region)
                dfprice_region = pd.read_pickle(tmp_fpath) if os.path.isfile(tmp_fpath) else pd.DataFrame()
                dfprice_region = cc(dfprice_region, dfprice_agg_region, axis=0).sort_index()
                dfprice_region.to_pickle(tmp_fpath) # save
                print(' - done')
        else:
            print(' -> no files to update')

    # demand
    if update_demand:
        print(f' -> Updating demand')
        dfdemand_hold = []

        file_names_diff = _get_diff_file_names(all_file_names_lst, demand_fpath, regions, 'dispatchIS_reports')
        all_file_names_diff = all_file_names.loc[all_file_names['value'].isin(file_names_diff)].reset_index(drop=True)[:1000]
        lenFN = len(all_file_names_diff)
        if lenFN>0:
            for ix, row in all_file_names_diff.iterrows():
                dir = row['variable'] # 2nd zip
                f = row['value'] # csv file
                print(f" ->> {ix}/{lenFN} - {f}")

                # get tables indexes - I=header, D=data rows
                csv_params = {'usecols': [0, 2], 'index_col': False, 'header': None}
                dfidx = read_inner_csv(base_fpath, dir, f, csv_params)

                # demand
                hidx = dfidx.loc[(dfidx[0]=='I') & (dfidx[2]=='REGIONSUM')].index[0]
                didx = dfidx.loc[(dfidx[0] == 'D') & (dfidx[2] == 'REGIONSUM')].index
                csv_params = {'skiprows': hidx, 'nrows': len(didx)}
                tmpdfd = read_inner_csv(base_fpath, dir, f, csv_params)

                # remove some cols
                # tmpdfp = tmpdfp.drop(columns=['I','DISPATCH', 'PRICE','5','RUNNO'])

                dfdemand_hold.append(tmpdfd)
                pass

            # agg and parse
            dfdemand_hold_agg = pd.concat(dfdemand_hold, axis=0)
            dfdemand_hold_agg['SETTLEMENTDATE'] = pd.to_datetime(dfdemand_hold_agg['SETTLEMENTDATE'])
            #dfdemand_hold_agg['update_datetime'] = dfdemand_hold_agg['SETTLEMENTDATE'] - pd.DateOffset(minutes=5)

            cols2f = ['LASTCHANGED','SETTLEMENTDATE']
            dfdemand_hold_agg = dfdemand_hold_agg[cols2f + [col for col in dfdemand_hold_agg.columns if col not in cols2f]]
            dfdemand_hold_agg = dfdemand_hold_agg.set_index('SETTLEMENTDATE', drop=False)
            dfdemand_hold_agg['LASTCHANGED'] = pd.to_datetime(dfdemand_hold_agg['LASTCHANGED'])

            # merge and save by region
            for ix, region in chunks(regions, enum=True):
                print(f' -> {region} - concat and saving data ', end='')
                mask = dfdemand_hold_agg['REGIONID']==region
                dfdemand_agg_region = dfdemand_hold_agg.loc[mask]

                tmp_fpath = demand_fpath.format(region)
                dfdemand_region = pd.read_pickle(tmp_fpath) if os.path.isfile(tmp_fpath) else pd.DataFrame()
                dfdemand_region = cc(dfdemand_region, dfdemand_agg_region, axis=0).sort_index()
                dfdemand_region.to_pickle(tmp_fpath) # save
                print(' - done')
        else:
            print(' -> no files to update')
    pass

def parse_dispatchIS_reports_current_DONT_USE():
    """
    extend version of aggregated data collected here
    - https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data
    - we want to include UIGF generation as well
    - along with some other information
    - use Dispatch_IS_Report file to compile
    - 5min data
    - region and interconnectors

    Some definitions (ChatGPT)
    - Total Demand: grid demand (does not inlcude rooftop PV)
    - dispatchable generation: output from dispatchable(scheduled+semi-scheduled) generators
    - available generation: total capacity offered by generators
    - net interchange: net import (+) or export (-) of electricity (MW) across all interconnectors connected to a region

    - total demand = dispatchable generation + net interchange + losses
    --> electricity demand is met via combo of local generation and imports/exports


    features:
    - RRP: regional reference price - wholesale electricity spot price ($/MWh)
    - CLEAREDSUPPLY: scheduled demand (MW)
    - DISPATCHABLEGENERATION: scheduled generation (MW) + semi scheduled generation (MW)


    about data file:
    - Settlementdate
    - file is available ~5minutes (4.5mins) before settlement date

    """

    update_price = True
    update_demand = True

    regions = ['NSW1','QLD1','SA1','TAS1','VIC1']

    root_save_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data"
    price_fpath = root_save_fpath + "/NEM/historical/price_{}.pkl"
    demand_fpath = root_save_fpath + "/NEM/historical/demand_{}.pkl"

    base_fpath = r'D:\nemdata\CURRENT\DispatchIS_Reports'
    file_names = os.listdir(base_fpath) [:]

    # price
    if update_price:
        print(f' -> Updating prices')
        dfprice_hold = []

        file_names_diff = _get_diff_file_names(file_names, price_fpath, regions, 'dispatchIS_reports')
        lenFN = len(file_names_diff)
        if lenFN>0:
            for ix, f in chunks(file_names_diff, enum=True):
                print(f" ->> {ix}/{lenFN} - {f}")
                tmpfn = base_fpath + f"/{f}"

                # get tables indexes - I=header, D=data rows
                dfidx = pd.read_csv(tmpfn, usecols=[0, 2], index_col=False, header=None)

                # price
                hidx = dfidx.loc[(dfidx[0]=='I') & (dfidx[2]=='PRICE')].index[0]
                didx = dfidx.loc[(dfidx[0] == 'D') & (dfidx[2] == 'PRICE')].index
                tmpdfp = pd.read_csv(tmpfn, skiprows=hidx, nrows=len(didx))

                # remove some cols
                # tmpdfp = tmpdfp.drop(columns=['I','DISPATCH', 'PRICE','5','RUNNO'])

                dfprice_hold.append(tmpdfp)
                pass

            # agg and parse
            dfprice_hold_agg = pd.concat(dfprice_hold, axis=0)
            dfprice_hold_agg['SETTLEMENTDATE'] = pd.to_datetime(dfprice_hold_agg['SETTLEMENTDATE'])
            #dfprice_hold_agg['update_datetime'] = dfprice_hold_agg['SETTLEMENTDATE'] - pd.DateOffset(minutes=5)

            cols2f = ['LASTCHANGED','SETTLEMENTDATE']
            dfprice_hold_agg = dfprice_hold_agg[cols2f + [col for col in dfprice_hold_agg.columns if col not in cols2f]]
            dfprice_hold_agg = dfprice_hold_agg.set_index('SETTLEMENTDATE', drop=False)
            dfprice_hold_agg['LASTCHANGED'] = pd.to_datetime(dfprice_hold_agg['LASTCHANGED'])

            # merge and save by region
            for ix, region in chunks(regions, enum=True):
                print(f' -> {region} - concat and saving data ', end='')
                mask = dfprice_hold_agg['REGIONID']==region
                dfprice_agg_region = dfprice_hold_agg.loc[mask]

                tmp_fpath = price_fpath.format(region)
                dfprice_region = pd.read_pickle(tmp_fpath) if os.path.isfile(tmp_fpath) else pd.DataFrame()
                dfprice_region = cc(dfprice_region, dfprice_agg_region, axis=0).sort_index()
                dfprice_region.to_pickle(tmp_fpath) # save
                print(' - done')
        else:
            print(' -> no files to update')

    # demand
    if update_demand:
        print(f' -> Updating demand')
        dfdemand_hold = []

        file_names_diff = _get_diff_file_names(file_names, demand_fpath, regions, 'dispatchIS_reports')
        lenFN = len(file_names_diff)
        if lenFN>0:
            for ix, f in chunks(file_names_diff, enum=True):
                print(f" ->> {ix}/{lenFN} - {f}")
                tmpfn = base_fpath + f"/{f}"

                # get tables indexes - I=header, D=data rows
                dfidx = pd.read_csv(tmpfn, usecols=[0, 2], index_col=False, header=None)

                # price
                hidx = dfidx.loc[(dfidx[0]=='I') & (dfidx[2]=='REGIONSUM')].index[0]
                didx = dfidx.loc[(dfidx[0] == 'D') & (dfidx[2] == 'REGIONSUM')].index
                tmpdfd = pd.read_csv(tmpfn, skiprows=hidx, nrows=len(didx))

                # remove some cols
                # tmpdfp = tmpdfp.drop(columns=['I','DISPATCH', 'PRICE','5','RUNNO'])

                dfdemand_hold.append(tmpdfd)
                pass

            # agg and parse
            dfdemand_hold_agg = pd.concat(dfdemand_hold, axis=0)
            dfdemand_hold_agg['SETTLEMENTDATE'] = pd.to_datetime(dfdemand_hold_agg['SETTLEMENTDATE'])
            #dfdemand_hold_agg['update_datetime'] = dfdemand_hold_agg['SETTLEMENTDATE'] - pd.DateOffset(minutes=5)

            cols2f = ['LASTCHANGED','SETTLEMENTDATE']
            dfdemand_hold_agg = dfdemand_hold_agg[cols2f + [col for col in dfdemand_hold_agg.columns if col not in cols2f]]
            dfdemand_hold_agg = dfdemand_hold_agg.set_index('SETTLEMENTDATE', drop=False)
            dfdemand_hold_agg['LASTCHANGED'] = pd.to_datetime(dfdemand_hold_agg['LASTCHANGED'])

            # merge and save by region
            for ix, region in chunks(regions, enum=True):
                print(f' -> {region} - concat and saving data ', end='')
                mask = dfdemand_hold_agg['REGIONID']==region
                dfdemand_agg_region = dfdemand_hold_agg.loc[mask]

                tmp_fpath = demand_fpath.format(region)
                dfdemand_region = pd.read_pickle(tmp_fpath) if os.path.isfile(tmp_fpath) else pd.DataFrame()
                dfdemand_region = cc(dfdemand_region, dfdemand_agg_region, axis=0).sort_index()
                dfdemand_region.to_pickle(tmp_fpath) # save
                print(' - done')
        else:
            print(' -> no files to update')
    pass

########################################################################################################################

def parse_dispatch_scada_archive():
    """
    populates data from archive
    - add region from gen data df
    - move lastchanged + region columns to front
    """

    base_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data"
    save_fpath = base_fpath + r"\NEM\historical\dispatch_scada.pkl"

    # generator data
    gendata = pd.read_pickle(base_fpath + r"\NEM\generator_data.pkl")

    # list all zip files
    base_fpath_src = r'D:\nemdata\ARCHIVE\Dispatch_SCADA'
    file_names = {}
    file_names_l1 = os.listdir(base_fpath_src) [:] # layer1
    for f in file_names_l1:
        with zipfile.ZipFile(base_fpath_src + f"/{f}", 'r') as zip_ref:
            file_names[f] = zip_ref.namelist()
    all_file_names = pd.DataFrame(file_names).melt()
    all_file_names_lst = all_file_names['value'].to_list()

    # # check for missing dates
    # str_strip = 'PUBLIC_DISPATCHSCADA_.zip'
    # file_names_df = pd.Series(all_file_names_lst).to_frame('file_names')
    # file_names_df['datetime'] = pd.to_datetime(file_names_df['file_names'].str.strip(str_strip).str.split('_', expand=True)[0])
    # file_names_df['date'] = pd.to_datetime(file_names_df['datetime'].dt.date)
    # dr = pd.date_range(file_names_df['date'].min(), file_names_df['date'].max(), freq='D')
    # mask = file_names_df['date'].isin(dr)
    # file_names_diff = file_names_df.loc[~mask, 'file_names'].to_list()
    # if len(file_names_diff)>0:
    #     raise Exception

    print(f' -> Updating dispatch generator scada')
    dfhold = []

    file_names_diff = _get_diff_file_names(all_file_names_lst, save_fpath,  file_type='dispatch_scada')
    all_file_names_diff = all_file_names.loc[all_file_names['value'].isin(file_names_diff)].reset_index(drop=True)
    all_file_names_diff = all_file_names_diff.iloc[:].reset_index(drop=True)  # in chunks of 25k
    lenFN = len(all_file_names_diff)
    if lenFN > 0:
        for ix, row in all_file_names_diff.iterrows():
            dir = row['variable']  # 2nd zip
            f = row['value']  # csv file
            print(f" ->> {ix}/{lenFN} - {f}")

            csv_params = {'skiprows': 1, 'skipfooter': 1}
            tmpdf = read_inner_csv(base_fpath_src, dir, f, csv_params)

            # parse
            cols2p = ['LASTCHANGED', 'SETTLEMENTDATE','DUID','SCADAVALUE']
            if 'LASTCHANGED' not in tmpdf.columns:
                tmpdf['LASTCHANGED'] = tmpdf['SETTLEMENTDATE']
            tmpdf = tmpdf[cols2p]
            tmpdfpivot = pd.Series(tmpdf['SCADAVALUE'].values, index=tmpdf['DUID'])
            tmpdfpivot['LASTCHANGED'] = tmpdf['LASTCHANGED'].iloc[0]
            tmpdfpivot['SETTLEMENTDATE'] = tmpdf['SETTLEMENTDATE'].iloc[0]

            dfhold.append(tmpdfpivot)
            pass

        # to_datetime date cols
        # LASTCHANGED minus 5mins
        dfhold_agg = pd.concat(dfhold, axis=1)
        dfhold_agg = dfhold_agg.T
        dfhold_agg['LASTCHANGED'] = pd.to_datetime(dfhold_agg['LASTCHANGED'])
        dfhold_agg['SETTLEMENTDATE'] = pd.to_datetime(dfhold_agg['SETTLEMENTDATE'])
        dfhold_agg = dfhold_agg.set_index('SETTLEMENTDATE', drop=False)
        dfhold_agg['SCADA_units'] = 'MW'
        cols2f = ['LASTCHANGED', 'SETTLEMENTDATE','SCADA_units']
        dfhold_agg = dfhold_agg[cols2f + [col for col in dfhold_agg.columns if col not in cols2f]]

        # last changed
        mask = dfhold_agg['LASTCHANGED']==dfhold_agg['SETTLEMENTDATE']
        if mask.any():
            dfhold_agg.loc[mask, 'LASTCHANGED'] = dfhold_agg.loc[mask, 'SETTLEMENTDATE'] - pd.DateOffset(minutes=5)

        # save
        print(f' -> concat and saving data ', end='')
        dfscada = pd.read_pickle(save_fpath) if os.path.isfile(save_fpath) else pd.DataFrame()
        dfscada = cc(dfscada, dfhold_agg, axis=0).sort_index()
        dfscada.to_pickle(save_fpath)  # save
        print(' - done')

    else:
        print(' -> no files to update')




    pass

########################################################################################################################

if __name__ == '__main__':
    # parse_dispatch_scada_archive()
    # parse_dispatchIS_reports_current() ### Don't use, use archive instead
    #parse_dispatchIS_reports_archive()
    parse_dispatchIS_reports_deeparchive()
    pass