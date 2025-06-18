import pandas as pd
from conf.config import *
import requests
from copy import deepcopy


# https://archive-api.open-meteo.com/v1/archive?latitude=-27.4679,-33.8678&longitude=153.0281,151.2073&start_date=2024-01-01&end_date=2024-12-31
# &hourly=wind_speed_10m,cloud_cover,shortwave_radiation&models=ecmwf_ifs,era5&timezone=Australia%2FSydney


def _build_query_url(params):
    query_url = 'https://archive-api.open-meteo.com/v1/archive?'

    if 'coords' in params.keys:
        latitude = ','.join([c.split(',')[0].strip() for c in params['coords']])
        longitude = ','.join([c.split(',')[1].strip() for c in params['coords']])
        query_url = query_url + f'latitude={latitude}&longitude={longitude}'

    if 'start_date' in params.keys:
        query_url = query_url + f"&start_date={pd.to_datetime(params['start_date']):%Y-%m-%d}"

    if 'end_date' in params.keys:
        query_url = query_url + f"&end_date={pd.to_datetime(params['end_date']):%Y-%m-%d}"

    if 'hourly_vars' in params.keys:
        query_url = query_url + f"&hourly={','.join(params['hourly_vars'])}"

    if 'models' in params.keys:
        query_url = query_url + f"&models={','.join(params['models'])}"

    if 'timezone' in params.keys:
        query_url = query_url + f"&timezone={params['timezone'].strip().replace(' ', '%2F')}" # replace empty spaces

    return query_url

def get_data_from_url(url=None, coords=None, var_name=None):
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)

    df = pd.DataFrame()
    metadata = pd.DataFrame()
    if response.status_code==200:
        data = response.json()
        for ix, c in enumerate(coords):
            cdata = data[ix] if len(coords)>1 else data
            df0 = pd.DataFrame(cdata['hourly']).set_index('time')
            df0.index = pd.to_datetime(df0.index)
            df0.columns = [c]
            df = cc(df, df0, axis=1)

            md0 = {k:v  for k, v in cdata.items() if k not in ['hourly','hourly_units']}
            md0[var_name] = cdata['hourly_units'][var_name]
            metadata = cc(metadata, pd.Series(md0).to_frame(c).T, axis=0)

        print(' - success', end='')
    return df, metadata

def get_historical_ecmwf_ifs():
    # load data
    base_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data"
    gendata = pd.read_pickle(base_fpath + r"\NEM\generator_data.pkl")
    wvars = pd.read_pickle(base_fpath + r"\open_meteo\weather_variable_definitions.pkl")

    # prep data
    coords = gendata['coordinates'].drop_duplicates().to_list()
    vars2drop = ['pressure_msl','surface_pressure','snowfall','global_tilted_irradiance',
                 'sunshine_duration','et0_fao_evapotranspiration','snow_depth']
    vars2drop = vars2drop + [c for c in wvars['Variable'] if c.startswith('soil')]
    wvars2collect = [c for c in wvars['Variable'] if c not in vars2drop]

    # GTI: global_tilted_irradiance, ~ can be modelled using GHI, DHI and DNI
    # GHI: shortwave_radiation, ~ GHI = DHI + DNI * cos(solar zenith angle)
    # DHI: diffuse_radiation,
    # DNI: direct_normal_irradiance
    wvars2collect = ['shortwave_radiation', 'diffuse_radiation', 'direct_normal_irradiance'] #TODO: remove

    # params
    params = Struct()
    #params['coords'] = coords[:2]
    params['start_date'] = '2017-01-01' # ecmwf 26/12/2016 sdate
    params['end_date'] = '2024-12-31'
    #params['hourly_vars'] = wvars2collect[:2]
    # params['tilt'] = 30
    # params['azimuth'] = 180
    params['models'] = ['ecmwf_ifs']
    params['timezone'] = 'GMT' #'Australia%2FBrisbane' #'GMT'

    today = pd.Timestamp.today()
    varlen = len(wvars2collect)
    for ix, var in enumerate(wvars2collect):
        print(f' -> {ix}/{varlen} - Downloading - {var}')

        # load df
        var_fpath = base_fpath + f'/open_meteo/historical/ecmwf_ifs/{var}.pkl'
        var_data = pd.read_pickle(var_fpath) if os.path.isfile(var_fpath) else pd.DataFrame()
        var_md_fpath = base_fpath + f'/open_meteo/historical/ecmwf_ifs/{var}_metadata.pkl'
        var_md_data = pd.read_pickle(var_md_fpath) if os.path.isfile(var_md_fpath) else pd.DataFrame()

        # collect in batches
        tmpparams = deepcopy(params)
        tmpparams['hourly_vars'] = [var]
        chk_size = 5

        # check diffs
        coords_diff = [c for c in coords if c not in var_data.columns]
        if len(coords_diff)>0:
            chklen = int(len(coords_diff)/chk_size)
            for chkix, chk in chunks(coords_diff, chk_size, enum=True):
                print(f' ->> chunk - {chkix}/{chklen}', end='')
                tmpparams['coords'] = chk
                url = _build_query_url(tmpparams)
                df, metadata = get_data_from_url(url, chk, var)
                metadata['last_updated_BNE_datetime'] = today

                # concat
                if not df.empty:
                    var_data = cc(var_data, df, axis=1)
                    var_md_data = cc(var_md_data, metadata, axis=0)

                    # save
                    var_data.to_pickle(var_fpath)
                    var_md_data.to_pickle(var_md_fpath)
                    print(' - saved')
                else:
                    print(' - failed')

    pass

def update_historical_ecmwf_ifs():
    # load data
    base_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data"
    gendata = pd.read_pickle(base_fpath + r"\NEM\generator_data.pkl")
    wvars = pd.read_pickle(base_fpath + r"\open_meteo\weather_variable_definitions.pkl")

    # prep data
    coords = gendata['coordinates'].drop_duplicates().to_list()
    vars2drop = ['pressure_msl','surface_pressure','snowfall','global_tilted_irradiance',
                 'sunshine_duration','et0_fao_evapotranspiration','snow_depth']
    vars2drop = vars2drop + [c for c in wvars['Variable'] if c.startswith('soil')]
    wvars2collect = [c for c in wvars['Variable'] if c not in vars2drop]

    # params
    params = Struct()
    #params['coords'] = coords[:2]
    params['start_date'] = '2017-01-01' # ecmwf 26/12/2016 sdate
    params['end_date'] = '2024-12-31'
    #params['hourly_vars'] = wvars2collect[:2]
    # params['tilt'] = 30
    # params['azimuth'] = 180
    params['models'] = ['ecmwf_ifs']
    params['timezone'] = 'GMT' #'Australia%2FBrisbane' #'GMT'

    today = pd.Timestamp.today()
    varlen = len(wvars2collect)
    for ix, var in enumerate(wvars2collect):
        print(f' -> {ix}/{varlen} - Downloading - {var}')

        # load df
        var_fpath = base_fpath + f'/open_meteo/historical/ecmwf_ifs/{var}.pkl'
        var_data = pd.read_pickle(var_fpath) if os.path.isfile(var_fpath) else pd.DataFrame()
        var_md_fpath = base_fpath + f'/open_meteo/historical/ecmwf_ifs/{var}_metadata.pkl'
        var_md_data = pd.read_pickle(var_md_fpath) if os.path.isfile(var_md_fpath) else pd.DataFrame()

        # collect in batches
        tmpparams = deepcopy(params)
        tmpparams['hourly_vars'] = [var]
        chk_size = 50

        # update diff date
        lvi = min([var_data[c].last_valid_index() for c in var_data.columns])
        tmpparams['start_date'] = f"{lvi:%Y-%m-%d}"
        tmpparams['end_date'] = f"{today:%Y-%m-%d}"

        # extend df index
        new_idx = var_data.index.union(pd.date_range(lvi, today.today(), freq='h'))
        var_data = var_data.reindex(new_idx)

        chklen = int(len(coords)/chk_size)
        for chkix, chk in chunks(coords, chk_size, enum=True):
            print(f' ->> chunk - {chkix}/{chklen}', end='')
            tmpparams['coords'] = chk
            url = _build_query_url(tmpparams)
            df, metadata = get_data_from_url(url, chk, var)
            metadata['last_updated_BNE_datetime'] = today

            # update df
            var_data.update(df, overwrite=False) # only update missing values
            var_md_data.update(metadata, overwrite=True) # overwrite old metadata

            # save
            var_data.to_pickle(var_fpath)
            var_md_data.to_pickle(var_md_fpath)
            print(' - saved')

    pass




if __name__ == '__main__':
    get_historical_ecmwf_ifs() # use this to get full historical for new coords and vars
    #update_historical_ecmwf_ifs() # use this to update diff dates
    pass