import pandas as pd
from conf.config import *
import requests

def get_raw_generator_data(file_path=None, fn_save=None, update=True):
    if update:
        # ExistingGeneration&NewDevs
        gens = pd.read_excel(file_path, sheet_name='ExistingGeneration&NewDevs', skiprows=1)
        gens = gens.dropna(subset=['Asset Type','Site Name','Fuel Type'])
        gens = gens.loc[(gens['Asset Type']=='Existing Plant') & (gens['DUID'].notnull())] # one duid can have more than one unit attached
        gens = gens.reset_index(drop=True)
        gens.to_pickle(fn_save)
    pass

def generate_search_phrase(search_terms=None):



    return search_phrase

def get_generator_coords(fn_gendata=None, update=False, update_diff=False):
    """
    using google geocoding api
    - neeed to get api key first and store into secrets folder
    """

    # load some data
    import json
    with open(config.project.folder + '/secrets/gmaps.json', "r") as f:
        api_key = json.load(f)['key']
    gendata = pd.read_pickle(fn_gendata)

    # query string
    query_str = 'https://maps.googleapis.com/maps/api/geocode/json?key={}&address={}'

    # collect geocoding data
    search_terms = ['Region','Site Name','Owner']
    for ix, row in gendata.iterrows():
        search_terms = row[search_terms]
        search_phrase = generate_search_phrase(search_terms)
        tmp = requests.get(query_str.format(api_key, search_phrase))
        # tmp = requests.get(query_str.format(api_key, "adelaide desalination plant"))
    pass

def get_generator_data():
    # flags
    update_raw_generator_data = False
    update_gendata_coords = False
    populate_gendata_coords = True

    # file paths
    fn_gendata = r'U:\Research\Projects\sef\encast\NEM\generators\generator_data.pkl' # qut - rdss
    fn_gendata = r"U:\NEM\generators\generator_data.pkl" # pc
    fn_gendata = r"C:\Users\Jimmy\OneDrive - Queensland University of Technology\Documents\encast\data\NEM\generator_data.pkl" # onedrive - pc
    fn_gendata_xl = r"C:\Users\Jimmy\OneDrive - Queensland University of Technology\Documents\encast\data\NEM\generator_data_coordinates.xlsx" # onedrive excel - pc
    fn_gendata = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data\NEM\generator_data.pkl" # onedrive - qut
    fn_gendata_xl = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data\NEM\generator_data_coordinates.xlsx" # onedrive excel - qut

    if update_raw_generator_data:
        # get raw generator data from AEMO
        # https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information
        # Get latest excel file url - NEM Generation information publications
        raw_file_path = r"U:\Research\Projects\sef\encast\NEM\generators\NEM-Generation-Information-April-2025.xlsx"
        raw_file_path = r"U:\NEM\generators\NEM-Generation-Information-April-2025.xlsx"
        get_raw_generator_data(raw_file_path, fn_gendata)

    if update_gendata_coords:
        # save to excel
        df = pd.read_pickle(fn_gendata)
        df['address_manual'] = None
        df.to_excel(fn_gendata_xl, index=False)
        # manually fill in addresses/coordinates of generators - copied straight from google maps

    if populate_gendata_coords:
        # some postprocessing
        df_coords = pd.read_excel(fn_gendata_xl)
        dfmap = df_coords.set_index(['Region','Site Name','Owner'])[['coordinates_manual','notes']].dropna(subset=['coordinates_manual'])
        dfmap = dfmap.loc[~dfmap.index.duplicated(keep='last')]
        tmpdf = df_coords.set_index(['Region','Site Name','Owner'], drop=False)
        tmpdf['coordinates'] = tmpdf.index.map(dfmap['coordinates_manual']).str.strip()
        tmpdf['notes'] = tmpdf.index.map(dfmap['notes'])

        # split into latitude, longitude
        tmpdf['latitude'] = tmpdf['coordinates'].str.split(',', expand=True)[0].str.strip().astype(float).round(2)
        tmpdf['longitude'] = tmpdf['coordinates'].str.split(',', expand=True)[1].str.strip().astype(float).round(2)

        # save
        tmpdf = tmpdf.reset_index(drop=True)
        tmpdf.to_pickle(fn_gendata)


        pass



    pass




if __name__ == '__main__':
    get_generator_data()
    pass