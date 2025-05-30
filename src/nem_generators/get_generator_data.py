from conf.config import *
import requests

def get_raw_generator_data(file_path=None, fn_save=None, update=False):
    if update:
        # ExistingGeneration&NewDevs
        gens = pd.read_excel(file_path, sheet_name='ExistingGeneration&NewDevs', skiprows=1)
        gens = gens.dropna(subset=['Asset Type','Site Name','Fuel Type'])
        gens = gens.loc[(gens['Asset Type']=='Existing Plant') & (gens['DUID'].notnull())] # one duid can have more than one unit attached
        gens = gens.reset_index(drop=True)
        gens.to_pickle(fn_save)
    pass

def get_generator_coords(fn_save=None, update=False):
    gens = pd.read_pickle(fn_save)

    # region, site name,
    tmp = requests.get('https://maps.googleapis.com/maps/api/geocode/json?key={}&address=adelaide%20desalination%20plant')
    pass



def get_generator_data():
    # get raw generator data from AEMO
    # https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information
    # Get latest excel file url - NEM Generation information publications
    raw_file_path = r"U:\Research\Projects\sef\encast\NEM\generators\NEM-Generation-Information-April-2025.xlsx"
    fn_save = r'U:\Research\Projects\sef\encast\NEM\generators\generator_data.pkl'
    get_raw_generator_data(raw_file_path, fn_save, update=False)

    # get coordinates for each generator
    get_generator_coords(fn_save, update=True)




    pass



if __name__ == '__main__':
    get_generator_data()
    pass