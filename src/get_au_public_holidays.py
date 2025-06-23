import pandas as pd

from conf.config import *
import holidays

"""
srcs:
https://github.com/govau/public-holidays-validation
https://holidays.readthedocs.io/en/latest/auto_gen_docs/australia/

need to cross check dates 
need to check all public holidays captured



To check:
https://www.fairwork.gov.au/employment-conditions/public-holidays/2025-public-holidays
- TAS: Royal Hobart Regatta/Recreation Day
- QLD: Royal Queensland Show - Monday for Moreton bay region, Wednesday for Brisbane

"""


def get_au_public_holidays():
    # get data
    states = ['NSW', 'QLD', 'VIC', 'SA', 'TAS']
    hds = pd.DataFrame()
    for state in states:
        tmp_hds = holidays.country_holidays(country='AU',subdiv =state, years=range(2010, 2026))
        tmp_hds = pd.Series({k:v for k,v in tmp_hds.items()}).to_frame(state)
        hds = cc(hds, tmp_hds, axis=1)
        pass

    # some parsing
    hds.index = pd.to_datetime(hds.index)
    hds = hds.sort_index()
    hds = hds[states]

    # save
    save_fpath = r'C:\Users\Jimmy\OneDrive - Queensland University of Technology\Documents\encast\data\public_holidays.pkl'
    hds.to_pickle(save_fpath)
    pass


if __name__ == '__main__':
    get_au_public_holidays()
    pass