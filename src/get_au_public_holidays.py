import warnings
warnings.filterwarnings("ignore")

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
- cross check with github val set

1) QLD: The Royal Queensland Show - Monday for some outer Brisbane regions, Wednesday for Brisbane
- 2010-2019, Monday/Wednesday split
- 2020, only 14/8/2020 for all regions, People’s long weekend
- 2021, only 29/10/2021 for all regions, People’s long weekend
- 2022-onwards, Monday/Wednesday split

2) need to add NSW bank holiday, 1st Monday in August
- Only banks and certain financial institutions receive the Bank Holiday

3) https://www.safework.sa.gov.au/__data/assets/pdf_file/0007/235474/Public-Holidays-since-2007.pdf
https://www.qld.gov.au/recreation/travel/holidays/public
QLD - part day: Christmas Eve 6pm – 12am Tuesday 24 December, 2019 onwards
SA  - part day: Christmas Eve 7pm – 12am Tuesday 24 December, 2012 onwards
SA  - part day: New Year Eve  7pm – 12am Tuesday 31 December, 2012 onwards


4) https://www.fairwork.gov.au/employment-conditions/public-holidays/2025-public-holidays
- TAS: Royal Hobart Regatta/Recreation Day
https://worksafe.tas.gov.au/topics/laws-and-compliance/public-holidays
TAS - Easter Tuesday - generally Tasmanian Public Service only
TAS - Royal Hobart Regatta - 2nd Monday Feb - South parts of TAS
TAS - Recreation Day - 1st Monday Nov - All parts of the state which do not observe Royal Hobart Regatta.

"""


def _add_public_holidays(df=None):
    new_hds = pd.DataFrame()

    ## group 1) - add monday public holiday for other bris regions,
    mask = (df['QLD']=='The Royal Queensland Show') & (~df.index.year.isin([2020,2021]))
    df1 = df.loc[mask]
    df1.index = df1.index - pd.DateOffset(days=2) # shift back to Monday
    assert all(df1.index.dayofweek==0)
    df1['QLD'] = 'The Royal Queensland Show - Outer Brisbane Regions'
    new_hds = cc(new_hds, df1, axis=0)

    ## group 2) Bank Holiday - 1st Monday of August
    dr = pd.date_range(f'{df.index.year.min()}-08-01', f'{df.index.year.max()}-08-01', freq='YS-AUG') + pd.DateOffset(weekday=0)
    df2 = pd.DataFrame(index=dr, columns=df.columns)
    df2['NSW'] = 'Bank Holiday'
    new_hds = cc(new_hds, df2, axis=0)

    ## group 3) part day public holidays QLD and SA
    # https://v6.nemreview.info/use/enjoy/data/datasets/glossary/index.aspx - timezone of data
    # NEM -> AEST (GMT+10), i.e QLD Time, no daylight savings
    # SA daylight savings - 5 Oct -> 5 Apr,
    # 30mins ahead of AEST during daylights savings
    dr = pd.date_range(f'2019-12-01', f'{df.index.year.max()}-12-01', freq='YS-DEC') + pd.DateOffset(day=24)
    df3_qld = pd.DataFrame(index=dr, columns=df.columns)
    df3_qld['QLD'] = 'Christmas Eve 6pm – 12am'

    dr = pd.date_range(f'2012-12-01', f'{df.index.year.max()}-12-01', freq='YS-DEC') + pd.DateOffset(day=24)
    df3_sa = pd.DataFrame(index=dr, columns=df.columns)
    df3_sa['SA'] = 'Christmas Eve 6:30pm – 11:30pm' # 'Christmas Eve 7pm – 12am'

    dr = pd.date_range(f'2012-12-01', f'{df.index.year.max()}-12-01', freq='YS-DEC') + pd.DateOffset(day=31)
    df3_sa2 = pd.DataFrame(index=dr, columns=df.columns)
    df3_sa2['SA'] = "New Year's Eve 6:30pm – 11:30pm" # "New Year's Eve 7pm – 12am"


    new_hds = cc(new_hds, df3_qld, df3_sa, df3_sa2, axis=0)




    # reshape df by state - dropna and cc

    return df


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

    # add additional public holidays - noted above
    hds = _add_public_holidays(hds)

    # save
    save_fpath = r'C:\Users\Jimmy\OneDrive - Queensland University of Technology\Documents\encast\data\public_holidays.pkl'
    hds.to_pickle(save_fpath)
    pass


if __name__ == '__main__':
    get_au_public_holidays()
    pass