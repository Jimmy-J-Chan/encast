from conf.config import *
import requests


if __name__ == '__main__':
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(r'https://open-meteo.com/en/docs/historical-weather-api?#hourly_parameter_definition', headers=headers)
    dfs = pd.read_html(response.text)
    wvars = [df for df in dfs if ('Valid time' in df.columns)]
    wvars = [df for df in wvars if ('Preceding hour sum' in df['Valid time'].values)][0]

    # split some Variable names
    vars2split = [c for c in wvars['Variable'] if len(c.split(' '))>1]
    for v in vars2split:
        mask = wvars['Variable']==v
        tmpdf = wvars.loc[mask]

        new_vs = v.split(' ')
        tmpdf2 = tmpdf.loc[[tmpdf.index[0]]*len(new_vs)]
        tmpdf2['Variable'] = new_vs
        wvars = cc(wvars, tmpdf2, axis=0)

    wvars = wvars.loc[~wvars['Variable'].isin(vars2split)]
    wvars = wvars.sort_index().reset_index(drop=True)
    wvars.to_pickle(r'C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data\open_meteo\weather_variable_definitions.pkl')
    pass