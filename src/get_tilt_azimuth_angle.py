import pandas as pd
from conf.config import *
import requests
from bs4 import BeautifulSoup
import time


def _profile_solar_au_yearly_optimal(save_fpath=None, update=False):
    if update:
        url = 'https://profilesolar.com/countries/AU/'
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)

        # Step 2: Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Step 3: Find the table
        table = soup.find('table')
        if not table:
            raise ValueError("No table found on the page.")

        # Step 4: Extract original headers
        original_headers = [th.get_text(strip=True) for th in table.find_all('th')]

        # Step 5: Modify headers to add Link columns
        extended_headers = []
        for col in original_headers:
            extended_headers.append(col)
            extended_headers.append(f"{col} Link")

        # Step 6: Extract rows
        rows = []
        for tr in table.find_all('tr')[1:]:  # Skip header
            cells = tr.find_all('td')
            row = []
            for cell in cells:
                text = cell.get_text(strip=True)
                link = ''
                a = cell.find('a')
                if a and a.has_attr('href'):
                    #link = 'https://profilesolar.com' + a['href']
                    link = a['href']
                row.append(text)
                row.append(link)
            rows.append(row)

        # Step 7: Create DataFrame
        df = pd.DataFrame(rows, columns=extended_headers)

        # parse
        df = df[[c for c in df.columns if (not c.endswith('Link')) | (c=='Location Link')]]
        df['avg_optimal_tilt_angle'] = df['Panel Tilt Angle'].str.split('°', expand=True)[0].str.strip()
        df['avg_optimal_azimuth'] = df['Panel Tilt Angle'].str.split('°', expand=True)[1].str.strip()
        df['avg_optimal_azimuth_angle'] = df['avg_optimal_azimuth'].map({'North':0, 'South':180})

        float_cols = ['Latitude', 'Longitude', 'kWh/day Summer', 'kWh/day Autumn', 'kWh/day Winter', 'kWh/day Spring',
                      'avg_optimal_tilt_angle', 'avg_optimal_azimuth_angle']
        df[float_cols] = df[float_cols].astype(float)

        # save
        df.to_pickle(save_fpath)
    else:
        df = pd.read_pickle(save_fpath)
    return df

def _profile_solar_au_seasonal_optimal(save_fpath=None, update=True):
    df = pd.read_pickle(save_fpath)
    df = df.set_index('Location Link', drop=False)

    seasonal_optimal = pd.DataFrame()
    for ix, row in df.iterrows():
        time.sleep(1)
        print(f" -> {ix}/{len(df)} - {row['Location']}")
        url = row['Location Link']
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises HTTPError if the request returned an unsuccessful status code

        # Step 2: Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        table = soup.find('table')

        # Extract headers
        headers = [th.get_text(strip=True) for th in table.find_all('th')]

        # Extract rows
        rows = []
        for tr in table.find_all('tr')[1:]:  # Skip the header row
            cells = [td.get_text(strip=True) for td in tr.find_all('td')]
            if cells:
                rows.append(cells)

        # Convert to DataFrame
        dfs = pd.DataFrame(rows, columns=headers, index=[url])

        # parse
        tmp = {}
        for season in ['summer', 'autumn', 'winter', 'spring']:
            col_name = [c for c in dfs.columns if season.capitalize() in c][0]
            tmp[f'{season}_optimal_tilt_angle'] = dfs[col_name].iloc[0].split('°')[0].strip()
            tmp[f'{season}_optimal_azimuth'] = dfs[col_name].iloc[0].split(' ')[1].strip()
            tmp[f'{season}_optimal_azimuth_angle'] = {'North':0, 'South':180}[tmp[f'{season}_optimal_azimuth']]
        tmpdf = pd.DataFrame(tmp, index=[url])
        seasonal_optimal = cc(seasonal_optimal, tmpdf, axis=0)

    # join and save
    df = cc(df, seasonal_optimal, axis=1).reset_index(drop=True)
    df.to_pickle(save_fpath)
    return df

def get_profile_solar_data(update=False):
    """
    - seasonal: Profile Solar - https://profilesolar.com/worldwidesolar/
    - https://profilesolar.com/countries/AU/
    -> need to interpolate this one
    -> only collect for Australia
    """

    # yearly average optimal
    fpath = r'C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data\optimal_tilt_azimuth_profilesolar.pkl'
    df = _profile_solar_au_yearly_optimal(save_fpath=fpath, update=update)

    # seasonal optimal
    df = _profile_solar_au_seasonal_optimal(save_fpath=fpath, update=update)

    pass





def get_global_solar_atlas_data(update=False):
    """
    using selenium
    """

    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    import time

    # Configure options
    chrome_options = Options()
    #chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    save_fpath = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data\optimal_tilt_azimuth_globalatlas.pkl"
    gen_coords = pd.read_pickle(r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\encast\data\NEM\generator_data.pkl")
    gen_coords_unique = gen_coords.drop_duplicates(subset=['coordinates'])

    atlasdf = pd.DataFrame() if ((not os.path.isfile(save_fpath)) | update) else pd.read_pickle(save_fpath)
    for ix, row in gen_coords_unique.iterrows():
        print(f" -> {ix}/{len(gen_coords_unique)}")

        latitude = row['latitude']
        longitude = row['longitude']
        coord_idx = f'{round(latitude, 2)},{round(longitude, 2)}'
        if coord_idx in atlasdf.index: continue
        try:
            # Open the Global Solar Atlas map at specific coordinates
            url = r"https://globalsolaratlas.info/detail?s={},{}"
            driver.get(url.format(latitude, longitude))

            # Wait until the 'Optimum tilt of PV modules' field is loaded AND populated
            wait = WebDriverWait(driver, 20)  # max 20 seconds wait

            # Wait for the specific field to appear and have non-empty content
            xpath_opta = "/html/body/gsa-app/div/div/main/ng-component/div/gsa-detail-site/gsa-detail-site-data/gsa-section/section/div[3]/div[1]/gsa-card[1]/div/gsa-site-data/mat-list/mat-list-item[6]/span/gsa-site-data-item/div/div[3]/sg-unit-value[1]/sg-unit-value-inner"
            tilt_element = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, xpath_opta)
                )
            )

            # Optionally, wait until it has non-empty text
            wait.until(lambda d: tilt_element.text.strip() != "")
            time.sleep(3)

            # build table
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            # Find the table under 'Map data'
            tmpdf = {}
            tmpdf['latitude_input'] = latitude
            tmpdf['longitude_input'] = longitude

            location_name = driver.find_element(By.XPATH, "/html/body/gsa-app/div/div/main/ng-component/div/gsa-detail-site/section[1]/gsa-selected-site/div/h3").text
            if '°' in location_name:
                tmpdf['location_coordinates'] = location_name
                tmpdf['location_latitude'] = tmpdf['location_coordinates'].split(',')[0][:-1]
                tmpdf['location_longitude'] = tmpdf['location_coordinates'].split(',')[1][:-1]
            else:
                tmpdf['location_name'] = driver.find_element(By.XPATH, "/html/body/gsa-app/div/div/main/ng-component/div/gsa-detail-site/section[1]/gsa-selected-site/div/h3").text
                tmpdf['location_coordinates'] = driver.find_element(By.XPATH, "/html/body/gsa-app/div/div/main/ng-component/div/gsa-detail-site/section[1]/gsa-selected-site/div/div[1]/sg-unit-toggle-value/span/span/sg-unit-value-inner").text
                tmpdf['location_latitude'] = tmpdf['location_coordinates'].split(',')[0][:-1]
                tmpdf['location_longitude'] = tmpdf['location_coordinates'].split(',')[1][:-1]
                tmpdf['location_address'] = driver.find_element(By.XPATH, "/html/body/gsa-app/div/div/main/ng-component/div/gsa-detail-site/section[1]/gsa-selected-site/div/div[2]").text

            rows = soup.find_all('mat-list-item')
            for r in rows:
                itms = r.find_all('div')
                itms = [c.text for c in itms[1:]]
                if itms[-1].endswith('arrow_drop_down'):
                    tmp = itms[-1][:-len('arrow_drop_down')]
                    itms.pop()
                    itms.append(tmp)

                # into df
                coln = itms[1]
                if coln=='OPTA':
                    tmpdf[f"{coln}_tilt_angle"] = itms[2].split('/')[0].strip()
                    tmpdf[f"{coln}_azimuth_angle"] = itms[2].split('/')[1].strip()
                    tmpdf[f"{coln}_desc"] = itms[0]
                    tmpdf[f"{coln}_unit"] = itms[3]
                else:
                    tmpdf[coln] = itms[2]
                    tmpdf[f"{coln}_desc"] = itms[0]
                    tmpdf[f"{coln}_unit"] = itms[3]

            tmpdf = pd.DataFrame(tmpdf, index=[coord_idx])
            atlasdf = cc(atlasdf, tmpdf, axis=0)

            # save
            atlasdf.to_pickle(save_fpath)
        except:
            print(f'Could not get data - {coord_idx}')

    driver.quit()
    pass


if __name__ == '__main__':
    """
    Get optimal tilt and azimuth angle from:
    - average: The Global Solar Atlas - https://globalsolaratlas.info/map
    - seasonal: Profile Solar - https://profilesolar.com/worldwidesolar/ 
        -> need to interpolate this one
        -> only collect for Australia 
    """

    get_profile_solar_data(update=False)
    get_global_solar_atlas_data(update=False)
    pass