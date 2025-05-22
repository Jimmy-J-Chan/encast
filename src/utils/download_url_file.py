import requests

def download_url_file(url=None, fn_save=None):
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open a local file in binary write mode
        with open(fn_save, 'wb') as file:
            # Write the content of the response to the file
            file.write(response.content)
    # else:
    #     print('Failed to download the file. Status code:', response.status_code)
    return response.status_code