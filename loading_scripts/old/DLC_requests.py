import requests
import pandas as pd
import argparse

ATOTI_SERVER = "http://localhost:10010/mr-application"
INPUT_FILE = 'dlc_operations.csv'
DLC_ENDPOINT = '/connectors/rest/dlc/v1/execute'

user = 'admin'
password = 'admin'


def execute_dlc_requests(server_url, requests_input_file):
    url = server_url + DLC_ENDPOINT
    df = pd.read_csv(requests_input_file)
    df = df[df['status'] == 'Starting']
    request_data = df[['operation', 'topics']]
    rows_as_json = request_data.to_dict(orient='records')
    for json_data in rows_as_json:
        json_data['topics'] = [json_data['topics']]
        response = requests.post(url, json=json_data, auth=(user, password))
        print(f"Sent DLC request: {json_data}")
        if response.status_code == 200:
            print(f"  Response: {response.json()}")
        else:
            print(f"  Error: Status code {response.status_code}, Response: {response.text}")
    print("All DLC requests processed.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="send DLC requests to Atoti server from Csv file")

    parser.add_argument("-s", "--server", default=ATOTI_SERVER, help="Atoti server URL.")
    parser.add_argument("-i", "--input", default=INPUT_FILE, help="csv input file")
    args = parser.parse_args()

    execute_dlc_requests(args.server, args.input)
