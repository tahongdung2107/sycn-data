import requests
import json
import time
import hmac
import hashlib

def call_crm_api(path: str, data: dict):
    project_token = '6764d00a-a80f-4114-8978-f743705e1533'
    api_secret = 'c6e7fe4396471feb99ee4a4d070b972e5e8584a2'
    access_key = '0xdo6hvke2675b94c800df246251c72cdde0a447wRgoS73O'

    timestamp = str(int(time.time() * 1000))  # ms
    message = timestamp + project_token
    signature = hmac.new(
        api_secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha512
    ).hexdigest()

    url = f'https://crm.bizfly.vn{path}'
    headers = {
        'cb-access-key': access_key,
        'cb-project-token': project_token,
        'cb-access-sign': signature,
        'cb-access-timestamp': timestamp,
        'Content-Type': 'application/json',
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    response.raise_for_status()
    return response.json()
