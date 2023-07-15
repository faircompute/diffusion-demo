import json
import os
import time
from typing import List

import requests

#SERVER_ADRESS="http://faircompute.com:8000"
SERVER_ADRESS="http://localhost:8000/api/v1"
DOCKER_IMAGE="faircompute/stable-diffusion:pytorch-1.13.1-cu116"

def authenticate(email: str, password: str):
    url = f'{SERVER_ADRESS}/auth/login'
    json_obj = {"email": email, "password": password}
    resp = requests.post(url, json=json_obj)
    token = resp.json()["token"]
    return token

def get(url, token):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers)

    if not response.ok:
        raise Exception(f"Error! status: {response.status_code}")
    return response


def put(url, token, data):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    data = json.dumps(data)
    print(data)
    response = requests.put(url, headers=headers, data=data)

    if not response.ok:
        raise Exception(f"Error! status: {response.status_code}")
    return response


def put_program(token, launcher: str, image: str, runtime: str, command: List[str]):
    url = f"{SERVER_ADRESS}/programs"
    data = {
        launcher: {
            "image": image,
            "command": command,
            "runtime": runtime
        }
    }
    response = put(url=url, token=token, data=data)

    return int(response.text)


def put_job(token, program_id, input_files, output_files):
    url = f"{SERVER_ADRESS}/jobs?program={program_id}"
    data = {
        'input_files': input_files,
        'output_files': output_files
    }

    response = put(url=url, token=token, data=data)

    return int(response.text)


def get_job_status(token, job_id):
    url = f"{SERVER_ADRESS}/jobs/{job_id}/status"
    response = get(url=url, token=token)
    return response.text


def get_cluster_summary(token):
    url = f"{SERVER_ADRESS}/nodes/summary"

    response = get(token=token, url=url)

    return response.json()


def wait_for_file(token, job_id, path, attempts=10):
    for i in range(attempts):
        try:
            url = f"{SERVER_ADRESS}/jobs/{job_id}/data/files/{path}"

            response = get(url=url, token=token)

            if response.ok:
                break

        except Exception as e:
            pass

        print(f"Waiting for file {path}...")
        time.sleep(0.5)

    print(f"File {path} ready")


if __name__=="__main__":
    email = os.getenv('FAIRCOMPUTE_EMAIL')
    password = os.environ.get('FAIRCOMPUTE_PASSWORD')
    token = authenticate(email=email, password=password)

    print(token)

    summary = get_cluster_summary(token=token)
    print("Summary:")
    print(summary)
    program_id = put_program(token=token,
                             launcher="Docker",
                             image=DOCKER_IMAGE,
                             runtime="nvidia",
                             command=[])
    print(program_id)

    job_id = put_job(token=token,
                     program_id=program_id,
                     input_files=[],
                     output_files=["/workspace/result.png"])

    print(job_id)

    status = get_job_status(token=token,
                            job_id=job_id)
    print(status)
    while status != "Processing" and status != "Completed":
         status = get_job_status(token=token,
                                 job_id=job_id)
         print(status)
         time.sleep(0.5)

    print("Done!")
    resp = wait_for_file(token=token,
                         job_id=job_id,
                         path="/workspace/result.png")
    print(resp)

