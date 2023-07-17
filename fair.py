import json
import os
import time
from typing import List

import requests

SERVER_ADRESS="http://faircompute.com:8000/api/v1"
#SERVER_ADRESS="http://localhost:8000/api/v1"
DOCKER_IMAGE="faircompute/stable-diffusion:pytorch-1.13.1-cu116"
#DOCKER_IMAGE="sha256:e06453fe869556ea3e63572a935aed4261337b261fdf7bda370472b0587409a9"

def authenticate(email: str, password: str):
    url = f'{SERVER_ADRESS}/auth/login'
    json_obj = {"email": email, "password": password}
    resp = requests.post(url, json=json_obj)
    token = resp.json()["token"]
    return token

def get(url, token, **kwargs):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers, **kwargs)

    if not response.ok:
        raise Exception(f"Error! status: {response.status_code}")
    return response


def put(url, token, data):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    if not isinstance(data, str):
        data = json.dumps(data)
    response = requests.put(url, headers=headers, data=data)

    if not response.ok and response.status_code != 206:
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


def put_job_stream_data(token, job_id, name, data):
    url = f"{SERVER_ADRESS}/jobs/{job_id}/data/streams/{name}"
    response = put(url=url, token=token, data=data)

    return response.text


def put_job_stream_eof(token, job_id, name):
    url = f"{SERVER_ADRESS}/jobs/{job_id}/data/streams/{name}/eof"

    response = put(url=url, token=token, data=None)

    return response.text


def wait_for_file(token, job_id, path, local_path, attempts=10):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    for i in range(attempts):
        url = f"{SERVER_ADRESS}/jobs/{job_id}/data/files/{path}"
        print(f"Waiting for file {path}...")
        try:
            with requests.get(url=url, headers=headers, stream=True) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f"File {local_path} ready")
                return local_path
        except Exception as e:
            print(e)
            time.sleep(0.5)

    print(f"Failed to receive {local_path}")


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

    res = put_job_stream_data(token=token,
                        job_id=job_id,
                        name="stdin",
                        data="Robot dinozaur\n")
    print(res)

    res = put_job_stream_eof(token=token,
                       job_id=job_id,
                       name="stdin")
    print(res)

    status = get_job_status(token=token,
                            job_id=job_id)
    print(status)

    while status == "Processing":
         status = get_job_status(token=token,
                                 job_id=job_id)
         print(status)
         time.sleep(0.5)
    if status == "Completed":
        print("Done!")
    else:
        print("Job Failed")
    resp = wait_for_file(token=token,
                         job_id=job_id,
                         path="%2Fworkspace%2Fresult.png",
                         local_path="result.png")
    print(resp)

