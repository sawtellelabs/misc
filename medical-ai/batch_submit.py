"""
©2023 Sawtelle. All Rights Reserved. sawtellelabs.com
"""
import os
import sys
import argparse
import time
import json
import pathlib
import tempfile
import asyncio
import aiohttp
import aiofiles
import pandas as pd
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__file__)

ROOT_URL = "https://medical-ai.sawtellelabs.com"
TH_SEC = 120

async def job(row,authsession,session):
    task_name = row['task_name']
    nifti_file_path = row['nifti_file_path']
    output_folder = row['output_folder']
    
    get_presigned_s3_url = ROOT_URL+f"/{task_name}/api/v2/submit-job"
    check_status_url = ROOT_URL+f"/{task_name}/api/v2/check-job-status"
    usage_id = None
    input_url = None
    output_url = None

    # get presigned s3 url
    async with authsession.get(get_presigned_s3_url) as response:
        if response.status == 200:
            myjson = await response.json()
            start_time = time.time()
            usage_id = myjson['usage_id']
            post_url = myjson['url']
        else:
            msg = "unauthorized or not subscribed!"
            logger.error(msg)
            return {"message":msg}

    if usage_id is None:
        msg = "usage_id cannot be None!"
        logger.error(msg)
        return {"message":msg}

    logger.info(f'usage_id: {usage_id}, file: {nifti_file_path}')

    data = {
        "key": myjson['fields']['key'],
        "policy": myjson['fields']['policy'],
        "x-amz-algorithm": myjson['fields']['x-amz-algorithm'],
        "x-amz-credential": myjson['fields']['x-amz-credential'],
        "x-amz-date": myjson['fields']['x-amz-date'],
        "x-amz-signature": myjson['fields']['x-amz-signature'],
        "file": open(nifti_file_path, 'rb').read(),
    }

    async with session.post(post_url, data=data) as response:
        myout = await response.text()
        if response.status != 204:
            msg = "unable to download!"
            logger.error(msg)
            return {"message":msg}

    wait_time = 0

    while wait_time < TH_SEC:
        params = {'usage_id':usage_id}
        await asyncio.sleep(3)
        async with authsession.get(check_status_url,params=params) as response:
            status_dict = await response.json()
        wait_time = time.time()-start_time
        if "status" in status_dict.keys():
            logger.info(f'usage_id: {usage_id}, status: {status_dict["status"]}')
            if "task-success" == status_dict["status"]:
                input_url = status_dict["input_url"]
                output_url = status_dict["output_url"]
                break

    logger.info(f'usage_id: {usage_id}, downloding data {output_url}')

    if output_url is not None:

        async with session.get(output_url) as resp:
            basename = os.path.basename(output_url.split("?")[0]).split("/")[-1]
            target_file_path = os.path.join(output_folder,f"{usage_id}-{basename}")
            target_file_path = os.path.abspath(target_file_path)
            logger.info(f'downloding to {target_file_path}')
            if resp.status == 200:
                f = await aiofiles.open(target_file_path, mode='wb')
                await f.write(await resp.read())
                await f.close()
        return {'input_path':nifti_file_path,'output_path':target_file_path}

async def worker(queue, authsession, session, results):
    while True:
        row = await queue.get()
        results.append(await job(row, authsession, session))
        queue.task_done()

async def main(task_name,api_token,input_folder,output_folder):

    # prepare files to process
    rows = []
    for x in pathlib.Path(input_folder).rglob("*.nii.gz"):
        rows.append(dict(
            task_name=task_name,
            nifti_file_path=str(x),
            output_folder=output_folder,
        ))

    # setup async workers and tasks
    N_WORKERS = 3

    queue = asyncio.Queue(N_WORKERS)
    results = []

    headers = {"Authorization": f"Bearer {api_token}"}
    async with aiohttp.ClientSession(headers=headers) as authsession, \
        aiohttp.ClientSession() as session:

        workers = [asyncio.create_task(worker(queue, authsession, session, results)) for _ in range(N_WORKERS)]

        # TODO: this is blocking, need to update to async.
        for row in rows:
            await queue.put(row)

        # wait for jobs to complete
        await queue.join()

        for tmpw in workers:
            tmpw.cancel()

    df=pd.DataFrame(results)
    df.to_csv(os.path.join(output_folder,"results.csv"),index=False)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='...')
    parser.add_argument('task_name',
        choices=["mr-prostate-seg","ct-scan-body-part-detection"],
        type=str,help='')
    parser.add_argument('api_token',type=str,help='api-token obtained from medical-ai.sawtellelabs.com .')
    parser.add_argument('input_folder',type=str,help='input folder path containing .nii.gz files.')
    parser.add_argument('output_folder',type=str,help='output folder path which results will be stored at.')
    args = parser.parse_args()
    
    os.makedirs(args.output_folder,exist_ok=True)

    results = asyncio.run(
        main(args.task_name,args.api_token,args.input_folder,args.output_folder)
    )
    logger.info("done")

"""

# usage:

export taskname=
export apitoken=
export inputfolder=
export outputfolder=

python3 batch_submit.py $taskname $apitoken $inputfolder $outputfolder


"""

