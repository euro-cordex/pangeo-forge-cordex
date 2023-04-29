import os
import ssl

import fsspec
import pandas as pd
import requests
import xarray as xr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pyesgf.logon import LogonManager

freq_map = {"mon": "M", "day": "D", "6hr": "6H", "3hr": "3H", "1hr": "1H"}


def logon():
    lm = LogonManager(verify=True)
    if not lm.is_logged_on():
        myproxy_host = "esgf-data.dkrz.de"
        # if we find those in environment, use them.
        if "ESGF_USER" in os.environ and "ESGF_PASSWORD" in os.environ:
            lm.logon(
                hostname=myproxy_host,
                username=os.environ["ESGF_USER"],
                password=os.environ["ESGF_PASSWORD"],
                interactive=False,
                bootstrap=True,
            )
        else:
            lm.logon(
                hostname=myproxy_host,
                interactive=True,
                bootstrap=True,
            )

    print(f"logged on: {lm.is_logged_on()}")

    # create SSL context
    sslcontext = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    sslcontext.load_verify_locations(capath=lm.esgf_certs_dir)
    sslcontext.load_cert_chain(lm.esgf_credentials)
    return sslcontext


def create_recipe(urls, recipe_kwargs=None, pattern_kwargs=None):
    if recipe_kwargs is None:
        recipe_kwargs = {}
    if pattern_kwargs is None:
        pattern_kwargs = {}
    pattern = pattern_from_file_sequence(urls, "time", **pattern_kwargs)
    if urls is not None:
        return XarrayZarrRecipe(
            pattern, xarray_concat_kwargs={"join": "exact"}, **recipe_kwargs
        )


def parse_urls(response):
    types = {}
    for r in response:
        url_type = r.split("|")[1]
        if "opendap" in url_type:
            types["opendap"] = r.split("|")[0][0:-5]
        elif "netcdf" in url_type:
            types["netcdf"] = r.split("|")[0]
    return types
    # return {r.split("|")[1]: r.split("|")[0] for r in response}


def sort_files_by_dataset_id(response):
    files = response.json()["response"]["docs"]
    # return files
    # result = dict.fromkeys([f['dataset_id'] for f in files], {})
    result = {f["dataset_id"]: {} for f in files}
    for f in files:
        id = f["dataset_id"]
        # print(f["size"])
        # result[id]["size"] += f["size"]
        urls = parse_urls(f["url"])
        for url_type, url in urls.items():
            if url_type in result[id].keys():
                result[id][url_type].append(url)
            else:
                result[id][url_type] = [url]
        # result[id].update(urls)
    return result


def number_of_timesteps(dset):
    start = dset["datetime_start"]
    stop = dset["datetime_stop"]
    cf_freq = dset["time_frequency"][0]
    ntime = pd.date_range(start, stop, freq=freq_map[cf_freq]).size
    print(f"Found {ntime} timesteps!")
    return ntime


def time_chunksize(ntime, size):
    chunksize_optimal = 100e6
    return max(int(ntime * chunksize_optimal / size), 1)


def target_chunks(dset, url=None, ssl=None):
    ntime = number_of_timesteps(dset)
    var = dset["variable"][0]
    print(url)
    if url:
        fs = fsspec.filesystem("https")
        with xr.open_dataset(fs.open(url, ssl=ssl)) as ds:
            size = ds[var].isel(time=0).nbytes * ntime
            # size = ds.nbytes / ds.time.size * ntime
            print(f"Estimated size: {size/1.e6} MB")
    else:
        size = dset["size"]
    # print(f"Estimated size: {size/1.e6} MB")
    return {"time": time_chunksize(ntime, size)}


def combine_response(dset_info, files_by_id):
    file_ids = list(files_by_id.keys())
    # dset_combine = dset_info.copy()
    for dset_id in dset_info.keys():
        files_id = [file_id for file_id in file_ids if dset_id in file_id]
        if len(files_id) != 1:
            print("responses not for dataset and files not consistent!")
        dset_info[dset_id]["urls"] = files_by_id[files_id[0]]
    return dset_info


def parse_dataset_response(response):
    dsets = response.json()["response"]["docs"]
    ndsets = len(dsets)
    print(f"Found {ndsets} dataset(s)!")
    return {dset["master_id"]: dset for dset in dsets}


def request(
    url="https://esgf-node.llnl.gov/esg-search/search",
    project="CORDEX",
    type="File",
    **search,
):
    params = dict(project=project, type=type, format="application/solr+json", limit=500)
    params.update(search)
    return requests.get(url, params)


def esgf_search(
    url="https://esgf-node.llnl.gov/esg-search/search",
    files_type="OPENDAP",
    project="CORDEX",
    **search,
):
    response = request(url, project, "Dataset", **search)
    dset_info = parse_dataset_response(response)
    response = request(url, project, "File", **search)
    files_by_id = sort_files_by_dataset_id(response)
    responses = combine_response(dset_info, files_by_id)
    return responses


def create_recipe_inputs(response, ssl=None):
    pattern_kwargs = {}
    if ssl:
        pattern_kwargs["fsspec_open_kwargs"] = {"ssl": ssl}
    inputs = {}
    for k, v in response.items():
        inputs[k] = {}
        urls = v["urls"]["netcdf"]
        recipe_kwargs = {}

        recipe_kwargs["target_chunks"] = target_chunks(v, urls[0], ssl)
        inputs[k]["urls"] = urls
        inputs[k]["recipe_kwargs"] = recipe_kwargs
        inputs[k]["pattern_kwargs"] = pattern_kwargs
    return inputs
