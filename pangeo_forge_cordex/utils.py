import fsspec
import pandas as pd
import xarray as xr

freq_map = {"mon": "M", "day": "D", "6hr": "6H", "3hr": "3H", "1hr": "1H"}


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
            print(f"responses for dataset {dset_id} and files not consistent!")
        dset_info[dset_id]["urls"] = files_by_id[files_id[0]]
    return dset_info


def parse_dataset_response(response):
    dsets = response.json()["response"]["docs"]
    ndsets = len(dsets)
    print(f"Found {ndsets} dataset(s)!")
    return {dset["instance_id"]: dset for dset in dsets}
