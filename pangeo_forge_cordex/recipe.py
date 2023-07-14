from .esgf_access import esgf_search
from .parsing import facets_from_iid
from .utils import freq_map


def number_of_timesteps(dset):
    import pandas as pd

    start = dset["datetime_start"]
    stop = dset["datetime_stop"]
    cf_freq = dset["time_frequency"][0]
    ntime = pd.date_range(start, stop, freq=freq_map[cf_freq]).size
    print(f"Found {ntime} timesteps!")
    return ntime


def time_chunksize(ntime, size):
    chunksize_optimal = 100e6
    return max(int(ntime * chunksize_optimal / size), 1)


def estimate_timestep_size(url, var):
    import xarray as xr

    with xr.open_dataset(url) as ds:
        return ds[var].isel(time=0).nbytes


def estimate_time_chunks(dset, urls=None, ssl=None):
    """Estimate chunksize for time

    Estimate time chunksize from the size of the
    first timestep in the dataset and the total number
    of timesteps defined by the frequency, datetime_start
    and datetime_stop.

    """
    import fsspec

    ntime = number_of_timesteps(dset)
    var = dset["variable"][0]

    # first try openda
    try:
        print("trying opendap...")
        url = urls["opendap"][0]
        size = estimate_timestep_size(url, var) * ntime
        print(f"Estimated size from opendap: {size/1.e6} MB")
        return {"time": time_chunksize(ntime, size)}
    except Exception as e:
        print(f"opendap access failed: {e}")
    try:
        print("trying https...")
        url = urls["netcdf"][0]
        fs = fsspec.filesystem("https")
        file = fs.open(url, ssl=ssl)
        size = estimate_timestep_size(file, var) * ntime
        print(f"Estimated size from https: {size/1.e6} MB")
        return {"time": time_chunksize(ntime, size)}
    except Exception as e:
        print(f"https access failed: {e}")
        size = dset["size"] * 10 * ntime
        print(f"Estimated size roughly: {size/1.e6} MB")
        return {"time": time_chunksize(ntime, size)}


def get_chunksizes(dset, ssl):
    chunks = estimate_time_chunks(dset, dset["urls"], ssl)
    return chunks


def create_recipe_inputs(responses, ssl=None):
    pattern_kwargs = {}
    if ssl:
        pattern_kwargs["fsspec_open_kwargs"] = {"ssl": ssl}
    inputs = {}
    for k, v in responses.items():
        print(f"creating recipe inputs for {k}")
        inputs[k] = {}
        recipe_kwargs = {}
        recipe_kwargs["target_chunks"] = get_chunksizes(v, ssl)
        inputs[k]["urls"] = v["urls"]["netcdf"]
        inputs[k]["recipe_kwargs"] = recipe_kwargs
        inputs[k]["pattern_kwargs"] = pattern_kwargs
    return inputs


def recipe_inputs_from_iids(iids, ssl=None):
    if not isinstance(iids, list):
        iids = [iids]
    dset_responses = {}
    for iid in iids:
        facets = facets_from_iid(iid)
        dset_responses.update(esgf_search(**facets))

    return create_recipe_inputs(dset_responses, ssl)


def create_recipe(urls, recipe_kwargs=None, pattern_kwargs=None):
    from pangeo_forge_recipes.patterns import pattern_from_file_sequence
    from pangeo_forge_recipes.recipes import XarrayZarrRecipe

    if recipe_kwargs is None:
        recipe_kwargs = {}
    if pattern_kwargs is None:
        pattern_kwargs = {}
    pattern = pattern_from_file_sequence(urls, "time", **pattern_kwargs)
    if urls is not None:
        return XarrayZarrRecipe(
            pattern, xarray_concat_kwargs={"join": "exact"}, **recipe_kwargs
        )
