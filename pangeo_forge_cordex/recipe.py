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


def target_chunks(dset, url=None, ssl=None):
    """Estimate chunksize for time

    Estimate time chunksize from the size of the
    first timestep in the dataset and the total number
    of timesteps defined by the frequency, datetime_start
    and datetime_stop.

    """
    import fsspec
    import xarray as xr

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


def create_recipe_inputs(responses, ssl=None):
    pattern_kwargs = {}
    if ssl:
        pattern_kwargs["fsspec_open_kwargs"] = {"ssl": ssl}
    inputs = {}
    for k, v in responses.items():
        inputs[k] = {}
        urls = v["urls"]["netcdf"]
        recipe_kwargs = {}

        recipe_kwargs["target_chunks"] = target_chunks(v, urls[0], ssl)
        inputs[k]["urls"] = urls
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
