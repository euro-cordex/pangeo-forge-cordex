from .esgf_access import esgf_search
from .parsing import facets_from_iid


def recipe_inputs_from_iids(iids, ssl=None):
    if not isinstance(iids, list):
        iids = [iids]
    dset_responses = {}
    for iid in iids:
        facets = facets_from_iid(iid)
        dset_responses.update(esgf_search(**facets))
    return dset_responses
