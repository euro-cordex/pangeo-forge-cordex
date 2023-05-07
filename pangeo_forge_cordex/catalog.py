from os import path as op

from .parsing import project_from_iid

cordex_cmip5_facets = [
    "project_id",
    "product",
    "CORDEX_domain",
    "institute_id",
    "driving_model_id",
    "experiment_id",
    "member",
    "model_id",
    "rcm_version_id",
    "frequency",
    "variable_id",
    "version",
]

catalog_facets = {
    "CORDEX": cordex_cmip5_facets,
    "CORDEX-Reklies": cordex_cmip5_facets,
    "CORDEX-Adjust": None,
    "CORDEX-ESD": None,
}


def facets_from_iid(iid, facets=None):
    """get catalog attributes from iid"""
    if facets is None:
        project = project_from_iid(iid)
        facets = catalog_facets[project]
    attrs = iid.split(".")
    return dict(zip(facets, attrs))


def path(iid, project=None):
    if project is None:
        project = project_from_iid(iid)
        facets = catalog_facets[project]
    else:
        facets = None
    attrs = facets_from_iid(iid, facets)
    return op.join(*[attrs[k] for k in facets])


def catalog_entry(iids, df=None):
    import pandas as pd

    if isinstance(iids, str):
        iids = [iids]
    attrs = {}
    for i, iid in enumerate(iids):
        attrs[i] = facets_from_iid(iid)

    rows = pd.DataFrame.from_dict(attrs, orient="index")
    if df is not None:
        cat = pd.concat([df, rows], ignore_index=True)
        if cat.duplicated().any():
            duplicates = cat.where(cat.duplicated()).dropna()
            raise Exception(f"Found duplicates: {duplicates}")
    return rows
