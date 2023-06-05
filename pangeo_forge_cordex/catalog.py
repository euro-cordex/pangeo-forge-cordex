from os import path as op

from .parsing import project_from_iid

# the facet names in the intake catalogs differ from those in the ESGF API
# since the intake facets represent actual datasets attributes.
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

cordex_adjust_facets = [
    "project_id",
    "product",
    "CORDEX_domain",
    "institute_id",
    "driving_model_id",
    "experiment_id",
    "member",
    "model_id",
    "bias_adjustment",
    "frequency",
    "variable_id",
    "version",
]

catalog_facets = {
    "CORDEX": cordex_cmip5_facets,
    "CORDEX-Reklies": cordex_cmip5_facets,
    "CORDEX-Adjust": cordex_adjust_facets,
    "CORDEX-ESD": cordex_cmip5_facets,
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


def get_url(iid, bucket, prefix="", fs="s3"):
    return f"{fs}://{op.join(bucket, prefix, path(iid))}"


def catalog_entry(iid, url, df=None):
    import pandas as pd

    attrs = facets_from_iid(iid)
    attrs["path"] = url

    rows = pd.DataFrame(attrs, index=[0])
    if df is not None:
        cat = pd.concat([df, rows], ignore_index=True)
        if cat.duplicated().any():
            duplicates = cat.where(cat.duplicated()).dropna()
            raise Exception(f"Found duplicates: {duplicates}")
    return rows
