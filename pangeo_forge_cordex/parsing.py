import re

import requests

# 'cordex.output.EUR-11.DMI.ECMWF-ERAINT.evaluation.r1i1p1.HIRHAM5.v1.mon.tas'
known_projects = [
    "CORDEX",  # Usual Cordex datasets from CMIP5 downscaling
    "CORDEX-Reklies",  # This is Downscaling from Reklies project
    "CORDEX-Adjust",  # Bias adjusted output
    "CORDEX-ESD",  # Statistical downscaling
    "CORDEX-FPSCONV",  # FPS convection
]


cordex_template = "project.product.domain.institute.driving_model.experiment.ensemble.rcm_name.rcm_version.time_frequency.variable.version"
cordex_adjust_template = "project.product.domain.institute.driving_model.experiment.ensemble.rcm_name.bias_adjustment.time_frequency.variable.version"

base_params = {
    # "type": "File",
    "format": "application/solr+json",
    # "fields": "instance_id",
    "fields": "url,size,table_id,title,instance_id,replica,data_node,frequency,time_frequency,version,size,datetime_start,datetime_stop",
    "latest": True,
    "distrib": True,
    "limit": 500,
}

cordex_params = base_params

request_params = {
    "CORDEX": cordex_params,
    "CORDEX-Reklies": cordex_params,
    "CORDEX-Adjust": cordex_params,
    "CORDEX-ESD": cordex_params,
}

id_templates = {
    "CORDEX": cordex_template,
    "CORDEX-Reklies": cordex_template,
    "CORDEX-Adjust": cordex_adjust_template,
    "CORDEX-ESD": cordex_template,
}


def ensure_project_str(project):
    """Ensure that the project string has right format

    This is mainly neccessary for CORDEX projects because the
    project facet in the dataset_id is lowercase while in the API
    search we have to use uppercase or a mixture of upper and lowercase.

    """
    for p in known_projects:
        if project.upper() == p.upper():
            return p
    return project


def project_from_iid(iid):
    """Get project information from first iid entry"""
    return ensure_project_str(iid.split(".")[0])


def facets_from_iid(iid, project=None):
    """Translates iid string to facet dict according to project naming scheme"""
    if project is None:
        # take project id from first iid entry by default
        project = project_from_iid(iid)
    iid = f"{project}." + ".".join(iid.split(".")[1:])
    iid_name_template = id_templates.get(project, cordex_template)
    # this does not work yet with CORDEX project
    # template = get_dataset_id_template(project)
    # facet_names = facets_from_template(template)
    facets = {}
    for name, value in zip(iid_name_template.split("."), iid.split(".")):
        if value != "*":
            facets[name] = value
    if project == "CORDEX-Reklies":
        # There is another problem with CORDEX-Reklies, e.g.
        # "cordex-reklies.output.EUR-11.GERICS.MIROC-MIROC5.historical.r1i1p1.REMO2015.v1.mon.tas"
        # The product="output" facet will give no result although the dataset_id clearly says it's "output".
        # However the API result is empty list, so the output facet has to be removed when CORDEX-Reklies is searched, hmmm...
        del facets["product"]
    return facets


def get_dataset_id_template(project, url=None):
    """Requests the dataset_id string template for an ESGF project"""
    if url is None:
        url = "https://esgf-node.llnl.gov/esg-search/search"
    params = {
        "project": project,
        "fields": "project,dataset_id_template_",
        "limit": 1,
        "format": "application/solr+json",
    }
    r = requests.get(url, params)
    return r.json()["response"]["docs"][0]["dataset_id_template_"][0]


def facets_from_template(template):
    """Parse the (dataset_id) string template into a list of (facet) keys"""
    regex = r"\((.*?)\)"
    return re.findall(regex, template)


def request_project_facets(project, url=None):
    template = get_dataset_id_template(project, url)
    return facets_from_template(template)


def request_from_facets(url, project, type="Dataset", **facets):
    params = request_params.get(project, cordex_params).copy()
    params.update(facets)
    params["project"] = project
    params["type"] = type
    return requests.get(url=url, params=params)


def instance_ids_from_request(json_dict):
    iids = [item["instance_id"] for item in json_dict["response"]["docs"]]
    uniqe_iids = list(set(iids))
    return uniqe_iids


def parse_facets(iid, project):
    facets = facets_from_iid(iid, project)
    # convert string to list if square brackets are found
    for k, v in facets.items():
        if "[" in v:
            v = (
                v.replace("[", "")
                .replace("]", "")
                .replace("'", "")
                .replace(" ", "")
                .split(",")
            )
        facets[k] = v
    return {k: v for k, v in facets.items() if v != "*" and k != "project"}


def request_instance_ids(iid, url=None, project=None, type="Dataset", **params):
    """Parse an instance id with wildcards

    Examples:
    'cordex.output.EUR-11.GERICS.ICHEC-EC-EARTH.*.*.REMO2015.*.mon.tas'
    'cordex-reklies.output.EUR-11.GERICS.*.historical.r1i1p1.REMO2015.v1.*.tas'
    'cordex-adjust.*.EUR-11.*.MPI-M-MPI-ESM-LR.rcp45.*.*.*.mon.tasAdjust'
    'cordex-esd.*.EUR-11.*.MPI-M-MPI-ESM-LR.historical.*.*.*.*.tas'

    """
    if url is None:
        url = "https://esgf-data.dkrz.de/esg-search/search"
    if project is None:
        # take project id from first iid entry by default
        project = ensure_project_str(iid.split(".")[0])

    facets_filtered = parse_facets(iid, project)
    params = facets_filtered | params

    resp = request_from_facets(url, project, type, **params)
    if resp.status_code != 200:
        print(f"Request [{resp.url}] failed with {resp.status_code}")
        return resp
    else:
        return resp.json()


def parse_instance_ids(iid, url=None, project=None, **params):
    """Parse an instance id with wildcards

    Examples:
    'cordex.output.EUR-11.GERICS.ICHEC-EC-EARTH.*.*.REMO2015.*.mon.tas'
    'cordex-reklies.output.EUR-11.GERICS.*.historical.r1i1p1.REMO2015.v1.*.tas'
    'cordex-adjust.*.EUR-11.*.MPI-M-MPI-ESM-LR.rcp45.*.*.*.mon.tasAdjust'
    'cordex-esd.*.EUR-11.*.MPI-M-MPI-ESM-LR.historical.*.*.*.*.tas'

    """
    resp = request_instance_ids(iid, url, project, **params)

    return instance_ids_from_request(resp)


def total_size_ids(iid, url=None, project=None, **params):
    resp = request_instance_ids(iid, url, project, **params)
    return sum([r["size"] for r in resp["response"]["docs"]])
