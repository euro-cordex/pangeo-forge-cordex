import os
import ssl

import requests

from .utils import combine_response, parse_dataset_response, sort_files_by_dataset_id

host = "https://esgf-data.dkrz.de/esg-search/search"


def logon(host=None):
    from pyesgf.logon import LogonManager

    if host is None:
        host = "esgf-data.dkrz.de"
    lm = LogonManager(verify=True)
    if not lm.is_logged_on():
        # if we find those in environment, use them.
        if "ESGF_USER" in os.environ and "ESGF_PASSWORD" in os.environ:
            lm.logon(
                hostname=host,
                username=os.environ["ESGF_USER"],
                password=os.environ["ESGF_PASSWORD"],
                interactive=False,
                bootstrap=True,
            )
        else:
            lm.logon(
                hostname=host,
                interactive=True,
                bootstrap=True,
            )

    print(f"logged on: {lm.is_logged_on()}")

    # create SSL context
    sslcontext = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    sslcontext.load_verify_locations(capath=lm.esgf_certs_dir)
    sslcontext.load_cert_chain(lm.esgf_credentials)
    return sslcontext


def request(
    url=None,
    project="CORDEX",
    type="File",
    **search,
):
    if url is None:
        url = host
    version = search.get("version", None)
    if type == "File" and version:
        # this does not work for File searches since version denotes here rcm_version
        del search["version"]
    elif version and version.startswith("v"):
        search["version"] = version[1:]
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
    # return response.json()["response"]
    dset_info = parse_dataset_response(response)
    response = request(url, project, "File", **search)
    # return response.json()["response"]
    files_by_id = sort_files_by_dataset_id(response)
    responses = combine_response(dset_info, files_by_id)
    return responses
