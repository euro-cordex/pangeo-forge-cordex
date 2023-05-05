from pangeo_forge_cordex import parse_instance_ids


def test_parsing():
    iids = parse_instance_ids(
        "cordex.output.EUR-11.GERICS.ECMWF-ERAINT.*.r1i1p1.*.v1.*.tas"
    )
    iids.sort()
    expected = [
        "cordex.output.EUR-11.GERICS.ECMWF-ERAINT.evaluation.r1i1p1.REMO2015.v1.day.tas.v20180813",
        "cordex.output.EUR-11.GERICS.ECMWF-ERAINT.evaluation.r1i1p1.REMO2015.v1.sem.tas.v20180813",
        "cordex.output.EUR-11.GERICS.ECMWF-ERAINT.evaluation.r1i1p1.REMO2015.v1.mon.tas.v20180813",
    ]
    expected.sort()
    assert iids == expected
