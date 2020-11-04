import logging

import pybgpstream


collectors = [
#    "route-views.phoix",
    "route-views.amsix",
    "route-views.chicago",
    "route-views.chile",
    "route-views.eqix",
    "route-views.flix",
    "route-views.fortaleza",
    "route-views.gixa",
    "route-views.gorex",
    "route-views.isc",
    "route-views.jinx",
    "route-views.kixp",
    "route-views.linx",
    "route-views.napafrica",
    "route-views.nwax",
    "route-views.perth",
    "route-views.rio",
    "route-views.saopaulo",
    "route-views.sfmix",
    "route-views.sg",
    "route-views.soxrs",
    "route-views.sydney",
    "route-views.telxatl",
    "route-views.wide",
    "route-views2",
    "route-views2.saopaulo",
    "route-views3",
    "route-views4",
    "route-views6",
]
record_type = "ribs"


def logger(collector, record_type):
    # Setup loggin
    logging.basicConfig(
        filename="{}-{}.log".format(collector, record_type),
        level=logging.INFO,
        format="%(message)s",
    )

    stream = pybgpstream.BGPStream(
        from_time="2020-09-01 00:00:00",
        until_time="2020-09-01 02:00:00",
        record_type=record_type,
        collector=collector,
    )
    for rec in stream.records():
        for elem in rec:
            # print(elem.type, elem.peer_address, elem.peer_asn, elem.fields)
            logging.info(
                "{}|{}|{}|{}|{}|{}|{}".format(
                    elem.project,
                    elem.collector,
                    elem.time,
                    elem.type,
                    elem.peer_asn,
                    elem.peer_address,
                    elem.fields,
                )
            )


for col in collectors:
    logger(collector=col, record_type=record_type)