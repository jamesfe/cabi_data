from elasticsearch import Elasticsearch, helpers
import json

DOC_TYPE = "rides"

CABI_INDEX = "cabi_data_1413577666"


def test():
    """
    test
    :return:
    """
    es = Elasticsearch([{"host": "localhost"}])

    query = {"query": {"match_all": {}}}

    scan_help = helpers.scan(client=es, query=query,
                             scroll="10m", index="cabi_data_1413577666",
                             doc_type="rides", timeout="10m")

    count = 0

    for k in scan_help:
        count += 1
        print k

    print count


def return_distinct_stations():
    """
    just get all the distinct stations, return them and their names in d3 node
    format
    :return:
    """
    es = Elasticsearch([{"host": "localhost"}])

    query = {
        "aggs": {
            "start_nums": {
                "terms": {"field": "start_stn_num"}
            },
            "end_nums": {
                "terms": {"field": "fin_stn_num"}
            }
        },
        "_source": "False"
    }

    link_results = es.search(index=CABI_INDEX,
                             doc_type=DOC_TYPE,
                             body=query)
    fin_nums = link_results['aggregations']['end_nums']['buckets']
    start_nums = link_results['aggregations']['start_nums']['buckets']
    fin_set = set([_['key'] for _ in fin_nums])
    start_set = set([_['key'] for _ in start_nums])

    final_set = fin_set.union(start_set)

    ret_dict = dict({"nodes": list()})
    for k in final_set:
        ret_dict['nodes'].append(dict({"name": k}))

    return json.dumps(ret_dict)


def return_trip_volume_numtrips():
    """
    test some form of aggregation
    :return:
    """
    es = Elasticsearch([{"host": "localhost"}])

    query = {
        "aggs": {
            "ut": {
                "terms": {"field": "from_to_quick",
                          "size": 0}
            }
        },
        "_source": False
    }

    link_results = es.search(index=CABI_INDEX,
                             doc_type=DOC_TYPE,
                             body=query)

    tot_results = list()
    for link in link_results['aggregations']['ut']['buckets']:
        res = dict()
        # print link
        key = [int(_) for _ in link['key'].split("_")]
        res['start'] = key[0]
        res['finish'] = key[1]
        res['value'] = link['doc_count']
        if res['value'] > 0:
            tot_results.append(res)
    links = {"links": tot_results}
    return json.dumps(links)
    # , sort_keys=True,
    # indent=4, separators=(',', ': '))
    # for pretty printing

if __name__ == '__main__':
    # test()
    print return_distinct_stations()
    print return_trip_volume_numtrips()
    #print return_trip_volume_numtrips()