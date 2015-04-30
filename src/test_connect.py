from elasticsearch import Elasticsearch, helpers
import json

DOC_TYPE = "rides"

CABI_INDEX = "cabi_data_1413577666"
# CABI_INDEX = "cabi_data_1413749232"


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
                "terms": {"field": "start_stn_num",
                          "size": 0}
            },
            "end_nums": {
                "terms": {"field": "fin_stn_num",
                          "size": 0}
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

    return json.dumps(ret_dict, sort_keys=True)


def return_trip_volume_numtrips(ref_array):
    """
    test some form of aggregation
    :ref_array: a json list that would be used for nodes
    :return:
    """

    lookup_list = dict()
    try:
        ref_list = json.loads(ref_array)['nodes']
        ref_list = [_['name'] for _ in ref_list]
        ref_list.sort()
        count = 0
        for k in ref_list:
            lookup_list[k] = count
            count += 1
    except:
        print "There was a big problem getting the list in."
        raise

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
        key = link['key'].split("_")
        res['source'] = lookup_list[key[0]]
        res['target'] = lookup_list[key[1]]
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
    ref_stn_list = return_distinct_stations()

    trips_list = return_trip_volume_numtrips(ref_stn_list)
    fin_pre_json = dict()

    fin_pre_json['links'] = json.loads(trips_list)['links']
    fin_pre_json['nodes'] = json.loads(ref_stn_list)['nodes']
    print json.dumps(fin_pre_json)
    #print return_trip_volume_numtrips()