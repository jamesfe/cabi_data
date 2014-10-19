"""
flask endpoint for something to do with this application?
"""

from flask import Flask
from elasticsearch import Elasticsearch, helpers

app = Flask(__name__)

@app.route('/')
def hello_world():
    """
    Test.
    :return:
    """
    return 'Hello World!'


@app.route("/gettrafficlinks/")
def get_traffic_links():
    """
    Pull links based on amount of traffic from one to another
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

    link_results = es.search(index="cabi_data_1413577666",
                             doc_type="rides",
                             body=query, _source="False")

    doc_count = 0

    for k in link_results['aggregations']['ut']['buckets']:
        print k
        doc_count += k['doc_count']
        print doc_count

if __name__ == '__main__':
    app.run()
