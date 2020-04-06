"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)

KSQL_URL = "http://192.168.68.109:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

# docker run -it confluentinc/cp-ksql-cli http://192.168.68.109:8088

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INTEGER
) WITH (
    KAFKA_TOPIC='com.udacity.dsnano.ghartner.project1.turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
 AS
    select station_id,station_name,COUNT(station_id) AS COUNT from turnstile group by (station_id,station_name);
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    #if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
    #    return

    logging.info("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
