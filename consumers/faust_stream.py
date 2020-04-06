"""Defines trends calculations for stations"""
import logging

import faust
import time


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations3-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("com.udacity.dsnano.ghartner.project1.stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("com.udacity.dsnano.ghartner.project1.stationOrder",value_type=TransformedStation, partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(out_topic)
async def updateTable(transStations):
    logger.info("writing to table...")
    async for station in transStations:
        table[station.station_id] = station

@app.agent(topic)
async def transform(dbStations):
    logger.info("transforming input...")
    async for dbStation in dbStations:
        color = None
        
        if dbStation.red:
            color = "red"
        if dbStation.blue:
            color = "blue"
        if dbStation.green:
            color = "green"

        transformedStation = TransformedStation(
            station_id = dbStation.station_id,
            station_name = dbStation.station_name,
            order = dbStation.order,
            line = color
        )
        
        await out_topic.send(key=str(dbStation.station_id),
                       value=transformedStation)

if __name__ == "__main__":
    app.main()
