import asyncio
import datetime
from typing import List
from prefect import flow, task
import pymongo
        
@task(name="Get cars to update", log_prints=True, retries=2, retry_delay_seconds=20)
async def fetch_car_ids(source_id: int) -> List[str]:
    mongo_connstr = "mongodb://cdb-aggregator-dev:wBmpRirwukWkIJ4TvRCFD3MqgpSZrbhMJi8jvojNkvlNrnIEk8wh0p3WVx8WPOvuTy2oPfBqMeeLACDb6t83oQ==@cdb-aggregator-dev.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@cdb-aggregator-dev@"
    client = pymongo.MongoClient(mongo_connstr)
    db = client["auto-aggregator"]
    cars = db["cars"]
    utc_now = datetime.datetime.utcnow()
    date_from = utc_now - datetime.timedelta(1)
    items = cars.find({"SourceId": source_id, "CreatedAt": {"$gte": date_from}}, {"AdId": 1}).limit(20)
    return list(items)

@flow(name="Get cars to update", log_prints=True)
async def update_cars(source_id: int = 1, chunk_size: int = 10) -> List[str]:
    car_ids = await fetch_car_ids(source_id)
    print(len(car_ids))
    
if __name__ == "__main__":
    print("Started")
    av_by_source_id = 1
    asyncio.run(update_cars(av_by_source_id))
    print("Finished")