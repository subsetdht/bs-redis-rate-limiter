import redis
import asyncio
import random # code commented out, but provides fake work random duration
import uuid
import datetime 

async def get_queue_capacity(queue_name='limit:textract:default', decrement=1):
    r = redis.Redis(host='localhost', decode_responses=True)
    p = r.pipeline()

    # 0 set up quota if it doesn't already exist
    p.set(name=queue_name, value=5, nx=True, ex=1)

    # 1 get current remaining quota
    p.get(queue_name)

    # 2 decrease quota by decrement scalar
    p.decr(queue_name, decrement)

    # 3 get decreased queue
    p.get(queue_name)

    # 4 get remaining expiration time
    p.ttl(queue_name)

    pipeline_result = p.execute()
    
    if(bool(pipeline_result[0])):
        print(f'{datetime.datetime.now().strftime('%H:%M:%S.%f')} - Quota refreshed for {queue_name}')

    results = {
        "ttl" : pipeline_result[4],
        "initial_quota" : pipeline_result[1],
        "remaining_quota" : pipeline_result[3]
    }

    return results

async def worker(worker_name, IsRateLimited=True, num_pages=5, queue_name='limit:textract:default'):
    # # sleep random between 1 and 5s
    # await asyncio.sleep(random.randint(1,3))

    async with asyncio.TaskGroup() as tg:


        # process each page
        for i in range(1, num_pages+1):
            tg.create_task(process_page(worker_name, i, queue_name))

        # run
async def rate_limiter(queue_name):
    # block execution chain until available capacity
    dequeue = await get_queue_capacity(queue_name=queue_name)
    while int(dequeue['remaining_quota']) < 0:
        # print(f'Worker:{worker_name} sleeping for {dq['ttl']} seconds)')
        await asyncio.sleep(dequeue['ttl'])
        dequeue = await get_queue_capacity(queue_name=queue_name)

async def process_page(worker_name, page_num, queue_name):
    await rate_limiter(queue_name)
    print(f'{datetime.datetime.now().strftime('%H:%M:%S.%f')} - API call for {worker_name} ( page {page_num} ) started')
    
    # work takes fake amount of seconds between 1 and 5
    # await asyncio.sleep(random.randint(1,5))
    # print(f'API call for {worker_name} ( page {page_num} ) finished')


async def main():
    # many workers ( many documents )
    # await asyncio.gather(
    #     worker(worker_name='workerA'), # equivalent to hitting docqa cloudfunction API
    #     worker(worker_name='workerB'),
    #     worker(worker_name='workerC')
    # )
    
    # many workers, many queues/accounts
    await asyncio.gather(
        worker(worker_name='WorkerA', queue_name=f'limit:textract:{uuid.uuid4()}', num_pages=20),
        worker(worker_name='WorkerB', queue_name=f'limit:textract:{uuid.uuid4()}')         
    )

    # few workers, synchronous degradation
    # await asyncio.gather(
    #     worker(worker_name=uuid.uuid4())
    # )
asyncio.run(main())
# if >= 0 then run
# if < 0 then sleep TTL and try again
