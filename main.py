import redis
import asyncio
import random # code commented out, but provides fake work random duration
import uuid
import datetime 
import time

def reserve_execution_capacity(rate_limit_bucket, decrement):
    
    redis_transaction = r.pipeline()

    # 0 set up quota if it doesn't already exist
    redis_transaction.set(name=rate_limit_bucket, value=300000000000, nx=True, ex=10)

    # 1 get current remaining quota
    redis_transaction.get(rate_limit_bucket)

    # 2 decrease quota by decrement scalar if capacity exists, or returns -1 to signal no capacity for this size request
    redis_transaction.evalsha(_REDIS_LIMIT_SCRIPT, 2, rate_limit_bucket, decrement)
#     redis_transaction.eval('''
# if tonumber(redis.call('GET', KEYS[1])) >= tonumber(KEYS[2]) then
#     return redis.call('DECRBY', KEYS[1], KEYS[2])
# else 
#     return -1
# end
#            ''', 2, rate_limit_bucket, decrement)

    # p.decr(rate_limit_bucket, decrement)
    
    # 3 get remaining expiration time
    redis_transaction.ttl(rate_limit_bucket)

    pipeline_result = redis_transaction.execute()
    
    if(bool(pipeline_result[0])):
        print(f'''{datetime.datetime.now().strftime('%H:%M:%S.%f')} - Quota refreshed for {rate_limit_bucket}''')

    # print(pipeline_result[2])

    results = {
        "ttl" : pipeline_result[3],
        "initial_quota" : pipeline_result[1],
        "remaining_quota" : pipeline_result[2]
    }

    return results

async def mock_cf_request(request_name, IsRateLimited=True, num_pages=5, queue_name='limit:openAI-<ptu_endpoint>:default'):
    # # sleep random between 1 and 5s
    # await asyncio.sleep(random.randint(1,3))

    async with asyncio.TaskGroup() as tg:
        # process each page
        for i in range(1, num_pages+1):
            tg.create_task(process_page(request_name, i, queue_name))

        # run
async def rate_limiter(queue_name, required_capacity):
    # block execution chain until available capacity
    dequeue = reserve_execution_capacity(rate_limit_bucket=queue_name, decrement=required_capacity)
    while int(dequeue['remaining_quota']) < 0:
        # print(f'Worker:{worker_name} sleeping for {dq['ttl']} seconds)')
        await asyncio.sleep(dequeue['ttl'])
        dequeue = reserve_execution_capacity(rate_limit_bucket=queue_name, decrement=required_capacity)

async def process_page(worker_name, page_num, queue_name):
    required_capacity = random.randint(1,5000)
    await rate_limiter(queue_name, required_capacity)
    # print(f'{datetime.datetime.now().strftime('%H:%M:%S.%f')} - API call for {worker_name} ( page {page_num} ( {required_capacity} tokens) ) started')
    
    # work takes fake amount of seconds between 1 and 5
    # await asyncio.sleep(random.randint(1,2))
    # print(f'API call for {worker_name} ( page {page_num} ) finished')


async def main():
    # many workers ( many documents )
    # await asyncio.gather(
    #     worker(worker_name='workerA'), # equivalent to hitting docqa cloudfunction API
    #     worker(worker_name='workerB'),
    #     worker(worker_name='workerC')
    # )
    global r
    global _REDIS_LIMIT_SCRIPT

    r = redis.Redis(host='localhost', decode_responses=True)    
    
    _REDIS_LIMIT_SCRIPT = r.script_load('''
if tonumber(redis.call('GET', KEYS[1])) >= tonumber(KEYS[2]) then
    return redis.call('DECRBY', KEYS[1], KEYS[2])
else 
    return -1
end
           ''')

    tStart = time.perf_counter_ns()
    # many workers, many queues/accounts
    # await asyncio.gather(
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu01', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu02', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu03', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu04', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu05', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu06', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu07', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu08', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu09', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu00', num_pages=200)),

    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu11', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu12', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu13', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu14', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu15', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu16', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu17', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu18', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu19', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu10', num_pages=200)),

    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu21', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu22', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu23', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu24', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu25', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu26', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu27', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu28', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu29', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu20', num_pages=200)),

    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu31', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu32', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu33', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu34', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu35', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu36', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu37', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu38', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu39', num_pages=200)),
    #     tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu30', num_pages=200)),
    # )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu01', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu02', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu03', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu04', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu05', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu06', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu07', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu08', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu09', num_pages=2000)),
        tg.create_task(mock_cf_request(request_name='Document1', queue_name=f'chr_ptu00', num_pages=2000)),

    tStop = time.perf_counter_ns()

    print(f"Elapsed Time: {(tStop-tStart)/1000000000}")
    # few workers, synchronous degradation
    # await asyncio.gather(
    #     worker(worker_name=uuid.uuid4())
    # )
asyncio.run(main())
# if >= 0 then run
# if < 0 then sleep TTL and try again
