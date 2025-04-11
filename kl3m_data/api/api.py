"""
FastAPI application for serving MLM training data using AsyncCacheLoader and DirectMLMLoader.
"""

# imports
import asyncio
import json
import random
import zlib
from contextlib import asynccontextmanager
from typing import Any, Dict, List

# packages
import valkey.asyncio as valkey
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from kl3m_data.logger import LOGGER


# project


class BatchRequest(BaseModel):
    batch_size: int = Field(
        ..., gt=0, le=16384, description="Number of records to fetch (1-16384)"
    )


class CacheStatus(BaseModel):
    sources: Dict[str, int] = Field(..., description="Source queue lengths")
    samples: Dict[str, int] = Field(..., description="Sample queue lengths")
    task_counts: Dict[str, int] = Field(..., description="Task queue lengths")


# Global variables
SEQUENCE_LENGTH = 512
CACHE_SIZE = 65536
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0


@asynccontextmanager
async def lifespan_handler(app_instance: FastAPI):
    """Context manager to handle the lifespan events of the FastAPI app

    Args:
        app_instance (FastAPI): FastAPI app instance

    Yields:
        None
    """
    # get the direct loader and cached loader on the app_instance

    try:
        # initialize the redis async pool
        connection_string = f"valkey://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
        app_instance.state.valkey_pool = valkey.ConnectionPool.from_url(
            connection_string
        )
    except Exception as e:
        LOGGER.error(f"Error initializing redis pool: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # get the list of valid datasets/prefixes
    try:
        client = valkey.Valkey.from_pool(app_instance.state.valkey_pool)
        app_instance.state.sample_queues = await client.keys("kl3m:samples:*")
    except Exception as e:
        LOGGER.error(f"Error fetching sample queues: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # continue with app startup
    yield

    # clean up after
    try:
        # close the redis pool
        await app_instance.state.valkey_pool.close()
    except Exception as e:
        LOGGER.error(f"Error closing redis pool: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


app = FastAPI(title="kl3m-data-api", version="0.1.0", lifespan=lifespan_handler)


async def get_source_queue_lengths(client: valkey.Valkey) -> dict[str, int]:
    """Get the lengths of the source queues.

    Args:
        client (valkey.Valkey): Redis client

    Returns:
        dict[str, int]: Dictionary of source queue lengths
    """
    return {
        queue.decode(): await client.llen(queue)
        for queue in await client.keys("kl3m:sources:*")
    }


async def get_sample_queue_lengths(client: valkey.Valkey) -> dict[str, int]:
    """Get the lengths of the sample queues.

    Args:
        client (valkey.Valkey): Redis client

    Returns:
        dict[str, int]: Dictionary of sample queue lengths
    """
    return {
        queue.decode(): await client.llen(queue)
        for queue in await client.keys("kl3m:samples:*")
    }


async def get_task_queue_length(client: valkey.Valkey, task: str) -> int:
    """Get the length of the task queue.

    Args:
        client (valkey.Valkey): Redis client
        task (str): Task name

    Returns:
        int: Length of the task queue
    """
    queues = await client.keys(f"kl3m:samples:{task}:*")
    lengths = await asyncio.gather(*(client.llen(queue) for queue in queues))
    return sum(lengths)


async def get_samples_uniform(
    valkey_client: valkey.Valkey, task: str, batch_size: int, min_datasets: int = 1
) -> List[Dict[str, Any]]:
    """Fetch samples uniformly from the Redis queue.

    Args:
        valkey_client (valkey.Valkey): Redis client
        task (str): Task name
        batch_size (int): Number of samples to fetch
        min_datasets (int): Minimum number of datasets to fetch from

    Returns:
        List[Dict[str, Any]]: List of samples
    """
    # divide the batch size between the number of valid sample queues
    sample_queues = await valkey_client.keys(f"kl3m:samples:{task}:*")
    num_queues = len(sample_queues)
    if num_queues == 0:
        LOGGER.error("No sample queues available")
        raise HTTPException(status_code=503, detail="No sample queues available")
    random.shuffle(sample_queues)

    # setup loop state and conditions
    samples = []
    remaining = batch_size
    unique_datasets = set()
    if batch_size < min_datasets:
        min_datasets = batch_size

    while len(samples) < batch_size or len(unique_datasets) < min_datasets:
        for queue in sample_queues:
            # fetch samples from the queue
            if random.random() < 0.5:
                records = await valkey_client.lpop(queue, 1)
            else:
                records = await valkey_client.rpop(queue, 1)

            if records is not None:
                unique_datasets.add(queue.decode())
                samples.extend(
                    [json.loads(zlib.decompress(record)) for record in records]
                )
                remaining -= len(records)

            if remaining <= 0:
                break

    random.shuffle(samples)
    return samples[:batch_size]


async def get_samples_weighted(
    valkey_client: valkey.Valkey, task: str, batch_size: int, weights: dict[str, float]
) -> List[Dict[str, Any]]:
    """Fetch samples weighted from the Redis queue.

    Args:
        valkey_client (valkey.Valkey): Redis client
        task (str): Task name
        batch_size (int): Number of samples to fetch
        weights (dict[str, float]): Weights for each dataset queue

    Returns:
        List[Dict[str, Any]]: List of samples
    """
    # divide the batch size between the number of valid sample queues
    sample_queues = await valkey_client.keys(f"kl3m:samples:{task}:*")
    num_queues = len(sample_queues)
    if num_queues == 0:
        LOGGER.error("No sample queues available")
        raise HTTPException(status_code=503, detail="No sample queues available")

    # filter the sample queues by the weights
    valid_queues = []
    value_weights = []
    for queue in sample_queues:
        dataset = queue.decode().split(":")[2]
        if dataset in weights:
            valid_queues.append(queue)
            value_weights.append(weights[dataset])

    # setup loop state and conditions
    samples = []
    remaining = batch_size
    unique_datasets = set()

    while len(samples) < batch_size:
        # randomly sample a queue based on the weights
        queue = random.choices(valid_queues, value_weights)[0]

        # fetch samples from the queue
        if random.random() < 0.5:
            records = await valkey_client.lpop(queue, 1)
        else:
            records = await valkey_client.rpop(queue, 1)

        if records is not None:
            unique_datasets.add(queue.decode())
            samples.extend([json.loads(record) for record in records])
            remaining -= len(records)

        if remaining <= 0:
            break

    random.shuffle(samples)
    return samples[:batch_size]


@app.post("/batch/{task}", response_model=List[Dict[str, Any]])
async def get_batch(
    request: Request,
    task: str,
    batch_request: BatchRequest,
    background_tasks: BackgroundTasks,
):
    """
    Get a batch of samples for the given task.
    """

    # lpop the records from the key
    try:
        client = valkey.Valkey.from_pool(request.app.state.valkey_pool)
        if await get_task_queue_length(client, task) < batch_request.batch_size:
            LOGGER.warning("Not enough data available for task %s", task)
            raise HTTPException(status_code=503, detail="Not enough data available")

        records = await get_samples_uniform(client, task, batch_request.batch_size)
    except Exception as e:
        LOGGER.error("Error fetching %s batch: %s", task, str(e))
        raise HTTPException(status_code=503, detail="No data available")

    if len(records) < batch_request.batch_size:
        raise HTTPException(status_code=503, detail="No data available")

    # parse the records and return them
    return records


@app.get("/status", response_model=CacheStatus)
async def get_cache_status(request: Request):
    """
    Get the current status of the cache.
    """
    try:
        client = valkey.Valkey.from_pool(request.app.state.valkey_pool)
        source_queue_lengths = await get_source_queue_lengths(client)
        sample_queue_lengths = await get_sample_queue_lengths(client)

        # list all task types
        task_keys = [
            str(key.split(b":")[2].decode())
            for key in await client.keys("kl3m:samples:*")
        ]

        task_counts = {
            task: await get_task_queue_length(client, task) for task in set(task_keys)
        }

        return CacheStatus(
            # sort by keys
            sources=dict(sorted(source_queue_lengths.items())),
            samples=dict(sorted(sample_queue_lengths.items())),
            task_counts=dict(sorted(task_counts.items())),
        )

    except Exception as e:
        LOGGER.error(f"Error fetching cache status: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/source/random", response_model=Dict[str, Any])
async def get_random_source(request: Request):
    """
    Get a random source record.
    """

    # get the list of all keys with kl3m:sources:*
    try:
        client = valkey.Valkey.from_pool(request.app.state.valkey_pool)
        source_keys = await client.keys("kl3m:sources:*")
    except Exception as e:
        LOGGER.error(f"Error fetching source keys: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # get a random source record from the keys
    source_key = random.choice(source_keys)

    # use either lindex 0 or -1
    if random.random() < 0.5:
        source_record = await client.lindex(source_key, 0)
    else:
        source_record = await client.lindex(source_key, -1)

    # add datset to record
    record = json.loads(source_record)
    record["dataset"] = source_key.decode().split(":").pop()
    return record


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
