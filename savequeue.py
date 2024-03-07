import asyncio
from constants import JSON_LIST, END_OF_QUEUE, SNAPSHOT_INTERVAL
import json
import datetime

class SaveQueue():
    def __init__(self):
        self.queue = asyncio.Queue()
        self.registry: JSON_LIST = []
        self.done: bool = False
        
    async def put(self, item):
        if item == END_OF_QUEUE:
            self.done = True
        else:
            self.registry.append(item)
        await self.queue.put(item)
    
    async def get(self):
        return await self.queue.get()
    
    def task_done(self):
        self.queue.task_done()
        
    def empty(self):
        return self.queue.empty()
    
    async def snapshot(self, filename: str, interval: int = SNAPSHOT_INTERVAL):
        while not self.done:
            self.save_registry(filename)
            await asyncio.sleep(interval)

    def save_registry(self, filename: str):
        with open(f"test/snapshot/{filename}.json", 'w') as file:
            json.dump(self.registry, file, indent=2)
        self.log_registry(filename)
        
    def log_registry(self, filename: str):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(f'test/log/{filename}.txt', 'a') as file:
            file.write(f"{current_time}: Saved {len(self.registry)} items from registry\n")