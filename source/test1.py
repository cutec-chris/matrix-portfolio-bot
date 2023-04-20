import asyncio
import threading

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

loop = asyncio.new_event_loop()
t = threading.Thread(target=start_loop, args=(loop,))
t.start()

# Verwenden Sie die Schleife
async def my_coroutine():
    print("Hello from coroutine")
    await asyncio.sleep(1.0)
    print("Coroutine has finished")

loop.call_soon_threadsafe(asyncio.create_task, my_coroutine())