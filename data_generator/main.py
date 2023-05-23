import utils
import asyncio
import time
from tqdm import tqdm

start = time.time()

for _ in tqdm(range(61)):
    for _ in range(10):
        tasks = []
        for _ in range(10):
            random_keywords = utils.generate_keywords()
            random_libs = utils.generate_lib()
            task = utils.generate_item(user_search=random_keywords, selected_lib=random_libs)
            tasks.append(task)

        result = asyncio.run(asyncio.wait(tasks))
        time.sleep(0.7)
    time.sleep(1)

end = time.time()

print(f"총 {end-start} 소요")
