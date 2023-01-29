from tqdm import tqdm

for i in tqdm(range(1000000), desc="Processing...",
              bar_format="{l_bar}{bar} [time left: {remaining}]"):
    pass