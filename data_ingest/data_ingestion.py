from datasets import load_dataset
import pandas as pd

dataset = load_dataset("FronkonGames/steam-games-dataset")
data = dataset['train']
df = data.to_pandas()
save_path = r"D:\documents\纽大\大三semester2\BigData\final_project\data_ingest\game_data.csv"
df.to_csv(save_path, index=False)