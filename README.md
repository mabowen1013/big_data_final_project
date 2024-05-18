1. Data Ingestion:
The dataset that I use is from huggingface on the informations about steam-games. Because I have little knowledge on how the spark handle the json file, I used python code first load the data as a DataFrame, then download it to my local computer as a csv file, then I upload the csv file to dataproc for scala processing.
The original json file, the code, and the csv file are all in the data_ingest directory. 
2. Data Cleaning:
For the dataset, I delete the rows that have NULL values, then delete some columns that are not beneficial for our analysis.
Code are in the etl_code file, and once you run the code, you can get a cleaned dataset.
3. Data Analysis:
I checked the correlation between different features, trying to find their relationships. Then I also counts the quantity of the user score feature, so see its distribution. At last I made a new column, which calculates the positive_vote_ratio for the game, which makes the viewer better understand if a game is good or not.
