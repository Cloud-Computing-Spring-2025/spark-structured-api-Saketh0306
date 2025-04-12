Overview of the Assignment
This assignment aims to analyze user listening behavior and music trends using Apache Spark Structured APIs. We will process two structured datasets containing user listening logs and song metadata to derive insights into genre preferences, song popularity, listener engagement, and recommend personalized songs to users based on their listening habits.

Description of the Datasets
listening_logs.csv
This dataset contains log data capturing each user's listening activity:

user_id: Unique ID of the user.

song_id: Unique ID of the song.

timestamp: Date and time when the song was played (e.g., 2025-03-23 14:05:00).

duration_sec: Duration in seconds for which the song was played.

Example row:
user_id,song_id,timestamp,duration_sec
1,101,2025-03-23 14:05:00,200
2,102,2025-03-23 14:07:00,240

songs_metadata.csv
This dataset contains metadata about the songs in the catalog:

song_id: Unique ID of the song.

title: Title of the song.

artist: Name of the artist.

genre: Genre of the song (e.g., Pop, Rock, Jazz).

mood: Mood category of the song (e.g., Happy, Sad, Energetic, Chill).

Example row:
song_id,title,artist,genre,mood
101,Song A,Artist X,Pop,Hip-Hop
102,Song B,Artist Y,Rock,Happy
Tasks to Perform
Find Each User’s Favorite Genre

Identify the most listened-to genre for each user by counting how many times they played songs in each genre.

Output the result in user_favorite_genres/.

Calculate the Average Listen Time per Song

Compute the average duration (in seconds) for each song based on user play history.

Output the result in avg_listen_time_per_song/.

List the Top 10 Most Played Songs This Week

Determine which songs were played the most in the current week and return the top 10 based on play count.

Output the result in top_songs_this_week/.

Recommend "Happy" Songs to Users Who Mostly Listen to "Sad" Songs

Identify users who primarily listen to "Sad" songs and recommend up to 3 "Happy" songs they haven’t played yet.

Output the result in happy_recommendations/.

Compute the Genre Loyalty Score for Each User

Calculate the proportion of their plays that belong to their most-listened genre.

Output users with a loyalty score above 0.8 in genre_loyalty_scores/.

Identify Users Who Listen to Music Between 12 AM and 5 AM

Extract users who frequently listen to music between 12 AM and 5 AM based on their listening timestamps.

Output the result in night_owl_users/.
Combine listening logs with song metadata and enrich them with genre and mood data.

Output the result in enriched_logs/.

Output Folder Structure
The outputs will be saved in the following folder structure:
output/
├── user_favorite_genres/
├── avg_listen_time_per_song/
├── top_songs_this_week/
├── happy_recommendations/
├── genre_loyalty_scores/
├── night_owl_users/
Spark Shell or PySpark Script Execution Commands
Running the PySpark Script To run the PySpark script, use the following command:

spark-submit script_name.py
Starting Spark Shell (if you prefer to use Spark Shell)
spark-shell --master local[4]

Loading DataFrames In PySpark, you can load the datasets using:
listening_logs_df = spark.read.csv("path/to/listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("path/to/songs_metadata.csv", header=True, inferSchema=True)
Example Commands for Task Execution

Favorite Genre Calculation:
favorite_genre_df = listening_logs_df.join(songs_metadata_df, on="song_id") \
                                    .groupBy("user_id", "genre") \
                                    .agg(count("song_id").alias("play_count"))
favorite_genre_df.write.csv("output/user_favorite_genres/")

Top Songs This Week:
top_songs_df = listening_logs_df.filter("timestamp >= '2025-03-20' AND timestamp <= '2025-03-27'") \
                                .groupBy("song_id") \
                                .agg(count("song_id").alias("play_count")) \
                                .orderBy("play_count", ascending=False) \
                                .limit(10)
top_songs_df.write.csv("output/top_songs_this_week/")
Errors Encountered and Resolutions
1. Error: "Py4JJavaError: An error occurred while calling o56.csv"
Solution:

This error may occur due to missing dependencies or incorrect file paths. Ensure that the file paths are correct and accessible from the Spark environment.

Verify that the necessary libraries and configurations are set up correctly.

2. Error: "AnalysisException: cannot resolve 'song_id' given input columns"
Solution:

This error can happen when performing a join operation between two DataFrames that don't share a column name. Make sure both DataFrames have the correct column names and data types before performing the join.

3. Error: "NullPointerException while processing data"
Solution:

NullPointerExceptions can arise when there is missing or corrupt data. Ensure that the input data is cleaned and pre-processed before any transformation.

4. Error: "OutOfMemoryError during large dataset processing"
Solution:

If you encounter memory issues while processing large datasets, consider increasing the executor memory by modifying the Spark configuration:

spark-submit --executor-memory 4G --driver-memory 2G script_name.py

Conclusion
This project demonstrates the use of Spark for processing structured data to analyze user behavior on a music streaming platform. By performing these tasks, we gain valuable insights into music trends, listener engagement, and can provide personalized song recommendations.

Screenshot
![alt text](image.png)
ouput Screenshots
![alt text](image-2.png)
![alt text](image-3.png)
![alt text](image-4.png)
![alt text](image-5.png)
![alt text](image-6.png)
![alt text](image-1.png)
