# youtube-data-fetching

YouTube Data Migration and Analysis
====================================
This repository contains a comprehensive Python script designed to facilitate the seamless extraction, processing, storage, and analysis of YouTube channel data. 
The script utilizes the Google API client for data retrieval, along with MongoDB and MySQL databases for data storage and manipulation. Furthermore, it offers an 
intuitive Streamlit-powered web interface to interact with the data migration process and perform analytical queries.

Key Features and Components:
============================

1. Data Retrieval and Processing
The heart of the script lies in its ability to fetch YouTube channel data, encompassing a wide range of details, including channel metadata, video attributes,
and comments. By utilizing the Google API, the script retrieves channel statistics, video information, and associated comments. It then meticulously processes
this raw data, converting timestamps, calculating video durations, and structuring comments into an organized format for efficient storage and analysis.

2. MongoDB Data Migration
The script provides the flexibility to seamlessly migrate processed YouTube data into a MongoDB database. It establishes a connection to a MongoDB cluster and
efficiently organizes the data into distinct collections. The channel data, video details, and comment information are systematically stored, ensuring optimal
data integrity and accessibility for future analysis.

3. MySQL Data Migration
In addition to MongoDB, the script supports data migration to a MySQL database. It establishes a connection to a MySQL server and dynamically creates tables
tailored to accommodate channel information, video data, and comments. Subsequently, the processed data is inserted into these respective tables, with attention
to maintaining referential integrity and adhering to relational database principles.

4. Streamlit Web Interface
To streamline user interaction with the data migration and analysis process, the script incorporates a powerful Streamlit-powered web interface. Users can
seamlessly navigate through the interface to:

  * Input a YouTube channel ID for data retrieval.
  * Fetch and process YouTube channel data, view it in a structured format, and validate its accuracy.
  * Choose to migrate the processed data to both MongoDB and MySQL databases through intuitive buttons.
  * Perform a range of analytical queries on the MySQL data using a user-friendly dropdown menu and instantly visualize the query results.
    
5. Analytical Queries
The Streamlit dashboard offers a set of predefined analytical queries that allow users to extract meaningful insights from the stored MySQL data. These
queries enable users to delve into various aspects of the YouTube channel data, such as:

  * Retrieving all channel data, including names, descriptions, and view counts.
  * Identifying the most viewed videos and their corresponding channels.
  * Exploring video comment counts and engagement metrics.
  * Calculating average video durations for each channel.
  * Analyzing the impact of likes and favorites on video popularity.
    
Getting Started:
================
1. Install the required Python libraries by running pip install -r requirements.txt.

2. Obtain a valid Google API Key and replace it in the script.

3. Configure MongoDB and MySQL connection details according to your database setup.

4. Execute the script using python script.py to initiate the Streamlit dashboard.

5.Use the dashboard to fetch YouTube data, migrate it to MongoDB and MySQL, and perform insightful analytical queries on the stored data.

6. Explore the rich functionalities and insights offered by the script to gain a deep understanding of YouTube channel data trends and performance metrics.


Feel free to customize, extend, and adapt this script to cater to your specific requirements and use cases. Whether you're a data enthusiast, researcher, 
or content creator, this script provides a robust framework for comprehensive YouTube data analysis and visualization.
