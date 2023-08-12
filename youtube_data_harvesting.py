from googleapiclient.discovery import build
from googleapiclient.errors import HttpError 
from pymongo import MongoClient
import mysql.connector
import streamlit as st
import re
from datetime import datetime, timezone, timedelta
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb+srv://guvi1415:guvi1415@cluster0.zf9q7hd.mongodb.net/")
db = client["youtube_data"]
API_KEY = "AIzaSyCb8dWqD8IdhnvT2-Zc5BYb4ajT1YLrToo"

# Connect to MySQL
mysql_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="youtube_data"
)

def convert_datetime_youtube_to_mysql(youtube_datetime):
    youtube_format = "%Y-%m-%dT%H:%M:%SZ"
    mysql_format = "%Y-%m-%d %H:%M:%S"

    # Parse the YouTube datetime string and convert to UTC timezone
    youtube_datetime_obj = datetime.strptime(youtube_datetime, youtube_format).replace(tzinfo=timezone.utc)

    # Convert to the local timezone (you can adjust this based on your application's timezone)
    local_timezone = timezone(timedelta(hours=5, minutes=30))  # Adjust to your desired timezone
    local_datetime = youtube_datetime_obj.astimezone(local_timezone)

    return local_datetime.strftime(mysql_format)

def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?' 
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'

    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds

    return total_seconds





# Function to get channel details, video details, and comment details
def get_channel_data(youtube, channel_id):
    channel_data = {}

    # Get channel details
    response = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id,
    ).execute()

    if "items" not in response or len(response["items"]) == 0:
        st.error("Invalid channel ID. Please enter a valid channel ID.")
        return None

    channel_info = response["items"][0]
    channel_data["ChannelId"] = channel_id
    channel_data["Channel name"] = channel_info["snippet"]["title"]
    channel_data["Channel description"] = channel_info["snippet"]["description"]
    channel_data["Channel subscriber count"] = channel_info["statistics"]["subscriberCount"]
    channel_data["Channel video count"] = channel_info["statistics"]["videoCount"]
    channel_data["Channel view count"] = channel_info["statistics"]["viewCount"]
    channel_data["PlaylistId"] = channel_info["contentDetails"]["relatedPlaylists"]["uploads"]
    # Get playlist ID for videos
    playlist_id = channel_info["contentDetails"]["relatedPlaylists"]["uploads"]

    # Get video details
    video_ids = []
    video_data = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response["items"]:
            video_ids.append(item["contentDetails"]["videoId"])

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()

        for video in response["items"]:
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})

            video_info = {
                "video_id": video["id"],
                "title": snippet["title"],
                "description": snippet["description"],
                "tags": snippet.get("tags", []),
                "publishedAt": snippet["publishedAt"],
                "thumbnail_url": snippet["thumbnails"]["default"]["url"],
                "viewCount": statistics.get("viewCount", 0),
                "likeCount": statistics.get("likeCount", 0),
                "favoriteCount": statistics.get("favoriteCount", 0),
                "commentCount": statistics.get("commentCount", 0),
                "duration": content_details.get("duration", ""),
                "definition": content_details.get("definition", ""),
                "caption": content_details.get("caption", "")
            }

            video_data.append(video_info)

    # Get comments for each video
    for video in video_data:
        video_id = video["video_id"]
        video["Comments"] = []

        try:
            next_page_token = None

            while True:
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token
                )
                response = request.execute()

                for item in response["items"]:
                    comment_id = item["id"]
                    comment = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
                    comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                    published_at = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]

                    reply_texts = []
                    for reply in item.get("replies", {}).get("comments", []):
                        reply_text = reply["snippet"]["textOriginal"]
                        reply_texts.append(reply_text)

                    comment_info = {
                        "Comment_Id": comment_id,
                        "Comment_Text": comment,
                        "Comment_Author": comment_author,
                        "Comment_PublishedAt": published_at,
                        "Replies": reply_texts
                    }

                    video["Comments"].append(comment_info)

                if "nextPageToken" in response:
                    next_page_token = response["nextPageToken"]
                else:
                    break

        except HttpError as e:
            # Handle the exception when comments are disabled
            if e.resp.status == 403 and "commentsDisabled" in str(e):
                print(f"Comments are disabled for video ID: {video_id}")
            else:
                print(f"Failed to retrieve comments for video ID: {video_id}")
                print(f"Error message: {str(e)}")
            continue
        except Exception as e:
            print(f"Failed to retrieve comments for video ID: {video_id}")
            print(f"Error message: {str(e)}")
            continue

    channel_data["Videos"] = video_data

    return channel_data


# Function to migrate data to MongoDB
def migrate_data_to_mongodb(channel_id):
    youtube = build("youtube", "v3", developerKey=API_KEY)
    channel_data = get_channel_data(youtube, channel_id)
    channels_collection = db["channel"]  

    # Check if channel data already exists
    existing_data = channels_collection.find_one({"ChannelId": channel_id})

    if existing_data:
        # Update the existing channel data
        channels_collection.update_one({"ChannelId": channel_id}, {"$set": channel_data})
    else:
        # Insert the new channel data
        channel_data["ChannelId"] = channel_id
        channels_collection.insert_one(channel_data)


    # Insert video data
    videos_collection = db["video"]
    for video in channel_data["Videos"]:
        video_id = video["video_id"]
        video_data = {
            "VideoId": video_id,
            "ChannelId": channel_id,
            "Title": video["title"],
            "Description": video["description"],
            "Tags": video["tags"],
            "PublishedAt": video["publishedAt"],
            "ThumbnailUrl": video["thumbnail_url"],
            "ViewCount": video["viewCount"],
            "LikeCount": video["likeCount"],
            "FavoriteCount": video["favoriteCount"],
            "CommentCount": video["commentCount"],
            "Duration": video["duration"],
            "Definition": video["definition"],
            "Caption": video["caption"]
        }
        videos_collection.insert_one(video_data)

        # Insert comment data
        comments_collection = db["comment"]
        for comment in video["Comments"]:
            comment_data = {
                "CommentId": comment["Comment_Id"],
                "VideoId": video_id,
                "CommentText": comment["Comment_Text"],
                "CommentAuthor": comment["Comment_Author"],
                "CommentPublishedAt": comment["Comment_PublishedAt"],
                "Replies": comment["Replies"]
            }
            comments_collection.insert_one(comment_data)

    # Insert playlist data
    playlist_id = channel_data["PlaylistId"]
    playlist_data = {
        "PlaylistId": playlist_id,
        "ChannelId": channel_id
    }
    playlists_collection = db["playlist"]
    playlists_collection.insert_one(playlist_data)

# Function to migrate data from MongoDB to MySQL
def migrate_data_to_sql(channel_id):
    # Retrieve channel data from MongoDB
    channel_data = db["channel"].find_one({"ChannelId": channel_id})

    if channel_data:
        # Check if channel data already exists
        with mysql_connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM channel WHERE channel_id = %s", (channel_id,))
            result = cursor.fetchone()
            if result[0] > 0:
                st.write("Channel data already exists in MYSQL. Migrating updated channel data")
                # Uses "ON DELETE CASCADE" to delete all corresponding details of given channel id
                cursor.execute("DELETE FROM channel WHERE channel_id= %s", (channel_id,))

        # Insert channel data into mySQL
        with mysql_connection.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO channel (channel_id, channel_name, channel_views, channel_description) "
                    "VALUES (%s, %s, %s, %s)",
                    (
                        channel_data["ChannelId"],
                        channel_data["Channel name"],
                        int(channel_data["Channel view count"]),
                        channel_data["Channel description"]
                    )
                )
                cursor.execute(
                    "INSERT INTO playlist (playlist_id, channel_id) "
                    "VALUES (%s, %s)",
                    (
                        channel_data["PlaylistId"],
                        channel_data["ChannelId"]
                    )
                )
                # Commit the transaction
                mysql_connection.commit()
                
                
                # Insert video data into mySQL
                for video in channel_data["Videos"]:
                    # Convert duration to seconds (no need to convert it to a timestamp)
                    duration_seconds = convert_duration(video["duration"])

                    # Convert YouTube published date to MySQL datetime format
                    publish_date_time_youtube = video["publishedAt"]
                    publish_date_time_mysql = convert_datetime_youtube_to_mysql(publish_date_time_youtube)

                    # Check if video with the same video_id already exists in the video table
                    cursor.execute("SELECT COUNT(*) FROM video WHERE video_id = %s", (video["video_id"],))
                    video_exists = cursor.fetchone()[0]

                    if video_exists:
                        # Update the existing video record
                        cursor.execute(
                            "UPDATE video SET playlist_id = %s, video_name = %s, video_description = %s, "
                            "published_date = %s, view_count = %s, like_count = %s, favorite_count = %s, "
                            "comment_count = %s, duration = %s, thumbnail = %s, caption_status = %s "
                            "WHERE video_id = %s",
                            (
                                channel_data["PlaylistId"],
                                video["title"],
                                video["description"],
                                publish_date_time_mysql,
                                int(video["viewCount"]),
                                int(video["likeCount"]),
                                int(video["favoriteCount"]),
                                int(video["commentCount"]),
                                duration_seconds,
                                video["thumbnail_url"],
                                video["caption"],
                                video["video_id"]
                            )
                        )
                        # Commit the transaction
                        mysql_connection.commit()
                
                
                    else:
                        # Insert the new video record
                        cursor.execute(
                            "INSERT INTO video (video_id, playlist_id, video_name, video_description, published_date, "
                            "view_count, like_count, favorite_count, comment_count, duration, thumbnail, caption_status) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (
                                video["video_id"],
                                channel_data["PlaylistId"],
                                video["title"],
                                video["description"],
                                publish_date_time_mysql,
                                int(video["viewCount"]),
                                int(video["likeCount"]),
                                int(video["favoriteCount"]),
                                int(video["commentCount"]),
                                duration_seconds,
                                video["thumbnail_url"],
                                video["caption"]
                            )
                        )
                        # Commit the transaction
                        mysql_connection.commit()
                
                

                    # Insert comment data into mySQL
                    for comment in video["Comments"]:
                        cursor.execute(
                            "INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date) "
                            "VALUES (%s, %s, %s, %s, %s)",
                            (
                                comment["Comment_Id"],
                                video["video_id"],
                                comment["Comment_Text"],
                                comment["Comment_Author"],
                                comment["Comment_PublishedAt"]
                            )
                        )

                        mysql_connection.commit()
                st.write("All data migrated from MongoDB to MySQL")
            except Exception as e:
                mysql_connection.rollback()  # Rollback changes if an error occurs
                st.error("Error occurred during migration: {}".format(str(e)))
    else:
        st.error("No data found for the provided channel ID")
        
def create_tables_in_mysql():
    with mysql_connection.cursor() as cursor:
        # Create Channel table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel (
                channel_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(255),
                channel_description TEXT,
                channel_subscriber_count INT,
                channel_video_count INT,
                channel_view_count INT,
                playlist_id VARCHAR(255)
            )
        """)
        
        # Create Playlist table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist (
                playlist_id VARCHAR(255) PRIMARY KEY,
                channel_id VARCHAR(255),
                FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
            )
        """)

        # Create Video table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS video (
                video_id VARCHAR(255) PRIMARY KEY,
                playlist_id VARCHAR(255),
                video_name VARCHAR(255),
                video_description TEXT,
                published_date TIMESTAMP,
                view_count INT,
                like_count INT,
                favorite_count INT,
                comment_count INT,
                duration INT,
                thumbnail_url VARCHAR(255),
                caption_status VARCHAR(10),
                FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
            )
        """)

        # Create Comment table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment (
                comment_id VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255),
                comment_text TEXT,
                comment_author VARCHAR(255),
                comment_published_date TIMESTAMP,  
                FOREIGN KEY (video_id) REFERENCES video(video_id)
            )
        """)

        

# Main Streamlit function
def main():
    # YouTube service client setup
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=API_KEY)

    # MySQL connection setup
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data"
    )
    create_tables_in_mysql()
    
    # ============    /   Configuring Streamlit GUI   /    ============    #

    st.set_page_config(layout='wide')
    # Title
    st.title(":red[YouTube Data Harvesting]")
    # columns for fetching & migration
    col1, col2 = st.columns(2)

    # ===============    /   Data Fetch Section   /    ================    #

    with col1:
        st.header(':white[YOUTUBE_DATA]')

        # User input of channel_id
        channel_id = st.text_input("Enter Channel ID")
        st.write('''Click the button to get the channel details.''')
        if st.button(":violet[Get Channel Data]"):
            channel_data = get_channel_data(youtube, channel_id)
            st.json(channel_data)

    # ===============    /   Migration Section   /    ================    #

    with col2:
        st.header(':white[Data Transmit]')

        # Initialize the 'fetched_channel_ids' key with an empty list if it doesn't exist
        fetched_channel_ids = st.session_state.setdefault('fetched_channel_ids', [])

        # Convert fetched_channel_ids to a list if it's a string
        # since channel_id is string & fetched_channel_ids need to be a list for ids to be appended
        if isinstance(fetched_channel_ids, str):
            fetched_channel_ids = [fetched_channel_ids]

        # Check if the channel ID is not already in fetched_channel_ids
        # Add the channel_id to the 'fetched_channel_ids' list
        if channel_id not in fetched_channel_ids:
            fetched_channel_ids.append(channel_id)

        # Store the updated 'fetched_channel_ids' list in session state
        st.session_state['fetched_channel_ids'] = fetched_channel_ids

        # Store channel IDs in a multi-selectable dropdown
        selected_channel_ids = st.multiselect("Select Channel IDs to migrate",
                                              st.session_state.get('fetched_channel_ids', []))

        if st.button(":violet[Store at MongoDB]"):
            for selected_id in selected_channel_ids:
                try:
                    migrate_data_to_mongodb(selected_id)
                    st.write(f"Data migrated to MongoDB for Channel ID: {selected_id}")
                except ValueError as e:
                    st.error(str(e))

        if st.button(":Violet[Migrate to SQL]"):
            for selected_id in selected_channel_ids:
                try:
                    migrate_data_to_sql(selected_id)
                    st.write(f"Data migrated to MYSQL for Channel ID: {selected_id}")
                except Exception as e:
                    st.error("Error occurred during migration: {}".format(str(e)))

    # ==================    /   Query Section   /    ===================    #

    # Define the SQL queries
    queries = {
        "0. All data channel table?":"""
                SELECT * FROM  channel;

                    """,
        "1. What are the names of all the videos and their corresponding channels?": """
                SELECT video.video_name, channel.channel_name
                FROM video
                JOIN playlist ON video.playlist_id = playlist.playlist_id
                JOIN channel ON playlist.channel_id = channel.channel_id;

                    """,
        "2. Which channels have the most number of videos, and how many videos do they have?": """
                SELECT c.channel_name, COUNT(v.video_id) AS video_count
                FROM channel c
                JOIN playlist p ON c.channel_id = p.channel_id
                JOIN video v ON p.playlist_id = v.playlist_id
                GROUP BY c.channel_name
                ORDER BY video_count DESC;

                """,
        "3. What are the top 10 most viewed videos and their respective channels?": """
                SELECT video.video_name, channel.channel_name, video.view_count
                FROM channel
                JOIN playlist ON channel.channel_id = playlist.channel_id
                JOIN video ON playlist.playlist_id = video.playlist_id
                ORDER BY view_count DESC
                LIMIT 10;

                """,
        "4. How many comments were made on each video, and what are their corresponding video names?": """
                SELECT video.video_name, COUNT(comment.comment_id) AS comment_count
                FROM video
                JOIN comment ON video.video_id = comment.video_id
                GROUP BY video.video_name
                ORDER BY comment_count DESC;

                """,
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?": """
                SELECT video.video_name, channel.channel_name
                FROM channel
                JOIN playlist ON channel.channel_id = playlist.channel_id
                JOIN video ON playlist.playlist_id = video.playlist_id
                WHERE video.like_count = (SELECT MAX(like_count) FROM video);

                """,
        "6. What is the total number of likes and favorites for each video, and what are their corresponding video names?": """
                SELECT video.video_name, video.like_count, video.favorite_count
                FROM video
                ORDER BY video.like_count DESC, video.favorite_count DESC;

                """,
        "7. What is the total number of views for each channel, and what are their corresponding channel names?": """
                SELECT channel.channel_name, SUM(video.view_count) AS channel_views
                FROM channel
                JOIN playlist ON channel.channel_id = playlist.channel_id
                JOIN video ON playlist.playlist_id = video.playlist_id
                GROUP BY channel.channel_name
                ORDER BY channel_views DESC;

                """,
        "8. What are the names of all the channels that have published videos in the year 2022?": """
                SELECT DISTINCT(channel.channel_name)
                FROM channel
                JOIN playlist ON channel.channel_id = playlist.channel_id
                JOIN video ON playlist.playlist_id = video.playlist_id
                WHERE EXTRACT(YEAR FROM video.published_date) = 2022;

                """,
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
                SELECT channel.channel_name, AVG(video.duration) AS average_duration
                FROM channel
                JOIN playlist ON channel.channel_id = playlist.channel_id
                JOIN video ON playlist.playlist_id = video.playlist_id
                GROUP BY channel.channel_name
                ORDER BY average_duration DESC;

                """,
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?": """
                SELECT video.video_name, channel.channel_name
                FROM channel
                JOIN playlist ON channel.channel_id = playlist.channel_id
                JOIN video ON playlist.playlist_id = video.playlist_id
                WHERE video.comment_count = (SELECT MAX(comment_count) FROM video);

                """
    }

    # Sidebar section
    st.sidebar.header('Question Section')

    # Create a dropdown menu to select the question
    selected_question = st.sidebar.selectbox("Select a question", list(queries.keys()))

    if st.sidebar.button("Display Data"):
        # Execute the selected query
        selected_query = queries[selected_question]
        with mysql_connection.cursor() as cursor:
            try:
                cursor.execute(selected_query)
                results = cursor.fetchall()

                # Convert the MySQL query results to a pandas DataFrame
                df = pd.DataFrame(results)

                # Display the pandas DataFrame using Streamlit
                if not df.empty:
                    st.write(df)
                else:
                    st.write("No results found.")

            except Exception as e:
                st.error(f"Error executing the query: {e}")
                df = pd.DataFrame()  # Create an empty DataFrame in case of an error

    # Close the MySQL connection when the Streamlit app is closed
    mysql_connection.close()

if __name__ == "__main__":
    # Create tables in MySQL before running the Streamlit app
    create_tables_in_mysql()
    main()
