import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import mysql.connector
from datetime import datetime

def get_database_connection():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",  # replace with your MySQL username
        password="root",  # replace with your MySQL password
        port=3306,
        database = 'youtube')
    return conn

def create_database():
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('CREATE DATABASE IF NOT EXISTS youtube')
    cursor.close()
    conn.close()

def create_tables(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS channel (
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        channel_views INT,
        channel_description TEXT,
        channel_status VARCHAR(255)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS video (
        video_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        video_name VARCHAR(255),
        video_description TEXT,
        published_date DATETIME,
        views_count INT,
        likes_count INT,
        favorite_count INT,
        comment_count INT,
        duration VARCHAR(255),
        thumbnails TEXT,
        caption_status VARCHAR(255),
        FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        comment_id VARCHAR(255) PRIMARY KEY,
        video_id VARCHAR(255),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_published DATETIME,
        channel_id VARCHAR(255),
        FOREIGN KEY (video_id) REFERENCES video(video_id),
        FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
    )
    """)
def convert_iso_to_datetime(iso_str):
    return datetime.strptime(iso_str, '%Y-%m-%dT%H:%M:%SZ')

def data_already_exists(cursor, df_channel):
    channel_id = df_channel.iloc[0]['channel_id']
    cursor.execute("SELECT COUNT(*) FROM channel WHERE channel_id = %s", (channel_id,))
    return cursor.fetchone()[0] > 0
    
def push_to_mysql(df_channel, df_video, df_comment):
    conn = None
    cursor = None
    success_message_shown = False  # Initialize the flag
    try:
        create_database()
        conn = get_database_connection()
        cursor = conn.cursor()
        create_tables(cursor)

        if data_already_exists(cursor, df_channel):
            st.warning("Same data already transferred.")
            return
        # Insert channel data
        for index, row in df_channel.iterrows():
            cursor.execute("""
            INSERT INTO channel (channel_id, channel_name, channel_views, channel_description, channel_status)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            channel_name = VALUES(channel_name),
            channel_views = VALUES(channel_views),
            channel_description = VALUES(channel_description),
            channel_status = VALUES(channel_status)
            """, (row['channel_id'], row['channel_name'], row['channel_views'], row['channel_description'], row['channel_status']))
        # Insert video data
        for index, row in df_video.iterrows():
            published_date = convert_iso_to_datetime(row['published_date'])
            cursor.execute("""
            INSERT INTO video (video_id, channel_id, video_name, video_description, published_date, views_count, likes_count, favorite_count, comment_count, duration, thumbnails, caption_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            channel_id = VALUES(channel_id),
            video_name = VALUES(video_name),
            video_description = VALUES(video_description),
            published_date = VALUES(published_date),
            views_count = VALUES(views_count),
            likes_count = VALUES(likes_count),
            favorite_count = VALUES(favorite_count),
            comment_count = VALUES(comment_count),
            duration = VALUES(duration),
            thumbnails = VALUES(thumbnails),
            caption_status = VALUES(caption_status)
            """, (row['video_id'], row['channel_id'], row['video_name'], row['video_description'], published_date, row['views_count'], row['likes_count'], row['favorite_count'], row['comment_count'], row['duration'], str(row['thumbnails']), row['caption_status']))

        # Insert comment data only if video_id exists in the video table
        for index, row in df_comment.iterrows():
            cursor.execute("SELECT COUNT(*) FROM video WHERE video_id = %s", (row['video_id'],))
            if cursor.fetchone()[0] > 0:
                comment_published = convert_iso_to_datetime(row['comment_published'])
                cursor.execute("""
                INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published, channel_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                video_id = VALUES(video_id),
                comment_text = VALUES(comment_text),
                comment_author = VALUES(comment_author),
                comment_published = VALUES(comment_published),
                channel_id = VALUES(channel_id)
                """, (row['comment_id'], row['video_id'], row['comment_text'], row['comment_author'], comment_published, row['channel_id']))

        conn.commit()
        success_message_shown = True  # Set the flag to True after successful push
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if success_message_shown:  # Check the flag before showing the message
            st.success("Data pushed to MySQL successfully.")

def api_connect():
    api_key = "AIzaSyBVZUjgnopY257wpzai_Sv52PVk_Y40ADo"  # Replace with your actual API key
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

def get_channel_details(c_id):
    youtube = api_connect()
    request = youtube.channels().list(part="snippet,status,contentDetails,statistics", id=c_id)
    response = request.execute()
    data = {}
    for i in response["items"]:
        data = {
            'channel_id': i["id"],
            'channel_name': i['snippet']['title'],
            'channel_views': i['statistics']['viewCount'],
            'channel_description': i['snippet']['description'],
            'channel_status': i['status']['privacyStatus']
        }
    return data

def get_video_info(channel_id):
    video_data = []
    video_ids = []
    youtube = api_connect()
    try:
        request = youtube.search().list(part='snippet', channelId=channel_id, type='video', maxResults=100)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['id']['videoId'])

        for video_id in video_ids:
            request = youtube.videos().list(part='snippet,contentDetails,statistics', id=video_id)
            response = request.execute()

            for item in response['items']:
                data = dict(
                    channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_id=item['id'],
                    video_name=item['snippet']['title'],
                    video_description=item['snippet'].get('description', ''),
                    published_date=item['snippet']['publishedAt'],
                    views_count=item['statistics'].get('viewCount', 0),
                    likes_count=item['statistics'].get('likeCount', 0),
                    favorite_count=item['statistics'].get('favoriteCount', 0),
                    comment_count=item['statistics'].get('commentCount', 0),
                    duration=item['contentDetails']['duration'],
                    thumbnails=item['snippet']['thumbnails'],
                    caption_status=item['contentDetails']['caption']
                )
                video_data.append(data)
    except Exception as e:
        st.error(f"Error: {e}")
    return video_data

def get_comment_info(channel_id):
    comment_data = []
    video_ids = []
    youtube = api_connect()
    try:
        request = youtube.search().list(part='snippet', channelId=channel_id, type='video', maxResults=50)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['id']['videoId'])

        for video_id in video_ids:
            try:
                request = youtube.commentThreads().list(part='snippet', videoId=video_id, maxResults=50)
                response = request.execute()
                for item in response['items']:
                    data = {
                        'comment_id': item['snippet']['topLevelComment']['id'],
                        'video_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                        'comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'comment_published': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                        'channel_id': item['snippet']['topLevelComment']['snippet']['channelId']
                    }
                    comment_data.append(data)
            except Exception as e:
                if "commentsDisabled" not in str(e):
                    st.error(f"Error fetching comments for video ID: {video_id}: {e}")
    except Exception as e:
        st.error(f"Error fetching comment data: {e}")
    return comment_data

create_database()

st.sidebar.subheader('Home')
menu = ['Data Harvesting and Warehousing', 'QueryData']
choice = st.sidebar.radio("Select an option", menu)

if choice == 'Data Harvesting and Warehousing':
    st.title(':rainbow[YouTube Data Harvesting & Warehousing]')

    channel_id = st.text_input("Enter your Channel ID", key='channel_id_input')

    if st.button('Get Channel Data', key='get_channel_data'):
        if not channel_id:
            st.warning("Please enter a Channel ID.")
        else:
            result_channel = get_channel_details(channel_id)
            if result_channel:
                st.session_state.df_channel = pd.DataFrame([result_channel])
                st.write("Channel Details:")
                st.write(st.session_state.df_channel)
            else:
                st.warning("No channel details found.")
        
            result_video = get_video_info(channel_id)
            if result_video:
                st.session_state.df_video = pd.DataFrame(result_video)
                st.write("Video Data:")
                st.write(st.session_state.df_video)
            else:
                st.warning("No video data found.")

            result_comment = get_comment_info(channel_id)
            if result_comment:
                st.session_state.df_comment = pd.DataFrame(result_comment)
                st.write('Comment Data')
                st.write(st.session_state.df_comment)
            else:
                st.write('No Comment Data Found')

    if st.button('Push data to MySQL', key='push_data_to_mysql'):
        if 'df_channel' in st.session_state and 'df_video' in st.session_state and 'df_comment' in st.session_state:
            push_to_mysql(st.session_state.df_channel, st.session_state.df_video, st.session_state.df_comment)
        else:
            st.warning("No data to push. Please get channel data first.")
elif choice == 'QueryData':

    def execute_query(query):
        conn = get_database_connection()
        cursor = conn.cursor()  

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    
    st.title(':rainbow[Select the Question]')      

    selected_query = st.selectbox("Select a question", [
    "1. What are the names of all the videos and their corresponding channels?",
    "2. Which channels have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments were made on each video, and what are their corresponding video names?",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
    "8. What are the names of all the channels that have published videos in the year 2022?",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
])

    if selected_query:
        if selected_query.startswith("1."):
            query = """
            SELECT v.video_name, c.channel_name 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            """
            column_names = ["Video Name", "Channel Name"]

        elif selected_query.startswith("2."):
            query = """
            SELECT c.channel_name, COUNT(*) AS num_videos 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            GROUP BY c.channel_name 
            ORDER BY num_videos DESC
            """
            column_names = ["Channel Name", "Number of Videos"]

        elif selected_query.startswith("3."):
            query = """
            SELECT v.video_name, v.views_count, c.channel_name 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            ORDER BY v.views_count DESC 
            LIMIT 10
            """
            column_names = ["Video Name", "Views", "Channel Name"]

        elif selected_query.startswith("4."):
            query = """
            SELECT v.video_name, COUNT(*) AS num_comments 
            FROM comment co 
            JOIN video v ON co.video_id = v.video_id 
            GROUP BY v.video_name
            """
            column_names = ["Video Name", "Number of Comments"]

        elif selected_query.startswith("5."):
            query = """
            SELECT v.video_name, v.likes_count, c.channel_name 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            ORDER BY v.likes_count DESC 
            LIMIT 10
            """
            column_names = ["Video Name", "Likes", "Channel Name"]

        elif selected_query.startswith("6."):
            query = """
            SELECT v.video_name, SUM(v.likes_count) AS total_likes 
            FROM video v
            GROUP BY v.video_name
            """
            column_names = ["Video Name", "Total Likes"]

        elif selected_query.startswith("7."):
            query = """
            SELECT c.channel_name, SUM(v.views_count) AS total_views 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            GROUP BY c.channel_name
            """
            column_names = ["Channel Name", "Total Views"]

        elif selected_query.startswith("8."):
            query = """
            SELECT DISTINCT c.channel_name 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            WHERE YEAR(v.published_date) = 2022
            """
            column_names = ["Channel Name"]

        elif selected_query.startswith("9."):
            query = """
            SELECT c.channel_name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(v.duration))), '%H:%i:%s') AS avg_video_duration 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            GROUP BY c.channel_name 
            ORDER BY avg_video_duration DESC
            """
            column_names = ["Channel Name", "Average Duration"]

        elif selected_query.startswith("10."):
            query = """
            SELECT v.video_name, v.comment_count, c.channel_name 
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            ORDER BY v.comment_count DESC 
            LIMIT 10
            """
            column_names = ["Video Name", "Comments", "Channel Name"]

        # Execute the query
        result_data = execute_query(query)
        
        # Display the results
        if result_data:
            df = pd.DataFrame(result_data, columns=column_names)
            st.dataframe(df)
        else:
            st.warning("No data available for the selected query.")