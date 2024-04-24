import streamlit as st
import pandas as pd
import googleapiclient.discovery
import json
from datetime import datetime
import time
import isodate
import pymysql
from sqlalchemy import create_engine
import mysql.connector

# Build the YouTube API client
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyDo22WNGfc2tg-SO_wNcpahHlIw7SqrJb8"  # Replace 'YOUR_API_KEY' with your actual API key
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
#=====================================================================================================
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="")
print(mydb)
mycursor = mydb.cursor(buffered=True)
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",pw="",db="capstone"))



##################===========================================================
#Main function to define the Streamlit app

st.set_page_config(page_title='Welcome to my Streamlit app', page_icon=':bar_chart:', layout="wide")

st.title("YouTube Data Harvesting and Warehousing")

    

    
# Function to handle the data collection page

st.title('Data Collection Zone')
st.write('(Hint: This zone collects data by using channel IDs through the YouTube API Key)')
channel_id = st.text_input('ENTER CHANNEL ID')

if st.button("Extract Data"):
    
    
#fetch channel details through channel_id
    def channel_info(youtube, channel_id):
    
        channel_details = []
        
        request = youtube.channels().list(
        part="snippet,contentDetails,statistics",  # 'statistics' should be lowercase
        id=channel_id
        )
        response = request.execute()
    # Print the response to see its structure
        if 'items' in response and response['items']:
            data = {
                    "channel_ID": channel_id,
                    "channel_name": response['items'][0]['snippet']['title'],
                    "channel_description": response['items'][0]['snippet']['description'],
                    "channel_viewcount": response['items'][0]['statistics']['viewCount'],
                    "channel_videocount": response['items'][0]['statistics']['videoCount'],
                    "channel_subscribercount": response['items'][0]['statistics']['subscriberCount'],
                    }
            channel_details.append(data)
        else:
            print("No items found in the response.")
        
        return channel_details
        

    channel_data = channel_info(youtube, channel_id)
    channel_df = pd.DataFrame(channel_data)


    request = youtube.channels().list(
        part="snippet,contentDetails,Statistics",
        id=channel_id )
    resp = request.execute()
    channel_PlaylistId = resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']

   
#fetch playlist details through channel_PlaylistId
    def playlist(youtube,channel_PlaylistId):
        video_batch=[]

    

        next_page = None

        while True:
            out = youtube.playlistItems().list(
                part="contentDetails",
        
                maxResults=100,
                playlistId=channel_PlaylistId,
                pageToken=next_page)
            videolist=out.execute()  # Added pageToken parameter
            for item in videolist['items']:
                video_batch.append( item['contentDetails']['videoId'])
            if 'nextPageToken' in videolist:
                next_page = videolist['nextPageToken']
            else:
                break
        return video_batch
    video_ids=playlist(youtube,channel_PlaylistId)



#fetch video details through video_ids


    def video_info(youtube,video_ids):
        all_video = []

        for i in range(0, len(video_ids), 50):
            video_batch = youtube.videos().list(    
                part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])).execute()

            for item in video_batch.get('items', []):

                published_date = item['snippet']['publishedAt']
                parsed_date = datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
                format_date = parsed_date.strftime('%Y-%m-%d')
                formatted_time = parsed_date.strftime('%H:%M:%S')
                video_dur=item['contentDetails']['duration']

                time= isodate.parse_duration(video_dur)
                hours = time.total_seconds() // 3600
                minutes = (time.total_seconds() % 3600) // 60
                seconds = time.total_seconds() % 60
                duratn=str(int(hours))+":"+str(int(minutes))+":"+str(int(seconds))


                data = {
                "video_id": item['id'],
                "channel_title": item['snippet']['channelTitle'],
                "channel_description": item['snippet']['description'],
                "video_title": item['snippet']['title'],
                "video_publisheddate": format_date,
                "video_publishedtime": formatted_time,
                "video_duration": duratn,
                "video_commentcount": item['statistics'].get('commentCount', 0),
                "video_favoritecount": item['statistics'].get('favoriteCount', 0),
                "video_likecount": item['statistics'].get('likeCount', 0),
                "video_viewcount": item['statistics'].get('viewCount', 0),
                "video_thumbnail": item['snippet']['thumbnails']['default']['url'],
                "caption_status": item['contentDetails'].get('caption', '')
                }
                all_video.append(data)

        return all_video
    video_data=video_info(youtube,video_ids)
    video_df=pd.DataFrame(video_data)
 
#fetch comment details through video_ids
    def comment_info(youtube,video_ids):
        allcomments=[]
    
        for videoid in video_ids:
            try:
                req=youtube.commentThreads().list(
                    part="snippet,replies",
               
                    videoId=videoid,
                    maxResults=100)
                comments=req.execute()


                for i in range(0,len(comments['items'])):
                    published_date = comments['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                    timestamp =published_date
                    parsed_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    date = parsed_timestamp.date()
                    time = parsed_timestamp.time()
                    comments_data={"comment_id":comments['items'][i]['id'],
                                        "channel_ID":comments['items'][i]['snippet']['channelId'],
                                        "video_ID":comments['items'][i]['snippet']['videoId'],
                                        "comment_text":comments['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                                        "comment_authorname":comments['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        "comments_publisheddate": date,
                                        "comments_publishedtime" : time,
                                        "comments_likecount":comments['items'][i]['snippet']['topLevelComment']['snippet']['likeCount'],
                                        "comment_replycount":comments['items'][i]['snippet']['totalReplyCount'] }
                    allcomments.append(comments_data)
            except:
                pass
        return allcomments
    comment_data=comment_info(youtube,video_ids)
    comment_df=pd.DataFrame(comment_data)
    







#====================================================================================



# Function to create table in MySQL
def create_table_in_mysql(engine):
    conn = engine.raw_connection()
    
    #scursor = conn.cursor()
    mycursor.execute('''CREATE TABLE IF NOT EXISTS capstonepro_1.channel (
                     channel_ID VARCHAR(255),
                     channel_name TEXT,
                     channel_description TEXT,
                     channel_viewcount BIGINT,
                     channel_videocount BIGINT,
                     channel_subscribercount BIGINT)''')
    mycursor.execute('''CREATE TABLE IF NOT EXISTS capstonepro_1.video (
                     video_id VARCHAR(255),
                     channel_title TEXT,
                     channel_description TEXT,
                     video_title TEXT,
                     video_publisheddate DATE,
                     video_publishedtime VARCHAR(255),
                     video_duration TEXT,
                     video_commentcount BIGINT,
                     video_favoritecount BIGINT,
                     video_likecount BIGINT,
                     video_viewcount BIGINT,
                     video_thumbnail TEXT,
                     caption_status TEXT)''')
    mycursor.execute('''CREATE TABLE IF NOT EXISTS capstonepro_1.comment(
                     comment_id VARCHAR(255),
                     channel_ID VARCHAR(255),
                     video_ID VARCHAR(255),
                     comment_text TEXT,
                     comment_authorname VARCHAR(255),
                     comments_publisheddate DATE,
                     comments_publishedtime VARCHAR(255),
                     comments_likecount BIGINT,
                     comment_replycount BIGINT)''')
    mydb.commit()
   # cursor.close()
    conn.commit()
   # conn.close()


# Function to insert data into MySQL
def insert_data(engine, channel_df, video_df, comment_df):
    conn = engine.raw_connection()
    cursor = conn.cursor()

    cursor.execute("USE capstonepro_1")

    channel_df.to_sql('channel', con=engine, if_exists="append", index=False)
    video_df.to_sql('video', con=engine, if_exists="append", index=False)
    comment_df.to_sql('comment', con=engine, if_exists="append", index=False)

    conn.commit()
    #cursor.close()
    #conn.close()


# Establish connection to MySQL database
def connect_to_database():
   
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",pw="",db="capstonepro_1"))
    return engine

# Function to handle data migration zone
st.title('DATA MIGRATION ZONE')
st.write('(Hint: This zone migrate data to SQL data warehouse)')

    # Connect to MySQL database
engine = connect_to_database()

    # Create table
create_table_in_mysql(engine)

    # Insert data into table
#insert_data(engine,channel_df,video_df,comment_df)


on = st.toggle('SCRAPE AND INSERT DATA')

if on:
    insert_data(engine,channel_df,video_df,comment_df)
    st.write('Data Migrated successfully!')

#function to handle data Analysis zone
def execute_selected_query(option):
    #mycursor = conn.cursor()
    if option == "1. What are the names of all the videos and their corresponding channels?":
        query="""
                 SELECT v.video_title, c.channel_name 
                FROM capstonepro_1.video AS v
                JOIN capstonepro_1.channel AS c ON v.channel_title = c.channel_name"""
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['video_title','channel_name'])
        st.write(df)
    elif option == "2. Which channels have the most number of videos, and how many videos do they have?":
        query="""
                SELECT c.channel_name, COUNT(*) AS channel_videocount
                FROM capstonepro_1.video v
                JOIN capstonepro_1.channel c ON v.channel_title = c.channel_name
                GROUP BY c.channel_name
            ORDER BY channel_videocount DESC"""
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['channel_name','channel_videocount'])
        st.write(df)
    elif option == "3. What are the top 10 most viewed videos and their respective channels?":
        query="""
                SELECT v.video_title, c.channel_name, v.video_viewcount
                FROM capstonepro_1.video v
                JOIN capstonepro_1.channel c ON v.channel_title = c.channel_name
                ORDER BY v.video_viewcount DESC
                LIMIT 10"""
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['channel_name','video_Viewcount'])
        st.write(df)
    elif option == "4. How many comments were made on each video, and what are their corresponding video names?":
        query="""
                SELECT v.video_title, COUNT(c.comment_id) AS comment_count
                FROM capstonepro_1.video AS v
                JOIN capstonepro_1.comment AS c ON v.video_id = c.video_ID
                GROUP BY v.video_title"""
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['video_title','comment_count'])
        st.write(df)
    elif option == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        query="""
                SELECT v.video_title, c.channel_name, v.video_likecount
                FROM capstonepro_1.video AS v
                JOIN capstonepro_1.channel AS c ON v.channel_title = c.channel_name
                ORDER BY v.video_likecount DESC"""
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['video_title','channel_name','video_likecount'])
        st.write(df)
    elif option == "6. What is the total number of likes for each video, and what are their corresponding video names?":
        query="""
                SELECT v.video_title, SUM(v.video_likecount) AS Total_likes
                FROM capstonepro_1.video AS v
                GROUP BY v.video_title
                """
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['video_title','Total_likes'])
        st.write(df)
    elif option == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        query="""
                SELECT c.channel_name, SUM(v.video_viewcount) AS Total_views
                FROM capstonepro_1.video AS v
                JOIN capstonepro_1.channel AS c ON v.channel_title = c.channel_name
                GROUP BY c.channel_name
                """
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['channel_name','Total_views'])
        st.write(df)
    elif option == "8. What are the names of all the channels that have published videos in the year 2022?":
        query="""
                SELECT DISTINCT channel_title
                FROM capstonepro_1.video
                WHERE YEAR(video_publisheddate) = 2022
                """
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['channel_title','Total_views'])
        st.write(df)
    elif option == "9. what is the average duration of all videos in each channel and what are their corresponding channel names?":
        query="""
                SELECT v.channel_title, AVG(TIME_TO_SEC(v.video_duration)) AS avg_duration_seconds
                FROM capstonepro_1.video AS v
                GROUP BY v.channel_title
                """
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['channel_title','avg_duration_seconds'])
        st.write(df)
    elif option == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query="""
                SELECT v.video_title, c.channel_name, COUNT(c.comment_id) AS comment_count
                FROM capstonepro_1.video AS v
                JOIN capstonepro_1.comment AS c ON v.video_id = c.video_ID
                GROUP BY v.video_title, c.channel_name
                ORDER BY comment_count DESC;
                """
        mycursor.execute(query)
        results=mycursor.fetchall()
        df=pd.DataFrame(results, columns=['video_title','channel_name','comment_count'])
        st.write(df)
st.title("Data Analysis Zone")
st.write('(Hint: This zone analyse  data by using SQL queriesto join tables in the SQL data warehouse)')

option=st.selectbox(
    "Select a query to execute:",
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. what is the average duration of all videos in each channel and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'

    ]

)
if st.button("Execute Query"):
    execute_selected_query(option)




