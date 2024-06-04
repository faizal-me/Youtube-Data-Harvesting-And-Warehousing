import googleapiclient.discovery
import streamlit as st
import mysql.connector

#API key connection

def Api_connect():
    Api_Id="AIzaSyAYAULG3ma4Guh_J_DFyn54eHcW88_DKAA"

    api_service_name="youtube"
    api_version="v3"

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data


#sql connection 

mydb = mysql.connector.connect(                      
 host="localhost", 
 user="root",
 password="",
 database="youtubedata"
 )

mycursor = mydb.cursor(buffered=True)  

def store_to_sql(channel_id):
    # Check if the channel ID already exists
    check_channel_query = "SELECT COUNT(*) FROM Channel WHERE ChannelId = %s"
    mycursor.execute(check_channel_query, (channel_id,))
    (count,) = mycursor.fetchone()

    if count > 0:
        st.write("Channel ID already exists in the database.")
    else:
        # Get channel information
        channel_info = get_channel_info(channel_id)
        # Insert channel data into Channel table
        insert_channel_query = """
            INSERT INTO Channel (ChannelId, ChannelName, Description, PlaylistID, VideoCount, SubscriberCount, ViewCount)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        channel_values = (
            channel_info['Channel_Id'],
            channel_info['Channel_Name'],
            channel_info['Channel_Description'],
            channel_info['Playlist_Id'],
            channel_info['Total_Videos'],
            channel_info['Subscribers'],
            channel_info['Views']
        )
        mycursor.execute(insert_channel_query, channel_values)
        mydb.commit()

        # Get video IDs
        video_ids = get_videos_ids(channel_id)

        # Get video information
        video_info = get_video_info(video_ids)
        # Insert video data into Video table
        for video in video_info:
            insert_video_query = """
                INSERT INTO Video (VideoID, ChannelID, Title, Tags, ThumbnailURL, Description, PublishedDate, Duration, Views, Comments, FavouriteCount, Definition, CaptionStatus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            video_values = (
                video['Video_Id'],
                video['Channel_Id'],
                video['Title'],
                ','.join(video['Tags']) if video['Tags'] else None,
                video['Thumbnail'],
                video['Description'],
                video['Published_Date'],
                video['Duration'],
                video['Views'],
                video['Comments'],
                video['Favorite_Count'],
                video['Definition'],
                video['Caption_Status']
            )
            mycursor.execute(insert_video_query, video_values)
            mydb.commit()

        # Get comment information
        comment_info = get_comment_info(video_ids)
        # Insert comment data into Comment table
        for comment in comment_info:
            insert_comment_query = """
                INSERT INTO Comment (CommentID, VideoID, CommentText, CommentAuthor, CommentPublished)
                VALUES (%s, %s, %s, %s, %s)
            """
            comment_values = (
                comment['Comment_Id'],
                comment['Video_Id'],
                comment['Comment_Text'],
                comment['Comment_Author'],
                comment['Comment_Published']
            )
            mycursor.execute(insert_comment_query, comment_values)
            mydb.commit()
        st.write("Data Stored Successfully")

# Function to execute SQL query and fetch results
def execute_query(query):
    mydb = mysql.connector.connect(                      
     host="localhost", 
     user="root",
     password="",
     database="youtubedata"
    )
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    result = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    return result


# Function to display query results as table in Streamlit
def display_results(result):
    if result:
        st.table(result)
    else:
        st.write("No data found.")

# Streamlit app
def main():
    st.title("YouTube Data Harvesting and Warehousing")
    
    # Text box to enter channel ID
    channel_id = st.text_input("Enter Channel ID:")

    # Button to extract data
    if st.button("Extract Data"):
        st.write("Data extracted successfully.")

    # Button to store data to SQL
    if st.button("Store to SQL"):
        store_to_sql(channel_id)
      

    # Dropdown to select the question
    question_tosql = st.selectbox('**Select your Question**',
                                  ('1. What are the names of all the videos and their corresponding channels?',
                                   '2. Which channels have the most number of videos, and how many videos do they have?',
                                   '3. What are the top 10 most viewed videos and their respective channels?',
                                   '4. How many comments were made on each video, and what are their corresponding video names?',
                                   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                   '8. What are the names of all the channels that have published videos in the year 2022?',
                                   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                   '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

    # Execute SQL query based on selected question
    if st.button("Show Tables"):
        if question_tosql.startswith('1'):
            query = """
                SELECT Video.Title, Channel.ChannelName
                FROM Video
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
            """
        elif question_tosql.startswith('2'):
            query = """
                SELECT Channel.ChannelName, COUNT(*) AS VideoCount
                FROM Video
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
                GROUP BY Channel.ChannelID
                ORDER BY VideoCount DESC
            """
        elif question_tosql.startswith('3'):
            query = """
                SELECT Video.Title, Channel.ChannelName, Video.Views
                FROM Video
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
                ORDER BY Video.Views DESC
                LIMIT 10
            """
        elif question_tosql.startswith('4'):
            query = """
                SELECT Video.Title, COUNT(*) AS CommentCount
                FROM Comment
                INNER JOIN Video ON Comment.VideoID = Video.VideoID
                GROUP BY Video.VideoID
            """
        elif question_tosql.startswith('5'):
            query = """
                SELECT Video.Title, Channel.ChannelName, Video.Likes
                FROM Video
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
                ORDER BY Video.Likes DESC
                LIMIT 1
            """
        elif question_tosql.startswith('6'):
            query = """
                SELECT Video.Title, SUM(Video.Likes) AS TotalLikes, SUM(Video.Dislikes) AS TotalDislikes
                FROM Video
                GROUP BY Video.VideoID
            """
        elif question_tosql.startswith('7'):
            query = """
                SELECT Channel.ChannelName, SUM(Video.Views) AS TotalViews
                FROM Video
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
                GROUP BY Channel.ChannelID
            """
        elif question_tosql.startswith('8'):
            query = """
                SELECT DISTINCT Channel.ChannelName
                FROM Video
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
                WHERE YEAR(Video.PublishedDate) = 2022
            """
        elif question_tosql.startswith('9'):
            query = """
                SELECT Channel.ChannelName, AVG(Video.Duration) AS AvgDuration
                FROM Video
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
                GROUP BY Channel.ChannelID
            """
        elif question_tosql.startswith('10'):
            query = """
                SELECT Video.Title, Channel.ChannelName, COUNT(*) AS CommentCount
                FROM Comment
                INNER JOIN Video ON Comment.VideoID = Video.VideoID
                INNER JOIN Channel ON Video.ChannelID = Channel.ChannelID
                GROUP BY Video.VideoID
                ORDER BY CommentCount DESC
                LIMIT 1
            """
        else:
            st.write("Invalid selection.")

        if 'query' in locals():
            result = execute_query(query)
            display_results(result)

if __name__ == "__main__":
    main()
