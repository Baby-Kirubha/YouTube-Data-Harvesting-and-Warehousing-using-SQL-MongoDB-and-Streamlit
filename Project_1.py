#!/usr/bin/env python
# coding: utf-8


KEY="AIzaSyBDaSf1NNopjNeggYQkRVDoRJFroZTVrOg"

import googleapiclient.discovery
from pprint import pprint
import re
import pandas as pd
import streamlit as st
st.sidebar.title(":red[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
st.sidebar.header(":green[Steps done in the project]")
st.sidebar.write("Enter Channel ID")
st.sidebar.divider()
st.sidebar.write("Extraction of data")
st.sidebar.divider()
st.sidebar.write("Uploading data to mongoDB")
st.sidebar.divider()
st.sidebar.write("Pushing data from mongoDb to MySQL")
st.sidebar.divider()

#TO GET THE CHANNEL DETAILS
def channelDetails(channel_id):
    import googleapiclient.discovery
    import re
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

# Get credentials and create an API client

    youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=KEY)

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id   )
    responseChannel = request.execute()
    

#channel details
    
    for i in responseChannel["items"]:
        info=dict(Channel_Name =i['snippet']['title'],
                 Channel_Id=i["id"],
                 Subscription_Count=i["statistics"]["subscriberCount"],
                 Channel_Views=i["statistics"]["viewCount"],
                 Channel_Description=i["snippet"]["description"],
                 Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    pl=responseChannel["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            
    return info,pl

#to get video info:
def video_info(pl):
    import googleapiclient.discovery
    import re
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"
    next_Page_Token = None
    video_ids = []
    youtube = googleapiclient.discovery.build(
                api_service_name, api_version, developerKey=KEY)
    while True:
            request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=pl,
                    maxResults=50,
                    pageToken=next_Page_Token

                )
            response = request.execute()
            for item in response.get('items', ''):
                video_ids.append(item['contentDetails']['videoId'])
            next_Page_Token = response.get('nextPageToken')
            if not next_Page_Token:
                break
    basket=[]
    for i in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i
        )
        response = request.execute()
        
        for i in response["items"]:
            try:
                Tags=''.join(i["snippet"]["tags"][0:2])
            except:
                Tags="NaN"
            try:
                Comment_Count=i["statistics"]["commentCount"]
            except:
                Comment_Count="0"
            try:
                LikeCount=i["statistics"]["likeCount"]
            except:
                LikeCount="0"
            try:
                DislikeCount=i["statistics"]["dislikeCount"]
            except:
                DislikeCount="0"
            data=dict(Channel_Name=i["snippet"]["channelTitle"],
                      Channel_ID=i["snippet"]["channelId"],
                      Playlist_Id=pl,
                     Video_Id=i["id"],
                     Video_Name=i["snippet"]["title"],
                     Video_Description=i["snippet"]["description"],
                     Tags=Tags,
                     Comment_Count=Comment_Count,
                     PublishedAt=re.sub("T.*$","",i["snippet"]["publishedAt"]),
                     ViewCount=i["statistics"]["viewCount"],
                     LikeCount=LikeCount,
                     DislikeCount=DislikeCount,
                     FavoriteCount=i["statistics"]["favoriteCount"],
                     Duration=i["contentDetails"]["duration"].replace("P","").replace("T","").replace("H",":").replace("M",":").replace("S",""),
                     Thumbnail=i["snippet"]["thumbnails"]["default"]["url"],
                     Caption_Status=i["contentDetails"]["caption"])
            basket.append(data)
    
    return [basket,video_ids]

#to get comments of relevent video:
def  comments(video_ids):
    import googleapiclient.discovery
    import re
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"
    youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=KEY)
    basket1=[]
    for i in video_ids:
        try:
            request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=i)

            response = request.execute()

            for i in response["items"]:
                data1=dict(Video_id=i["snippet"]["videoId"],
                              Comment_ID=i["id"],
                              Comment_Text=i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                              Comment_Author=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                              Comment_published_At=re.sub("T.*$","",i["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                )
                basket1.append(data1)
        except:
            pass
    
    return basket1

 #function to export to mongodb                

# st.button("Extract Data")

#to extract data
def main(channel_id):
    C=channelDetails(channel_id)
    V=video_info(C[0]["Playlist_Id"])
    CM=comments(V[1])
    output={"channel_details":C[0],
         "video_details":V[0],
         "comment_details":CM} 
    return output

#extract data
channel_id=st.text_input("Enter Channel ID:")
if st.button("Extract Data"):
    coll=main(channel_id)




# to upload to mongo
def mongo(coll):
    import pymongo
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["Project1"]
    collection=mydb.Channel_Details
    collection.insert_one(coll)
    return collection 
    
 
# In[7]:

if st.button("Upload to MongoDB"):
    coll=main(channel_id)
    upload=mongo(coll)


# In[85]:


#to extract data from mongodb and pushing to sql
def sql():
    import pymongo
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["Project1"]
    collection=mydb.Channel_Details
    import pandas as pd
    import numpy as np
    import pymysql
    myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="11721200#Baby")
    cur=myconnection.cursor()
    cur.execute("create database if not exists pro1")
    myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="11721200#Baby",database="pro1")
    cur=myconnection.cursor()
    #creating table for channel_details
    cur.execute('''create table if not exists ChannelDetails(Channel_Name varchar(100),Channel_Id varchar(100)primary key,
                Subscription_Count int,Channel_Views int,Channel_Description text,Playlist_Id varchar(100))''')
    sqlch='''insert ignore into ChannelDetails (Channel_Name ,Channel_Id ,Subscription_Count,
    Channel_Views,Channel_Description,Playlist_Id)values(%s,%s,%s,%s,%s,%s)'''
    for i in collection.find({},{'_id':0,'channel_details':1}):
        cur.execute(sqlch,tuple(i['channel_details'].values()))
        myconnection.commit()
    #creating table for video_details
    cur.execute('''create table if not exists video_details(Channel_Name varchar(100),Channel_ID varchar(100),Playlist_Id varchar(100),Video_Id varchar(60)primary key,Video_Name varchar(100),
                Video_Description varchar(7000),Tags varchar(400),Comment_Count int,PublishedAt varchar(60),ViewCount int,LikeCount int,
                DislikeCount int,FavoriteCount int,Duration varchar(60),Thumbnail varchar(60),Caption_Status varchar(60))''')
    sql='''insert ignore into video_details (Channel_Name,Channel_ID ,Playlist_Id,Video_Id,Video_Name,Video_Description,Tags,
             Comment_Count,PublishedAt ,ViewCount,LikeCount,DislikeCount,FavoriteCount,Duration,Thumbnail ,
             Caption_Status)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
    for i in collection.find({},{'_id':0,'video_details':1}):
        for j in i["video_details"]:
            cur.execute(sql,tuple(j.values()))
            myconnection.commit()
    #creating table for comments
    cur.execute('''create table if not exists comment_details(Video_id varchar(30),Comment_ID varchar(30)primary key,
            Comment_Text varchar(800),Comment_Author varchar(100),Comment_published_At varchar(200))''')
    sqlch='''insert ignore into comment_details(Video_id ,Comment_ID,Comment_Text ,Comment_Author ,Comment_published_At)
           values(%s,%s,%s,%s,%s)'''
        
    for i in collection.find({},{'_id':0,'comment_details':1}):
        for j in i["comment_details"]:
            cur.execute(sqlch,tuple(j.values()))
            myconnection.commit()
    return "inserted into sql successfully"

#pushing to sql
if st.button("Push to SQL"):
    sql()


# In[14]:

import pandas as pd
import pymysql
myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="11721200#Baby")
cur=myconnection.cursor()
cur.execute("create database if not exists pro1")
myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd="11721200#Baby",database="pro1")
cur=myconnection.cursor()

#questions
Q=st.selectbox("Select your question",("Select","1.What are the names of all the videos and their corresponding channels?",
                                       "2.Which channels have the most number of videos, and how many videos do they have?",
                                       "3.What are the top 10 most viewed videos and their respective channels?",
                                       "4.How many comments were made on each video, and what are their corresponding video names?",
                                       "5.Which video has the highest number of likes and what is the corresponding channel name?",
                                       "6.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                       "7.What is the total number of likes  for each video, and what are their corresponding video names?",
                                       "8.What is the total number of views for each channel, and what are their corresponding channel names?",
                                       "9.What are the names of all the channels that have published videos in the year 2022?",
                                       "10.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                       "11.Which video has the highest number of comments, and what is the corresponding channel name?",
                                       "12.Which top 10 videos have the highest number of comments, and what are their corresponding channel names?"))

if Q=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select Video_Name,Channel_Name from video_details'''
    cur.execute(query1)
    myconnection.commit()

    d1=cur.fetchall()
    df=pd.DataFrame(d1,columns=["Name of the video","Channel name"])

    st.write(df)

if Q=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select Channel_Name, count(Video_Id) from video_details group by 1 order by count(Video_Id) desc limit 1'''
    cur.execute(query2)
    myconnection.commit()

    d2=cur.fetchall()
    df=pd.DataFrame(d2,columns=["Channel name","Total no. of videos"])

    st.write(df)

if Q=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select Video_Name,Channel_Name,ViewCount from video_details order by ViewCount desc limit 10'''
    cur.execute(query3)
    myconnection.commit()

    d3=cur.fetchall()
    df=pd.DataFrame(d3,columns=["Video title","Channel name","No. of views"])
    
    st.write(df)

if Q=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select Video_Name,Comment_Count from video_details'''
    cur.execute(query4)
    myconnection.commit()

    d4=cur.fetchall()
    df=pd.DataFrame(d4,columns=["Video title","Comment count"])

    st.write(df)

if Q=="5.Which video has the highest number of likes and what is the corresponding channel name?":
    query5='''select Video_Name,Channel_name,LikeCount from video_details order by LikeCount desc limit 1'''
    cur.execute(query5)
    myconnection.commit()

    d5=cur.fetchall()
    df=pd.DataFrame(d5,columns=["Video title","Channel name","Total likes"])

    st.write(df)

if Q=="6.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query6='''select Video_Name,LikeCount from video_details order by LikeCount desc'''
    cur.execute(query6)
    myconnection.commit()

    d6=cur.fetchall()
    df=pd.DataFrame(d6,columns=["Video title","No. of likes"])

    st.write(df)

if Q=="7.What is the total number of likes  for each video, and what are their corresponding video names?":
    query7='''select Video_Name,LikeCount from video_details'''
    cur.execute(query7)
    myconnection.commit()

    d7=cur.fetchall()
    df=pd.DataFrame(d7,columns=["Video title","Likes"])
    st.write(df)


if Q=="8.What is the total number of views for each channel, and what are their corresponding channel names?":
    query8='''select Channel_Name,Channel_Views from channeldetails'''
    cur.execute(query8)
    myconnection.commit()

    d8=cur.fetchall()
    df=pd.DataFrame(d8,columns=["Channel name","Total views"])

    st.write(df)
    st.bar_chart(data=df,x="Channel name",y="Total views")

if Q=="9.What are the names of all the channels that have published videos in the year 2022?":
    query9='''select Channel_Name,PublishedAt from video_details where monthname(PublishedAt)>='january' and year(PublishedAt)=2022'''
    cur.execute(query9)
    myconnection.commit()

    d9=cur.fetchall()
    df=pd.DataFrame(d9,columns=["Channel name","Uploaded date"])

    st.write(df)

if Q=="10.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query10='''select  Channel_Name, round(avg(Duration),2) as Avg_Duration from video_details group by 1'''
    cur.execute(query10)
    myconnection.commit()

    d10=cur.fetchall()
    df=pd.DataFrame(d10,columns=["Channel name","Duration"])

    st.write(df)

if Q=="11.Which video has the highest number of comments, and what is the corresponding channel name?":
    query11='''select Video_Name, Channel_Name,Comment_Count from video_details order by Comment_Count desc limit 1'''
    cur.execute(query11)
    myconnection.commit()

    d11=cur.fetchall()
    df=pd.DataFrame(d11,columns=["Video title","Channel name","Comment count"])

    st.write(df)

if Q=="12.Which top 10 videos have the highest number of comments, and what are their corresponding channel names?":
    query12='''select Video_Name, Channel_Name,Comment_Count from video_details order by Comment_Count desc limit 10'''
    cur.execute(query12)
    myconnection.commit()

    d12=cur.fetchall()
    df=pd.DataFrame(d12,columns=["Video title","Channel name","Comment count"])
    st.bar_chart(data=df,x="Video title",y="Comment count")
    st.write(df)
