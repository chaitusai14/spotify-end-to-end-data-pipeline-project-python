import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list = []
    for row in data['items']:
        Album_Id = row['track']['album']['id']
        Album_name = row['track']['album']['name']
        Album_Release_date = row['track']['album']['release_date']
        Total_tracks = row['track']['album']['total_tracks']
        Album_URL = row['track']['album']['external_urls']['spotify']
        dict_albums = {'Album_Id': Album_Id, 'Album_Name': Album_name,'Album_Release_Date': Album_Release_date, 
                                 'Total_Tracks': Total_tracks, 'Album_URL': Album_URL}
        album_list.append(dict_albums)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'Artist_Id':artist['id'], 'Artist_Name':artist['name'], 'External_URL': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list  
    
def songs(data):
    songs_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        dict_songs = {'Song_Id':song_id,'Song_Name':song_name,'Duration_ms':song_duration,'Song_URL':song_url,
                    'Popularity':song_popularity,'Song_Added':song_added,'Album_Id':album_id}
        songs_list.append(dict_songs)
    return songs_list   

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = "spotify-etl-pipeline-project-sai"
    Key = "raw_data/to_processed/"
    data = []
    keys = []
    
    # Fetch JSON files from the specified S3 bucket and prefix
    for i in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = i['Key']
        if file_key.split(".")[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            data.append(jsonObject)
            keys.append(file_key)
            
    # Process each JSON object
    for i in data:
        album_list = album(i)
        artist_list = artist(i)
        songs_list = songs(i)
        
        # Convert to DataFrames
        df_album = pd.DataFrame(album_list)
        df_album = df_album.drop_duplicates(subset = ['Album_Id'])
        df_album['Album_Release_Date'] = pd.to_datetime(df_album['Album_Release_Date'])
        
        df_artist = pd.DataFrame(artist_list)
        df_artist = df_artist.drop_duplicates(subset = ['Artist_Id'])
        
        df_songs = pd.DataFrame(songs_list)
        df_songs['Song_Added'] = pd.to_datetime(df_songs['Song_Added'])
        
        
        # Upload song data
        key_song = "transformed_data/songs_data/songs_transformed_dataframe_" + str(datetime.now()) + ".csv"
        
        # This object acts as a buffer to temporarily hold the CSV data before it is uploaded to S3.
        song_buffer = StringIO()
        df_songs.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = key_song, Body = song_content)
        
        
        # Upload album data
        key_album = "transformed_data/album_data/album_transformed_dataframe_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        df_album.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = key_album, Body = album_content)
        
        # Upload artist data
        key_artist = "transformed_data/artist_data/artist_transformed_dataframe_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        df_artist.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = key_artist, Body = artist_content)
        
    
    #Copy each object from raw_data/to_processed to raw_data/processed.
    #Delete the original object once it has been copied.    
    s3_resource = boto3.resource('s3')
    for key in keys:
        copy_source = { 
                'Bucket': Bucket,
                'Key': key}
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
    
    
