import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Retrieve Spotify API credentials from environment variables
    client_id = os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    
    # Set up Spotify client credentials manager
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # Define the Spotify playlist link and extract the URI
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?_authfailed=1"
    playlist_uri = playlist_link.split("/")[-1].split("?")[0]
    
    # Fetch the playlist tracks data from Spotify
    data = sp.playlist_tracks(playlist_uri)
    
    # Set up S3 client
    client = boto3.client("s3")
    
    # Create a file name with a timestamp
    file_name = "spotify_raw_" + str(datetime.now()) + ".json"
    
    # Upload the JSON data to the specified S3 bucket and path
    client.put_object(
        Bucket="spotify-etl-pipeline-project-sai",
        Key="raw_data/to_processed/" + file_name,
        Body=json.dumps(data)
    )
    
