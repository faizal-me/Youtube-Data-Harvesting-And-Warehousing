#Table creation for channels,playlists,videos,comments
mycursor.execute("""CREATE TABLE Channel (
    ChannelId VARCHAR(50) PRIMARY KEY,
    ChannelName VARCHAR(255),
    Description TEXT,
    PlaylistID VARCHAR(50),
    VideoCount INT,
    SubscriberCount INT,
    ViewCount INT
)""")

mycursor.execute("""CREATE TABLE Video (
    VideoID VARCHAR(50) PRIMARY KEY,
    ChannelID VARCHAR(50),
    Title VARCHAR(255),
    Tags TEXT,
    ThumbnailURL VARCHAR(255),
    Description TEXT,
    PublishedDate DATETIME,
    Duration VARCHAR(50),
    Views INT,
    Comments INT,
    FavouriteCount INT,
    Definition VARCHAR(20),
    CaptionStatus VARCHAR(20),
    FOREIGN KEY (ChannelID) REFERENCES Channel(ChannelID)
)""")

mycursor.execute("""CREATE TABLE Comment (
    CommentID VARCHAR(50) PRIMARY KEY,
    VideoID VARCHAR(50),
    CommentText TEXT,
    CommentAuthor VARCHAR(255),
    CommentPublished DATETIME,
    FOREIGN KEY (VideoID) REFERENCES Video(VideoID)
)""")