import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

def read_csv_file():
    df = pd.read_csv('~/airflow/data/USvideos.csv')
    #"https://storage.googleapis.com/youtube_data_engineering/USvideos.csv"
    print(df.head())

    return df.to_json()

def read_json_file():
    df = pd.read_json('~/airflow/data/US_category_id.json')
    #"https://storage.googleapis.com/youtube_data_engineering/US_category_id.json"
    print(df.head())

    return df.to_json()

def preprocessing(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(json_data)

    df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m')
    df['publish_time'] = pd.to_datetime(df['publish_time'], utc=True).dt.tz_localize(None)
    # object to string
    df['video_id'] = df['video_id'].astype("string")
    df['description'] = df['description'].astype("string")
    df['title'] = df['title'].astype("string")
    df['channel_title'] = df['channel_title'].astype("string")
    df['tags'] = df['tags'].astype("string")
    df['thumbnail_link'] = df['thumbnail_link'].astype("string")

    # Add fact_id as SURROGATE Key
    df = df.drop_duplicates().reset_index(drop=True)
    df['fact_id'] = df.index
    # rename category_id
    df = df.rename(columns={"category_id": "category_no"}, errors="raise")
    # add column tags_count
    df['tag_count'] = df['tags'].astype('string').apply(lambda x: len(x.split('|')))

    print(df.head())

    return df.to_json()

def load_datetime_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)

    datetime_dim = df[['trending_date','publish_time']].drop_duplicates().reset_index(drop=True)
    
    datetime_dim['trending_date'] = pd.to_datetime(datetime_dim['trending_date'])
    datetime_dim['trending_day'] = datetime_dim['trending_date'].dt.day
    datetime_dim['trending_month'] = datetime_dim['trending_date'].dt.month
    datetime_dim['trending_year'] = datetime_dim['trending_date'].dt.year
    datetime_dim['trending_weekday'] = datetime_dim['trending_date'].dt.weekday
    datetime_dim['trending_weekdayname'] = datetime_dim['trending_date'].dt.day_name().astype("string")

    datetime_dim['publish_hour'] = datetime_dim['publish_time'].dt.hour
    datetime_dim['publish_day'] = datetime_dim['publish_time'].dt.day
    datetime_dim['publish_month'] = datetime_dim['publish_time'].dt.month
    datetime_dim['publish_year'] = datetime_dim['publish_time'].dt.year
    datetime_dim['publish_weekday'] = datetime_dim['publish_time'].dt.weekday
    datetime_dim['publish_weekdayname'] = datetime_dim['publish_time'].dt.day_name().astype("string")
    
    datetime_dim['datetime_id'] = datetime_dim.index
    # order columns 
    datetime_dim = datetime_dim[['datetime_id', 'trending_date', 'trending_day', 'trending_month', 'trending_year', 'trending_weekday', 'trending_weekdayname',
                                 'publish_time', 'publish_hour', 'publish_day', 'publish_month', 'publish_year', 'publish_weekday','publish_weekdayname']]
    
    print(datetime_dim.head())

    return datetime_dim.to_json()

def load_category_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)
    json_data2 = ti.xcom_pull(task_ids='read_json_file')
    df2 = pd.read_json(json_data2)
    
    category_dict = {}
    for item in df2['items']:
        category_dict[item['id']] = item['snippet']['title']
    
    category_dim = df[['category_no']].drop_duplicates().reset_index(drop=True)
    category_dim['category_id'] = category_dim.index
    category_dim['category_title'] = category_dim['category_no'].astype('string').map(category_dict)
    category_dim = category_dim[['category_id', 'category_no', 'category_title']]

    print(category_dim.head())

    return category_dim.to_json()
    
def load_title_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)

    title_dim = df[['title']].drop_duplicates().reset_index(drop=True)
    title_dim['title_id'] = title_dim.index
    title_dim = title_dim[['title_id','title']]
  
    print(title_dim.head())

    return title_dim.to_json()

def load_channel_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)
    
    channel_dim = df[['channel_title']].drop_duplicates().reset_index(drop=True)
    channel_dim['channel_id'] = channel_dim.index
    channel_dim = channel_dim[['channel_id','channel_title']]

    print(channel_dim.head())

    return channel_dim.to_json()

def load_tags_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)

    tags_dim = df[['tags']].drop_duplicates().reset_index(drop=True)
    tags_dim['tags_id'] = tags_dim.index
    tags_dim = tags_dim[['tags_id','tags']]

    print(tags_dim.head())

    return tags_dim.to_json()

def load_videoDesc_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)
    
    videoDesc_dim = df[['description']].drop_duplicates().reset_index(drop=True)
    videoDesc_dim['videoDesc_id'] = videoDesc_dim.index
    videoDesc_dim = videoDesc_dim[['videoDesc_id','description']]
    
    print(videoDesc_dim.head())

    return videoDesc_dim.to_json()

def load_settings_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)
    
    settings_dim = df[['comments_disabled', 'ratings_disabled', 'video_error_or_removed']].drop_duplicates().reset_index(drop=True)
    settings_dim['settings_id'] = settings_dim.index
    
    settings_dim = settings_dim[['settings_id','comments_disabled', 'ratings_disabled', 'video_error_or_removed']]

    print(settings_dim.head())

    return settings_dim.to_json()

def load_thumbnail_link_dim_table(ti):
    json_data = ti.xcom_pull(task_ids='preprocessing')
    df = pd.read_json(json_data)
    
    thumbnail_link_dim = df[['thumbnail_link']].drop_duplicates().reset_index(drop=True)
    thumbnail_link_dim['thumbnail_link_id'] = thumbnail_link_dim.index
    thumbnail_link_dim = thumbnail_link_dim[['thumbnail_link_id','thumbnail_link']]

    print(thumbnail_link_dim.head())

    return thumbnail_link_dim.to_json()


def load_fact_table(ti):
    json_data = ti.xcom_pull(task_ids='load_datetime_dim_table')
    json_data2 = ti.xcom_pull(task_ids='load_category_dim_table')
    json_data3 = ti.xcom_pull(task_ids='load_title_dim_table')
    json_data4 = ti.xcom_pull(task_ids='load_channel_dim_table')
    json_data5 = ti.xcom_pull(task_ids='load_tags_dim_table')
    json_data6 = ti.xcom_pull(task_ids='load_videoDesc_dim_table')
    json_data7 = ti.xcom_pull(task_ids='load_settings_dim_table')
    json_data8 = ti.xcom_pull(task_ids='load_thumbnail_link_dim_table')
    json_data9 = ti.xcom_pull(task_ids='preprocessing')
    
    datetime_dim = pd.read_json(json_data)
    category_dim = pd.read_json(json_data2)
    title_dim = pd.read_json(json_data3)
    channel_dim = pd.read_json(json_data4)
    tags_dim = pd.read_json(json_data5)
    videoDesc_dim = pd.read_json(json_data6)
    settings_dim = pd.read_json(json_data7)
    thumbnail_link_dim = pd.read_json(json_data8)
    df = pd.read_json(json_data9)
    
    fact_table = df.merge(datetime_dim, how = "left") \
                 .merge(category_dim,  how = "left") \
                 .merge(title_dim, how = "left") \
                 .merge(channel_dim, how = "left") \
                 .merge(tags_dim, how = "left") \
                 .merge(videoDesc_dim, how = "left")\
                 .merge(settings_dim, how = "left") \
                 .merge(thumbnail_link_dim, how = "left") \
                 [['fact_id','video_id','datetime_id','category_id','title_id','channel_id','tags_id','videoDesc_id',
                  'settings_id','thumbnail_link_id', 'tag_count', 'views', 'likes', 'dislikes', 'comment_count']]
    
    fact_table = fact_table.rename(columns={"views": "view_count", "likes": "like_count", "dislikes": "dislike_count"})
    print(fact_table.columns)

    return fact_table.to_json()

def write_mysql_db(ti):
    
    json_data = ti.xcom_pull(task_ids='load_datetime_dim_table')
    json_data2 = ti.xcom_pull(task_ids='load_category_dim_table')
    json_data3 = ti.xcom_pull(task_ids='load_title_dim_table')
    json_data4 = ti.xcom_pull(task_ids='load_channel_dim_table')
    json_data5 = ti.xcom_pull(task_ids='load_tags_dim_table')
    json_data6 = ti.xcom_pull(task_ids='load_videoDesc_dim_table')
    json_data7 = ti.xcom_pull(task_ids='load_settings_dim_table')
    json_data8 = ti.xcom_pull(task_ids='load_thumbnail_link_dim_table')
    json_data9 = ti.xcom_pull(task_ids='load_fact_table')

    datetime_dim = pd.read_json(json_data)
    category_dim = pd.read_json(json_data2)
    title_dim = pd.read_json(json_data3)
    channel_dim = pd.read_json(json_data4)
    tags_dim = pd.read_json(json_data5)
    videoDesc_dim = pd.read_json(json_data6)
    settings_dim = pd.read_json(json_data7)
    thumbnail_link_dim = pd.read_json(json_data8)
    fact_table = pd.read_json(json_data9)

    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="evan",
            password="Wrwr1234"
        )
        print("Connection established")
        cursor = mydb.cursor()
        cursor.execute("create database if not exists youtube")
        mydb.commit()
        print("Database created successfully")
        cursor.execute("use youtube")
    
    except mysql.connector.Error as err:
        print("An error occurred:", err)

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host="localhost", db="youtube", user="evan", pw="Wrwr1234"))
    fact_table.to_sql('fact_table', engine, if_exists='replace', index=False)
    datetime_dim.to_sql('datetime_dim', engine, if_exists='replace', index=False)
    category_dim.to_sql('category_dim', engine, if_exists='replace', index=False)
    title_dim.to_sql('title_dim', engine, if_exists='replace', index=False)
    channel_dim.to_sql('channel_dim', engine, if_exists='replace', index=False)
    tags_dim.to_sql('tags_dim', engine, if_exists='replace', index=False)
    videoDesc_dim.to_sql('videodesc_dim', engine, if_exists='replace', index=False)
    settings_dim.to_sql('settings_dim', engine, if_exists='replace', index=False)
    thumbnail_link_dim.to_sql('thumbnail_link_dim', engine, if_exists='replace', index=False)

    cursor.close()
    mydb.close()