from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pymongo
import json

import csv
import pprint
from csv import DictReader

#Create the client and database
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mongoDatabase"]

#csvReader reads a csv file and coverts it into a dictionary
def csvReader(path):
    with open(path, 'r', encoding="utf8") as f:
        dict_reader = DictReader(f)
        data = list(dict_reader)
    return data

#Creating the dictionary variables
movies = csvReader("movies.csv")

year = ""
title = ""
for movie in movies:
    title = movie['title']
    front = len(title) - 5
    back = len(title) - 1
    year = title[front:back]
    #print(year)
    movie["year"] = year

#print(movies)

ratings = csvReader("ratings.csv")
tags = csvReader("tags.csv")

#Creating the collection variables
movies_col = mydb["movies"]
ratings_col = mydb["ratings"]
tags_col = mydb["tags"]

#Inserting the documents into the collections
movies_data = movies_col.insert_many(movies)
ratings_data = ratings_col.insert_many(ratings)
tags_data = tags_col.insert_many(tags)

#Number 4-5
movies_data = movies_col.insert_one({"movieId": "99993", "title":"Spider-Man: Across the Spider-Verse (2023)", "genres":"Animation|Action-Adventure|Family|Fantasy|Sci-Fi", "year": "2023"})
ratings_data = ratings_col.insert_one({"userId": "100", "movieId": "99993", "rating": "2", "timestamp": "999873732"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99993", "tag": "marvel comics", "timestamp": "1537098603"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99993", "tag": "based on comic", "timestamp": "1537098604"})

movies_data = movies_col.insert_one({"movieId": "99994", "title":"12 Angry Men (1957)", "genres":"Crime|Drama", "year": "1957"})
ratings_data = ratings_col.insert_one({"userId": "100", "movieId": "99994", "rating": "5", "timestamp": "999873733"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99994", "tag": "jury", "timestamp": "1537098605"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99994", "tag": "dialogue driven", "timestamp": "1537098606"})

movies_data = movies_col.insert_one({"movieId": "99995", "title":"The Good, the Bad and the Ugly (1966)", "genres":"Adventure|Western", "year": "1966"})
ratings_data = ratings_col.insert_one({"userId": "100", "movieId": "99995", "rating": "5", "timestamp": "999873734"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99995", "tag": "shoot-out", "timestamp": "1537098607"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99995", "tag": "civil war", "timestamp": "1537098608"})

movies_data = movies_col.insert_one({"movieId": "99996", "title":"Whiplash (2014)", "genres":"Drama|Music", "year": "2014"})
ratings_data = ratings_col.insert_one({"userId": "100", "movieId": "99996", "rating": "4", "timestamp": "999873735"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99996", "tag": "jazz music", "timestamp": "1537098609"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99996", "tag": "emotional abuse", "timestamp": "1537098610"})

movies_data = movies_col.insert_one({"movieId": "99997", "title":"Fight Club (1999)", "genres":"Drama", "year": "1999"})
ratings_data = ratings_col.insert_one({"userId": "100", "movieId": "99997", "rating": "4", "timestamp": "999873736"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99997", "tag": "surprise ending", "timestamp": "1537098611"})
tags_data = tags_col.insert_one({"userId": "100", "movieId": "99997", "tag": "insomnia", "timestamp": "1537098612"})

#6a

stage_group_year_6a = {
    "$group": {
         "_id": "$year",
         "movie_count": { "$sum": 1}
    }
}

stage_sort_year_ascending_6a = {
    "$sort": {"_id": pymongo.ASCENDING}
}

pipeline_1 = [
    stage_group_year_6a, stage_sort_year_ascending_6a,
]
results_6a = movies_col.aggregate(pipeline_1)
for movie in results_6a:
   print(movie)

#6b

stage_group_genre_6b = {
    "$group": {
         "_id": "$genres",
         "genre_count": { "$sum": 1}
    }
}

stage_sort_genre_ascending_6b = {
    "$sort": {"_id": pymongo.ASCENDING}
}

pipeline_2 = [
    stage_group_genre_6b, stage_sort_genre_ascending_6b,
]
results_6b = movies_col.aggregate(pipeline_2)
for movie in results_6b:
   print(movie)

#6c

stage_group_ratings_6c = {
    "$group": {
         "_id": "$rating",
         "rating_count": { "$sum": 1}
    }
}

stage_sort_ratings_ascending_6c = {
    "$sort": {"_id": pymongo.ASCENDING}
}

pipeline_3 = [
    stage_group_ratings_6c, stage_sort_ratings_ascending_6c,
]
results_6c = ratings_col.aggregate(pipeline_3)
for movie in results_6c:
   print(movie)

#6d

stage_group_tag_6d = {
    "$group": {
         "_id": "$tag",
         "tag_count": { "$sum": 1}
    }
}

stage_sort_tag_ascending_6d = {
    "$sort": {"tag_count": pymongo.ASCENDING}
}

pipeline_4 = [
    stage_group_tag_6d, stage_sort_tag_ascending_6d,
]
results_6d = tags_col.aggregate(pipeline_4)
for movie in results_6d:
   print(movie)
   #6E: "In Netflix queue" is the most popular tag at 3930 tags.