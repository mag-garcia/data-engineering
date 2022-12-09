from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
import shutil
import uvicorn
import pandas as pd
import numpy as np

# importamos fastapi uvicorn

app = FastAPI( Title="Plataformas de streaming", 
description=" Esta API permite investigar los datos de las plataformas de streaming Amazon, Disney Plus, Hulu y Netflix",
version="1.0.0.0.1")

content =  pd.read_csv('Datasets/content.csv')

@app.get('/Maxima duracion segun tipo de film por plataforma y por anio/')
async def get_max_duration(plat:str, year:int, dur:str):
  plat_year_dur = content.loc[(content['platform']==plat) & (content['release_year']==year) & (content['duration_type']==dur)] 
  
  plat_year_dur['duration'] = plat_year_dur['duration'].astype('float64')
  
  max = plat_year_dur['duration'].max()

  title = plat_year_dur['title'].loc[plat_year_dur['duration'] == max].values[0]

      
  if dur == 'min':
    return {f'La mayor duracion en la plataforma {plat} la tiene la pelicula {title} con {max} minutos'}
  else: 
    return {f'La mayor duracion en la plataforma {plat} la tiene la serie {title} con {max} temporadas.'}

@app.get('/Cantidad de peliculas y series por plataforma/')
async def get_count_platform(plat:str):
  movies = content.loc[(content['platform']==plat) & (content['type']=='movie')].shape
  series = content.loc[(content['platform']==plat) & (content['type']=='tv show')].shape
  
  movies_cant = movies[0]
  series_cant = series[0]
      
  message = f'En la plataforma {plat} hay {movies_cant} peliculas y {series_cant} series'
  return {message}


@app.get('/Cantidad de veces que se repite un género y plataforma con mayor frecuencia del mismo/')
async def get_listedin (genre:str):
  genre_content = content.loc[(content['listed_in'].str.contains(genre))&(content['listed_in'].str.contains(genre+' ')==False)]
  plat_genre = genre_content['platform'].value_counts()
  plat_genre = plat_genre[plat_genre == plat_genre.max()].index[0]
  rep_genre = genre_content['platform'].value_counts().max()

  message = f'La plataforma con más series o películas del género {genre} es {plat_genre} con {rep_genre} repeticiones'
  return {message}


@app.get('/Actor que mas se repite segun plataforma y anio/')
async def get_actor(platform:str, year:int):
  cast = content[['platform','release_year','cast']]
  actors_list = cast.groupby(['platform','release_year'])['cast'].apply(lambda x: ','.join(x))

  actors = actors_list[platform][year]
  actors = actors.split(',')
  actors_set = set(actors)

  repeated = ('', 0) 

  for actor in actors_set:
    quant = actors.count(actor)

    if actor != 'No data':
      if quant > repeated[1]:
        repeated = (actor,quant)
    
  name = repeated[0]
  times= repeated[1]

  message = f'El actor que más se repite en la plataforma {platform} en el anio {year} es {name}, {times} veces.'
  return {message}
