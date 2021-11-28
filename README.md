# Использование продукта
Склонировать репозиторий к мебе
```
$ git clone https://github.com/kostyamyasso2002/SoCPC-hackaton
```
Ваши базы данных должны находиться по расположению ./data/datasets и называться как web_client.json, mobile_client.json, agg_usage.parquet, parent_operator.csv 
```
Затем постройте docker образ. 
```
$ docker build -t SoCPC-image SoCPC-hackaton/
```
Запустите получившийся Docker образ, чтобы запустить 
```
$ docker run --name SoCPC-image
```



2.1) Создание voice_traffic и data_traffic
Статус: Сделано
Стек: Apache Spark, Python
Описание: Здесь особо без идей: просто с помощью 2 исходных файлов собираем другие 2 по заданным формулам.

2.2)Создание client_profile
Статус: в процессе
Стек: Apache Spark, Python
Описание: тут тоже просто из 2 полученных бд создаем итоговую, отсорченную по месяцам.





