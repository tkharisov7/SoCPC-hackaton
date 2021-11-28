# Как использовать продукт? 
Склонируйте репозиторий 
```
$ git clone https://github.com/kostyamyasso2002/SoCPC-hackaton
```
Соберите docker образ
```
$ docker built -t SoCPC-image SoCPC-hackaton/
```
Запустите docker file
```
$ docker run --mount src=$(pwd)/data",target="/data/",type=bind --cpus 4 -m 8000M --name SoCPC-image 
```
Подождать когда image соберется. Полученные данные будут лежать в docker container по адресу /data
Чтобы их скопировать можно воспользоваться командой:
```
$ docker cp --name SoCPC-image .
```

Часть 2

2.1) Создание voice_traffic и data_traffic
Статус: Сделано
Стек: Apache Spark, Python
Описание: Здесь особо без идей: просто с помощью 2 исходных файлов собираем другие 2 по заданным формулам.
Время работы: 10 секунд

2.2)Создание client_profile
Статус: в процессе
Стек: Apache Spark, Python
Описание: тут тоже просто из 2 полученных бд создаем итоговую, отсорченную по месяцам.
Время работы:

2.3)Загнать все в Aerospike
Статус: Не осилили и решили вместо него использовать примитивные способы хранения данных
Время работы: медленно



