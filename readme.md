Набор парсеров

I. Требования:
    Тестировалось на Ubuntu 14.04
    Возможно потребуются дополнительные библиотеки:

    sudo apt-get install python-pip
    sudo pip install bs4
    sudo pip install scraperwiki


II. Использование
    git clone https://github.com/kuzovkov/scrapers.git

    Вариант на Python:

    cd scrapers/python
    chmod a+e run
    ./run -d <сайт-донор> [-t --test] [-c --clear-old]

    Описание опций:
     -d или --donor указываем какой сайт парсим, этом параметр должен совпадать с именем
     одного из каталогов в каталоге donors. В этих каталогах содержаться парсеры для конкретного сайта

     -t или --test если указан, то происходит запуск не основого метода в классе скрапера, а тестового, может быть полезно при отладке

     -c или --clear-old если указан то перед парсингом происходит очистка таблицы в базе от старых данных


     База данныз SQLite будет создана в каталоге python/db
     Имя базы будет <сайт-донор>.sqlite

     Файл schema.ini содержит схему базы данных (название таблиц, имена и типы полей)
     Нужен только если будем использовать свой пакет для работы с БД (storage).
     Пока не используется.
