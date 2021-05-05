# python-tinkoff_invest

Дампы БД и фалы свечей: https://disk.yandex.ru/d/6HfiZBrhxp-_hQ?w=1

Старт в фоне :
	chmod +x demon.py # только один раз

	nohup demon.py &
	
Проверка запуска в фоне:
	ps ax | grep demon.py
	
Остановка демона запущенного в фоне:
	kill <pid> # pid из ps ax | grep demon.py
	
Добавление папки в архив:
	zip -r figis.zip ./figis
