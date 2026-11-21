.PHONY: light-up hive hive-refresh hive-check hive-demo dashboard streaming stop-optional check test postgres-shell hdfs-list status batch

light-up:
	bash ./start-lightweight.sh

hive:
	bash ./start-hive.sh

hive-refresh:
	podman compose --profile hive -f ./docker-compose.yml exec -T hiveserver2 \
		/opt/hive/bin/beeline \
		-u 'jdbc:hive2://localhost:10000/default' \
		-f /tmp/refresh_partitions.sql

hive-check:
	podman compose --profile hive -f ./docker-compose.yml exec hiveserver2 \
		/opt/hive/bin/beeline \
		-u 'jdbc:hive2://localhost:10000/realtime_warehouse' \
		-e 'SHOW TABLES; DESCRIBE gold_daily_transaction_metrics; SELECT * FROM gold_daily_transaction_metrics LIMIT 5;'

hive-demo:
	podman compose --profile hive -f ./docker-compose.yml exec hiveserver2 \
		/opt/hive/bin/beeline \
		-u 'jdbc:hive2://localhost:10000/realtime_warehouse' \
		-f /tmp/demo_queries.sql

dashboard:
	bash ./start-dashboard.sh

streaming:
	podman compose --profile streaming -f ./docker-compose.yml up -d \
		kafka-init streaming-enricher

batch:
	podman compose --profile batch -f ./docker-compose.yml up -d warehouse-loader

postgres-shell:
	podman compose -f ./docker-compose.yml exec postgres \
		psql -U warehouse -d realtime_warehouse

hdfs-list:
	podman compose -f ./docker-compose.yml exec namenode hdfs dfs -ls -R /warehouse

status:
	podman ps --format 'table {{.Names}}\t{{.Status}}'

stop-optional:
	bash ./stop-hive-dashboard.sh

test:
	python3 -m unittest discover -s ./tests -v

check:
	python3 -m unittest discover -s ./tests -v
	python3 -m compileall -q ./app ./dashboard ./tests
	podman compose -f ./docker-compose.yml config >/dev/null
	podman compose --profile hive --profile dashboard --profile streaming \
		-f ./docker-compose.yml config >/dev/null
