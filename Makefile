.PHONY: check light-up hive hive-refresh hive-check hive-demo dashboard streaming stop-optional test

light-up:
	bash infra/hdfs/start-lightweight.sh

hive:
	bash infra/hdfs/start-hive.sh

hive-refresh:
	cd infra/hdfs && make hive-refresh

hive-check:
	cd infra/hdfs && make hive-check

hive-demo:
	cd infra/hdfs && make hive-demo

dashboard:
	bash infra/hdfs/start-dashboard.sh

streaming:
	podman compose --profile streaming -f infra/hdfs/docker-compose.yml up -d \
		kafka-init streaming-enricher

stop-optional:
	bash infra/hdfs/stop-hive-dashboard.sh

test:
	python3 -m unittest discover -s infra/hdfs/tests -v

check:
	python3 -m unittest discover -s infra/hdfs/tests -v
	python3 -m compileall -q infra/hdfs/app infra/hdfs/dashboard infra/hdfs/tests
	podman compose -f infra/hdfs/docker-compose.yml config >/dev/null
	podman compose --profile hive --profile dashboard --profile streaming \
		-f infra/hdfs/docker-compose.yml config >/dev/null
