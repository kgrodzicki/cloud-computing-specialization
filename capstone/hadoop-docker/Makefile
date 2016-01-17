build:
	docker build -t kgrodzicki/hadoop .

tag:
	docker tag -f kgrodzicki/hadoop:latest 291089697348.dkr.ecr.us-east-1.amazonaws.com/kgrodzicki/hadoop:latest

push:
	docker push 291089697348.dkr.ecr.us-east-1.amazonaws.com/kgrodzicki/hadoop:latest

login:
	docker exec -it -u hadoop hadoop bash -c 'cd /home/hadoop; exec /bin/bash'

run:
	docker run -d -p PORT_MAPPING="80:80" --name kgrodzicki/hadoop 291089697348.dkr.ecr.us-east-1.amazonaws.com/kgrodzicki/hadoop:latest
