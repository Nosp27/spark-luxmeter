if [ ! "`docker --version`" ] then
	echo "No docker installed. Please install docker to continue"
fi

sudo docker run -d --rm \
 --name graphite \
 -p 80:80 \
 -p 2003-2004:2003-2004 \
 -p 2023-2024:2023-2024 \
 -p 8125:8125/udp \
 -p 8126:8126 \
 graphiteapp/graphite-statsd

if [ 0 -eq `sudo docker ps | grep graphite | wc -l` ]
then
	echo "No graphite container seems to run"
else
	sudo docker run -d --rm \
	 --name grafana \
	 --link graphite \
	 -p 3000:3000 \
	 grafana/grafana:7.3.6-ubuntu
fi
