docker run \
	--detach \
	--env FTP_PASS=dl12345 \
	--env FTP_USER=password\
	--name docker-ftp \
	--publish 20-21:20-21/tcp \
	--publish 40000-40009:40000-40009/tcp \
	--volume /home/user/ubuntu/data/:/home/user \
	garethflowers/ftp-server
