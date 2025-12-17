import logging
import sys
import tempfile
import time

import docker
import pytest
import pathlib

HERE = pathlib.Path(pathlib.Path(__file__).resolve()).parent
sys.path.insert(0, HERE)
sys.path.append('..')
from tests import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def ftp_docker(request):
    client = docker.from_env()
    container = client.containers.run(
        image='garethflowers/ftp-server',
        auto_remove=True,
        environment={
            'FTP_USER': config.bbg.data.ftp.username,
            'FTP_PASS': config.bbg.data.ftp.password,
            },
        name='ftp_server',
        ports={
            f'{port}/tcp': (config.bbg.data.ftp.hostname, f'{port}/tcp')
            for port in [20, 21]+list(range(40000,40010))
            },
        volumes={tempfile.gettempdir(): {'bind': '/data', 'mode': 'rw'}},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    request.addfinalizer(container.stop)
