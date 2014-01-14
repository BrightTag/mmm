#!/usr/bin/env python

import gevent.monkey
gevent.monkey.patch_all()

import argparse
from logging import config
import logging
import sys
import yaml

from mmm.replication import ReplicationEngine

def logging_config(level, filename):
  return {
  "version": 1,
  "disable_existing_loggers": False,
  "formatters": {
    "standard": {
      "format": "%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
      "datefmt": '%Y-%m-%d %H:%M:%S'
    }
  },
  "handlers": {
    "default": {
      "level": level,
      "class": "logging.handlers.RotatingFileHandler",
      "formatter": "standard",
      "filename": filename,
      "maxBytes": 50 * 1024 * 1024, # 50 MB
      "backupCount": 7
    }
  },
  "loggers": {
    "": {
      "handlers": ["default"],
      "level": level,
      "propogate": True
    }
  }
}

"""
Config file should look like this:

master:
  name: 'my master'
  uri: 'localhost:27017'
  id: 'my-server-mongo'
replications:
  - name: 'another server'
    id: 'my-other-server-mongo'
    uri: 'localhost:27019'
    operations: 'iud'
    namespaces:
      - source: 'mydb.mycol'
        dest: 'otherdb.othercol'
      - source: 'mydb.anothercol'
        dest: 'otherdb.anothercol'
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--level", choices=[x for x in logging._levelNames.keys() if isinstance(x, str)],
        default="INFO", help="logging level string (e.g. DEBUG), defaults to INFO")
    parser.add_argument("-f", "--filename", default="./mmm.log", help="filename to log to, defaults to ./mmm.log")
    parser.add_argument('-c', '--config', default='test.yml', help='Topology config file', required=True)

    args = parser.parse_args()
    config.dictConfig(logging_config(args.level, args.filename))

    log = logging.getLogger('mmm')

    config = yaml.load(open(args.config))
    master = config["master"]
    engine = ReplicationEngine(master["id"], master["uri"], config["replications"])

    engine.start()

    while True:
        try:
            gevent.sleep(5)
            log.debug("Main thread sleeping")
        except KeyboardInterrupt:
            log.info("Exiting due to KeyboardInterrupt")
            sys.exit(0)
