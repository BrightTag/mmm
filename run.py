#!/usr/bin/env python

import gevent.monkey
gevent.monkey.patch_all()

import argparse
import logging.config
import sys
import yaml

from mmm.replication import ReplicationEngine

default_logging = {
    "format": '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    "datefmt": '%Y-%m-%d %H:%M:%S',
    "level": logging.DEBUG
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
    parser.add_argument(
        '-l', '--logging', dest='logging', default=None,
        help='Logging config file')
    parser.add_argument(
        '-c', '--config', dest='config', default='test.yml',
        help='Topology config file',
        required=True)

    args = parser.parse_args()
    if args.logging:
        logging.config.fileConfig(args.logging)
    else:
        logging.basicConfig(**default_logging)

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
