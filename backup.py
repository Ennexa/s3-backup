#!/usr/bin/env python

import datetime
from datetime import timedelta
import yaml
import logging
import logging.config
import subprocess
import os
import sys
import re
import glob
import argparse

class AbstractDriver(object):
    def __init__(self, config = None, logger = None):
        self.config = {'bin_path': '', 'env': None, 'aws_access_key': None, 'aws_secret_key': None}
        if config:
            self.config.update(config)
        self.logger = logger or logging.getLogger(__name__)
    
    def _run(self, cmd):
        env = os.environ.copy()
        if self.config['env']:
            env.update(self.config['env'])
        
        process = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        for line in process.stdout:
            self.logger.debug(line.rstrip())
        
        for line in process.stderr:
            self.logger.info(line.rstrip())
        
        process.wait()

class RcloneDriver(AbstractDriver):
    def __init__(self, config = None, logger = None):
        super(RcloneDriver, self).__init__(config, logger)
        
        if not self.config['bin_path']:
            self.config['bin_path'] = 'rclone'
        
        if not self.config['aws_access_key'] or not self.config['aws_secret_key']:
            if 'aws_env_auth' in self.config and not self.config['aws_env_auth']:
                raise Exception('Either configure aws_env_auth or configure access/secret key')
            else:
                self.config['aws_env_auth'] = True
        else:
            self.config['aws_env_auth'] = False
        
        self.flags = self.build_flags()
    
    def build_flags(self):
        flags = ['-v', '--s3-provider', 'AWS']
        
        if 'aws_env_auth' in self.config and self.config['aws_env_auth']:
            flags.extend(['--s3-env-auth'])
        else:
            flags.extend(['--s3-access-key-id', self.config['aws_access_key']])
            flags.extend(['--s3-secret-access-key', self.config['aws_secret_key']])
        
        if 'aws_region' in self.config:
            flags.extend(['--s3-region', self.config['aws_region']])
        
        if 'aws_s3_acl' in self.config:
            flags.extend(['--s3-acl', self.config['aws_s3_acl']])
        
        if 'aws_s3_server_side_encryption' in self.config:
            flags.extend(['--s3-server-side-encryption', self.config['aws_s3_server_side_encryption']])
        
        if 'aws_s3_storage_class' in self.config:
            flags.extend(['--s3-storage-class', self.config['aws_s3_storage_class']])
        
        return flags
    
    def fix_s3_path(self, path):
        return path.replace('s3://', ':s3:')
    
    def sync(self, src, dest):
        self.logger.info("rclone::syncing '%s' to amazon s3", src.get_name())
        
        flags = self.flags[:]
        
        for path in src.get_excluded_paths():
            flags.extend(['--exclude', self.fix_s3_path(path)])
        
        dest = dest.replace('s3://', ':s3:')
        
        included_paths = src.get_included_paths(self)
        if included_paths:
            for path in src.get_included_paths(self):
                cmd = [self.config['bin_path'], 'sync', self.fix_s3_path(path), dest]
                cmd.extend(flags)
                self._run(cmd)
        else:
            self.logger.info('Skipping copy as source list is empty')
    
    def remove(self, dest):
        self.logger.info("rclone::removing '%s' from amazon s3", dest)
        dest = dest.replace('s3://', ':s3:')
        cmd = [self.config['bin_path'], 'purge', self.fix_s3_path(dest)]
        cmd.extend(self.flags)
        
        self._run(cmd)
    
    def exists(self, dest):
        return True
    
    

class RsyncDriver(AbstractDriver):
    def __init__(self, config = None, logger = None):
        super(RsyncDriver, self).__init__(config, logger)
        
        if not self.config['bin_path']:
            self.config['bin_path'] = 'rsync'
    
    def sync(self, src, dest):
        self.logger.info("rsync::syncing '%s' to filesystem", src.get_name())
        cmd = [self.config['bin_path'], '-avP']
        
        for path in src.get_excluded_paths():
            cmd.extend(['--exclude', path])
        
        included_paths = src.get_included_paths(self)
        
        if not os.path.exists(dest):
            os.makedirs(dest)
        
        if not os.path.isdir(dest):
            raise Exception('Destination is not a directory %s' % dest);
        
        if included_paths:
            for path in included_paths:
                cmd.append(path)
            
            cmd.append(dest)
            self._run(cmd)
        else:
            self.logger.info('Skipping copy as source list is empty')
    
    def remove(self, dest):
        self.logger.info("rclone::removing '%s' from filesytem", dest)
        if dest == '/':
            self.logger.error("rsync::cannot remove '/' directory")
        else:
            self._run(['rm', '-rf', dest])
    
    def exists(self, dest):
        return os.path.isdir(dest)


class AmazonS3Store():
    def __init__(self, config, driver = None):
        # self.config = config
        self.logger = logging.getLogger(__name__)
        
        if 'bucket' not in config:
            raise Exception("'bucket' is required for amazons3 store")
        
        self.prefix = 's3://' + config['bucket']
        
        if 'prefix' in config:
            self.prefix = 's3://' + self.config['bucket'] + '/' + config['prefix']
        
        if driver == None:
            driver = RcloneDriver(config)
        
        self.driver = driver
    
    def __get_destination(self, backup, version):
        return os.path.join(self.prefix, version.strftime('%Y-%m-%d'), backup.get_name())
    
    def add(self, version, backup):
        self.driver.sync(backup, self.__get_destination(backup, version))
    
    def remove(self, backup, version):
        dest = self.__get_destination(backup, version)
        if self.driver.exists(dest):
            self.driver.remove(dest)

class FileSystemStore():
    def __init__(self, config, driver = None):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        if 'path' not in config:
            raise Exception("'path' is required for filesystem store")
        
        if os.path.isdir(config['path']) == False:
            raise Exception("%s is not a directory" % config['path'])
        
        if not os.access(config['path'], os.W_OK | os.X_OK):
            raise Exception("%s is not writeable" % config['path'])
        
        if driver == None:
            driver = RsyncDriver()
        
        self.driver = driver
    
    def __get_destination(self, backup, version):
        return os.path.join(self.config['path'], version.strftime('%Y-%m-%d'), backup.get_name())
    
    def add(self, version, backup):
        self.driver.sync(backup, self.__get_destination(backup, version))
    
    def remove(self, backup, version):
        dest = self.__get_destination(backup, version)
        if self.driver.exists(dest):
            self.driver.remove(dest)

class Backup():
    def __init__(self, config):
        self.config = {'onerror': 'exception'}
        self.config.update(config)
        self.logger = logging.getLogger(__name__)
        
        if 'include' not in config:
            raise "'include' is missing for source %s" % self.get_name()
    
    def get_name(self):
        return self.config['name']
    
    @staticmethod
    def replace_placeholder(match):
        placeholder = match.group(1) or match.group(2)
        
        if placeholder in ['Y', 'm', 'd']:
            return datetime.datetime.now().strftime('%' + placeholder)
        elif placeholder == 'LATEST':
            return 'LATEST'
        else:
            raise Exception("Invalid placeholder '%s'" % placeholder)
    
    def handle_error(self, exception):
        self.logger.error(str(exception))
        print(self.config)
        if self.config['onerror'] == 'alert':
            # Send Alert
            print('Send Alert')
        elif self.config['onerror'] != 'continue':
            raise exception
    
    def get_included_paths(self, driver):
        include = []
        now = datetime.datetime.now()
        for path in self.config['include']:
            path = re.sub('%([A-Za-z])|%\{([A-Za-z])\}+', Backup.replace_placeholder, path)
            paths = glob.glob(path)
            if not paths:
                self.handle_error(Exception('Path not found %s' % path))
            else:
                for path in paths:
                    self.logger.debug('Including path %s' % path)
                include.extend(paths)
        
        return include
    
    def get_excluded_paths(self):
        if 'exclude' in self.config:
            return self.config['exclude']
        else:
            return []


class Manager():
    def __init__(self, file, logger = None):
        self.backups = []
        self.destinations = []
        
        self.config = {
            'log_file': './backup.log'
        }
        self.config.update(self.load_config(file))
        self.init_logger()
        self.logger = logger or logging.getLogger(__name__)
        
        self.obsolete = self.load_obsolete()
        
        for destination in self.config['destination']:
            if destination['store'] == 'amazons3':
                destination = AmazonS3Store(destination['options'])
            elif destination['store'] == 'filesystem':
                destination = FileSystemStore(destination['options'])
            else:
                raise Exception('Unsupported storage store - %s' % (destination['name']));
            
            self.destinations.append(destination)
        
        for source in self.config['source']:
            backup = Backup(source)
            
            self.backups.append(backup)
    
    def init_logger(self, default_level=logging.DEBUG):
        value = os.getenv('LOG_CFG', None)
        
        if value and os.path.exists(value):
            path = value
            with open(path, 'rt') as f:
                config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)
        elif 'logging' in self.config:
            logging.config.dictConfig(self.config['logging'])
        else:
            logging.basicConfig(level=default_level)
    
    def load_config(self, file):
        stream = open(file, "r")
        config = yaml.safe_load(stream)

        if config.get('extends'):
            base = self.load_config(config['extends'])
            del config['extends']
            base.update(config)
            config = base

        return config
    
    def load_obsolete(self):
        now = datetime.datetime.now()
        dow = now.weekday()
        obsolete = []
        
        if dow == 1:
            date = now - timedelta(days=28)
            if date.day > 7:
                obsolete.append(date)
            
            date = now - timedelta(days=364)
            if date.day <= 8 and date.month > 1:
                obsolete.append(date)
        else:
            date = now - timedelta(days=7)
            obsolete.append(date)
        
        return obsolete;
    
    def run(self):
        version = datetime.datetime.now()
        for backup in self.backups:
            self.logger.info('Preparing to backup %s' % backup.get_name())
            for destination in self.destinations:
                try:
                    destination.add(version, backup)
                    for date in self.obsolete:
                        destination.remove(backup, date)
                except Exception as e:
                    backup.handle_error(e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-c', '--config-file', help='config file', default='./config.yaml')
    
    args = parser.parse_args()

    try:
        manager = Manager(args.config_file)
        manager.run()     
    except Exception as e:
       sys.exit( str(e))


# if [ $DOW -eq 1 ]; then
#     DATE_DAY=$(date -d "-28 days" +"%d")
#     if [ $DATE_DAY -gt 7 ]; then
#         DATE=$(date -d "-28 days" +"%Y-%m-%d")
#         delete $DATE
#     fi
#
#     DATE_DAY=$(date -d "-364 days" +"%d")
#     DATE_MONTH=$(date -d "-364 days" +"%m")
#     if [ $DATE_DAY -le 7 ] && [ $DATE_MONTH -gt 1 ]; then
#         DATE=$(date -d "-364 days" +"%Y-%m-%d")
#         delete $DATE
#     fi
# else
#     DATE=$(date -d "-7 days" +"%Y-%m-%d")
#     echo $DATE
#     delete $DATE
# fi

