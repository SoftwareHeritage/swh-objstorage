# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import click
import logging

from swh.core import config

from . import get_objstorage
from .exc import ObjNotFoundError, Error


class BaseContentChecker(config.SWHConfig, metaclass=abc.ABCMeta):
    """ Abstract class of the content integrity checker.

    This checker's purpose is to iterate over the contents of a storage and
    check the integrity of each file.
    Behavior of the checker to deal with corrupted status will be specified
    by subclasses.
    """

    DEFAULT_CONFIG = {
        'storage': ('dict',
                    {'cls': 'pathslicing',
                     'args': {'root': '/srv/softwareheritage/objects',
                              'slicing': '0:2/2:4/4:6'}}),
        'batch_size': ('int', 1000),
    }
    CONFIG_BASE_FILENAME = 'objstorage_checker'

    def __init__(self):
        """ Create a checker that ensure the objstorage have no corrupted file
        """
        self.config = self.parse_config_file()
        self.objstorage = get_objstorage(**self.config['storage'])
        self.batch_size = self.config['batch_size']

    def run_as_daemon(self):
        """ Start the check routine and perform it forever.

        Use this method to run the checker as a daemon that will iterate over
        the content forever in background.
        """
        while True:
            try:
                self.run()
            except:
                pass

    def run(self):
        """ Check a batch of content.
        """
        for obj_id in self._get_content_to_check(self.batch_size):
            cstatus = self._check_content(obj_id)
            if cstatus == 'corrupted':
                self.corrupted_content(obj_id)
            elif cstatus == 'missing':
                self.missing_content(obj_id)

    def _get_content_to_check(self, batch_size):
        """ Get the content that should be verified.

        Returns:
            An iterable of the content's id that need to be checked.
        """
        yield from self.objstorage.get_random(batch_size)

    def _check_content(self, obj_id):
        """ Check the validity of the given content.

        Returns:
            True if the content was valid, false if it was corrupted.
        """
        try:
            self.objstorage.check(obj_id)
        except ObjNotFoundError:
            return 'missing'
        except Error:
            return 'corrupted'

    @abc.abstractmethod
    def corrupted_content(self, obj_id):
        """ Perform an action to treat with a corrupted content.
        """
        raise NotImplementedError("%s must implement "
                                  "'corrupted_content' method" % type(self))

    @abc.abstractmethod
    def missing_content(self, obj_id):
        """ Perform an action to treat with a missing content.
        """
        raise NotImplementedError("%s must implement "
                                  "'missing_content' method" % type(self))


class LogContentChecker(BaseContentChecker):
    """ Content integrity checker that just log detected errors.
    """

    DEFAULT_CONFIG = {
        'storage': ('dict',
                    {'cls': 'pathslicing',
                     'args': {'root': '/srv/softwareheritage/objects',
                              'slicing': '0:2/2:4/4:6'}}),
        'batch_size': ('int', 1000),
        'log_tag': ('str', 'objstorage.checker')
    }

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.config['log_tag'])

    def corrupted_content(self, obj_id):
        """ Perform an action to treat with a corrupted content.
        """
        self.logger.error('Content %s is corrupted' % obj_id)

    def missing_content(self, obj_id):
        """ Perform an action to treat with a missing content.
        """
        self.logger.error('Content %s is detected missing' % obj_id)


class RepairContentChecker(LogContentChecker):
    """ Content integrity checker that will try to restore contents.
    """

    DEFAULT_CONFIG = {
        'storage': ('dict',
                    {'cls': 'pathslicing',
                     'args': {'root': '/srv/softwareheritage/objects',
                              'slicing': '0:2/2:4/4:6'}}),
        'batch_size': ('int', 1000),
        'log_tag': ('str', 'objstorage.checker'),
        'backup_storages': ('dict',
                            {'banco': {
                                'cls': 'remote',
                                'args': {'base_url': 'http://banco:5003/'}
                            }})
    }

    def __init__(self):
        super().__init__()
        self.backups = [get_objstorage(**storage)
                        for name, storage in self.config['backup_storages']]

    def corrupted_content(self, obj_id):
        """ Perform an action to treat with a corrupted content.
        """
        self._restore(obj_id)

    def missing_content(self, obj_id):
        """ Perform an action to treat with a missing content.
        """
        self._restore(obj_id)

    def _restore(self, obj_id):
        if not self._perform_restore(obj_id):
            # Object could not be restored
            self.logger.critical(
                'Object %s is corrupted and could not be repaired' % obj_id
            )

    def _perform_restore(self, obj_id):
        """ Try to restore the object in the current storage using the backups
        """
        for backup in self.backups:
            try:
                content = backup.get(obj_id)
                self.objstorage.restore(content, obj_id)
            except ObjNotFoundError as e:
                continue
            else:
                # Return True direclty when a backup contains the object
                return True
        # No backup contains the object
        return False


@click.command()
@click.argument('--checker-type', required=1, default='log')
@click.option('--daemon/--nodaemon', default=True,
              help='Indicates if the checker should run forever '
              'or on a single batch of content')
def launch(checker_type, is_daemon):
    types = {
        'log': LogContentChecker,
        'repair': RepairContentChecker
    }
    checker = types[checker_type]()
    if is_daemon:
        checker.run_as_daemon()
    else:
        checker.run()

if __name__ == '__main__':
    launch()
