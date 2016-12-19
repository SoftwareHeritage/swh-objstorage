# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import click
import logging

from swh.core import config
from swh.storage.archiver.storage import ArchiverStorage

from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError, Error


class BaseContentChecker(config.SWHConfig, metaclass=abc.ABCMeta):
    """Abstract class of the content integrity checker.

    This checker's purpose is to iterate over the contents of a storage and
    check the integrity of each file.
    Behavior of the checker to deal with corrupted status will be specified
    by subclasses.

    You should override the DEFAULT_CONFIG and CONFIG_BASE_FILENAME
    variables if you need it.

    """
    DEFAULT_CONFIG = {
        'storage': ('dict',
                    {'cls': 'pathslicing',
                     'args': {'root': '/srv/softwareheritage/objects',
                              'slicing': '0:2/2:4/4:6'}}),
        'batch_size': ('int', 1000),
    }

    CONFIG_BASE_FILENAME = 'objstorage/objstorage_checker'

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

    CONFIG_BASE_FILENAME = 'objstorage/log_checker'

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
                                'args': {'url': 'http://banco:5003/'}
                            }})
    }

    CONFIG_BASE_FILENAME = 'objstorage/repair_checker'

    def __init__(self):
        super().__init__()
        self.backups = [
            get_objstorage(**storage)
            for name, storage in self.config['backup_storages'].items()
        ]

    def corrupted_content(self, obj_id):
        """ Perform an action to treat with a corrupted content.
        """
        super().corrupted_content(obj_id)
        self._restore(obj_id)

    def missing_content(self, obj_id):
        """ Perform an action to treat with a missing content.
        """
        super().missing_content(obj_id)
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


class ArchiveNotifierContentChecker(LogContentChecker):
    """ Implementation of the checker that will update the archiver database

    Once the database is updated the archiver may restore the content on it's
    next scheduling as it won't be present anymore, and this status change
    will probably make the retention policy invalid.
    """
    DEFAULT_CONFIG = {
        'storage': ('dict',
                    {'cls': 'pathslicing',
                     'args': {'root': '/srv/softwareheritage/objects',
                              'slicing': '0:2/2:4/4:6'}}),
        'batch_size': ('int', 1000),
        'log_tag': ('str', 'objstorage.checker'),
        'storage_name': ('str', 'banco'),
        'dbconn': ('str', 'dbname=softwareheritage-archiver-dev')
    }

    CONFIG_BASE_FILENAME = 'objstorage/archive_notifier_checker'

    def __init__(self):
        super().__init__()
        self.archiver_db = ArchiverStorage(self.config['dbconn'])
        self.storage_name = self.config['storage_name']

    def corrupted_content(self, obj_id):
        """ Perform an action to treat with a corrupted content.
        """
        super().corrupted_content(obj_id)
        self._update_status(obj_id, 'corrupted')

    def missing_content(self, obj_id):
        """ Perform an action to treat with a missing content.
        """
        super().missing_content(obj_id)
        self._update_status(obj_id, 'missing')

    def _update_status(self, obj_id, status):
        self.archiver_db.content_archive_update(obj_id, self.storage_name,
                                                new_status=status)


@click.command()
@click.argument('checker-type', required=1, default='log')
@click.option('--daemon/--nodaemon', default=True,
              help='Indicates if the checker should run forever '
              'or on a single batch of content')
def launch(checker_type, daemon):
    types = {
        'log': LogContentChecker,
        'repair': RepairContentChecker,
        'archiver_notifier': ArchiveNotifierContentChecker
    }
    checker = types[checker_type]()
    if daemon:
        checker.run_as_daemon()
    else:
        checker.run()


if __name__ == '__main__':
    launch()
