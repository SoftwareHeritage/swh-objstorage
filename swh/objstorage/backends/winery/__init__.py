from .objstorage import WineryObjStorage  # noqa: F401


def get_datastore(cls, db):
    assert cls == "postgresql"
    from .sharedbase import SharedBase

    return SharedBase(base_dsn=db)
