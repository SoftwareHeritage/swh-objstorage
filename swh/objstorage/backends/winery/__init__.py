from .objstorage import WineryObjStorage

__all__ = ["WineryObjStorage", "get_datastore"]


def get_datastore(**cfg):
    assert "db" in cfg
    from .sharedbase import SharedBase

    return SharedBase(base_dsn=cfg["db"])
