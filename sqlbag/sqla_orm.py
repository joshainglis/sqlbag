import re
from collections import OrderedDict
from typing import Any, Dict, List

from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import Mapper, Session, class_mapper
from sqlalchemy.schema import MetaData
from sqlalchemy.orm.exc import UnmappedInstanceError


def metadata_from_session(s: Session) -> MetaData:
    """
    Args:
        s: an SQLAlchemy :class:`Session`
    Returns:
        The metadata associated with the session's bind.

    Get the metadata associated with the schema.
    """
    meta = MetaData()
    meta.reflect(bind=s.bind)
    return meta


class Base:
    """
    A modified ORM Base implementation that gives you a nicer __repr__
    (useful when printing/logging/debugging), along with some additional properties.
    """

    @declared_attr
    def __tablename__(cls) -> str:
        """Convert CamelCase class name to underscores_between_words
        table name."""
        name = cls.__name__
        return name[0].lower() + re.sub(
            r"([A-Z])", lambda m: "_" + m.group(0).lower(), name[1:]
        )

    def __repr__(self) -> str:
        items = row2dict(self).items()
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(["{}={!r}".format(k, v) for k, v in items]),
        )

    @property
    def _sqlachanges(self) -> Dict[str, List[Any]]:
        """
        Return the changes you've made to this object so far this session.
        """
        return sqlachanges(self)

    @property
    def _ordereddict(self) -> OrderedDict:
        """
        Return this object's properties as an OrderedDict.
        """
        return row2dict(self)

    def __str__(self) -> str:
        return repr(self)


def sqlachanges(sa_object: Any) -> Dict[str, List[Any]]:
    """
    Returns the changes made to this object so far this session,
    in {'propertyname': [listofvalues] } format.
    """
    try:
        inspected = inspect(sa_object)
        return {
            attr.key: list(reversed(attr.history.sum()))
            for attr in inspected.attrs
            if attr.history.has_changes()
        }
    except UnmappedInstanceError:
        raise TypeError("Object is not a mapped SQLAlchemy instance.")


def row2dict(sa_object: Any) -> OrderedDict:
    """
    Converts a mapped object into an OrderedDict.
    """
    return OrderedDict(
        (pname, getattr(sa_object, pname)) for pname in get_properties(sa_object)
    )


def get_properties(instance: Any) -> List[str]:
    """
    Gets the mapped properties of this mapped object.
    """
    try:
        mapper: Mapper = class_mapper(type(instance))
        return [prop.key for prop in mapper.iterate_properties]
    except UnmappedInstanceError:
        raise TypeError("Object is not a mapped SQLAlchemy instance.")
