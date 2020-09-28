#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


"""
operations related to Requests.
"""

import datetime

import sqlalchemy
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql.expression import asc

from idds.common import exceptions
from idds.common.constants import CollectionType, CollectionStatus, CollectionLocking, CollectionRelationType
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base import models


def create_collection(request_id, workload_id, scope, name, coll_type=CollectionType.Dataset, transform_id=None,
                      relation_type=CollectionRelationType.Input, bytes=0, status=CollectionStatus.New,
                      locking=CollectionLocking.Idle, total_files=0, new_files=0, processing_files=0,
                      processed_files=0, retries=0, expired_at=None,
                      coll_metadata=None):
    """
    Create a collection.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param coll_type: The type of dataset as dataset or container.
    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param size: The size of the collection.
    :param status: The status.
    :param locking: The locking.
    :param total_files: Number of total files.
    :param retries: Number of retries.
    :param expired_at: The datetime when it expires.
    :param coll_metadata: The metadata as json.

    :returns: collection.
    """
    new_coll = models.Collection(request_id=request_id, workload_id=workload_id, scope=scope, name=name,
                                 coll_type=coll_type, transform_id=transform_id,
                                 relation_type=relation_type, bytes=bytes, status=status, locking=locking,
                                 total_files=total_files, new_files=new_files, processing_files=processing_files,
                                 processed_files=processed_files, retries=retries,
                                 expired_at=expired_at, coll_metadata=coll_metadata)
    return new_coll


@transactional_session
def add_collection(request_id, workload_id, scope, name, coll_type=CollectionType.Dataset, transform_id=None,
                   relation_type=CollectionRelationType.Input, bytes=0, status=CollectionStatus.New,
                   locking=CollectionLocking.Idle, total_files=0, new_files=0, processing_files=0,
                   processed_files=0, retries=0, expired_at=None,
                   coll_metadata=None, session=None):
    """
    Add a collection.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param coll_type: The type of dataset as dataset or container.
    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param size: The size of the collection.
    :param status: The status.
    :param locking: The locking.
    :param total_files: Number of total files.
    :param retries: Number of retries.
    :param expired_at: The datetime when it expires.
    :param coll_metadata: The metadata as json.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: collection id.
    """
    try:
        new_coll = create_collection(request_id=request_id, workload_id=workload_id, scope=scope, name=name,
                                     coll_type=coll_type, transform_id=transform_id,
                                     relation_type=relation_type, bytes=bytes, status=status, locking=locking,
                                     total_files=total_files, new_files=new_files, retries=retries,
                                     processing_files=processing_files, processed_files=processed_files,
                                     expired_at=expired_at, coll_metadata=coll_metadata)
        new_coll.save(session=session)
        coll_id = new_coll.coll_id
        return coll_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Collection scope:name(%s:%s) with transform_id(%s) already exists!: %s' %
                                          (scope, name, transform_id, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_collection_id(transform_id, relation_type, session=None):
    """
    Get collection id or raise a NoObject exception.

    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Collection id.
    """

    try:
        query = session.query(models.Collection.coll_id)
        query = query.filter_by(transform_id=transform_id)
        query = query.filter(models.Collection.relation_type == relation_type)
        ret = query.first()
        if not ret:
            return None
        else:
            return ret[0]
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('collection(transform_id: %s, relation_type: %s) cannot be found: %s' %
                                  (transform_id, relation_type, error))
    except Exception as error:
        raise error


@read_session
def get_collection(coll_id=None, transform_id=None, relation_type=None, to_json=False, session=None):
    """
    Get a collection or raise a NoObject exception.

    :param coll_id: The id of the collection.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Collection.
    """

    try:
        if coll_id:
            query = session.query(models.Collection).filter_by(coll_id=coll_id)
        else:
            query = session.query(models.Collection).filter_by(transform_id=transform_id)\
                           .filter(models.Collection.relation_type == relation_type)

        ret = query.first()
        if not ret:
            return None
        else:
            if to_json:
                return ret.to_dict_json()
            else:
                return ret.to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('collection(coll_id: %s, transform_id: %s, relation_type: %s) cannot be found: %s' %
                                  (coll_id, transform_id, relation_type, error))
    except Exception as error:
        raise error


@read_session
def get_collection_ids_by_transform_id(transform_id=None, session=None):
    """
    Get collection ids by transform id or raise a NoObject exception.

    :param transform_id: The transform id related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collection ids.
    """
    try:
        query = session.query(models.Collection.coll_id)\
                       .filter_by(transform_id=transform_id)
        ret = query.all()
        if not ret:
            return []
        else:
            items = []
            for t in ret:
                items.append(t[0])
            return items
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collections with  transform_id(%s): %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_collections_by_status(status, relation_type=CollectionRelationType.Input, time_period=None,
                              locking=False, bulk_size=None, to_json=False, session=None):
    """
    Get collections by status, relation_type and time_period or raise a NoObject exception.

    :param status: The collection status.
    :param relation_type: The relation_type of the collection to the transform.
    :param time_period: time period in seconds since last update.
    :param locking: Wheter to retrieve unlocked files.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if not isinstance(status, (list, tuple)):
            status = [status]
        if len(status) == 1:
            status = [status[0], status[0]]

        query = session.query(models.Collection)\
                       .filter(models.Collection.status.in_(status))\
                       .filter(models.Collection.next_poll_at < datetime.datetime.utcnow())

        if relation_type is not None:
            query = query.filter(models.Collection.relation_type == relation_type)
        if time_period:
            query = query.filter(models.Collection.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period))
        if locking:
            query = query.filter(models.Collection.locking == CollectionLocking.Idle)

        query = query.order_by(asc(models.Collection.updated_at))
        if bulk_size:
            query = query.limit(bulk_size)

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collections with  status(%s), relation_type(%s), time_period(%s): %s' %
                                  (status, relation_type, time_period, error))
    except Exception as error:
        raise error


@read_session
def get_collections(scope=None, name=None, request_id=None, workload_id=None, transform_id=None,
                    relation_type=None, to_json=False, session=None):
    """
    Get collections by request id or raise a NoObject exception.

    :param scope: collection scope.
    :param name: collection name, can be wildcard.
    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: list of transform id related to this collection.
    :param relation_type: The relation type between this collection and the transform: Input, Ouput and Log.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if transform_id and type(transform_id) not in (list, tuple):
            transform_id = [transform_id]

        query = session.query(models.Collection)
        if scope:
            query = query.filter(models.Collection.scope == scope)
        if name:
            query = query.filter(models.Collection.name.like(name.replace('*', '%')))
        if request_id:
            query = query.filter(models.Collection.request_id == request_id)
        if workload_id:
            query = query.filter(models.Collection.workload_id == workload_id)
        if transform_id:
            query = query.filter(models.Collection.transform_id.in_(transform_id))
        if relation_type is not None:
            query = query.filter(models.Collection.relation_type == relation_type)

        query = query.order_by(asc(models.Collection.updated_at))

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collection with  scope(%s), name(%s), transform_id(%s): %s, relation_type: %s' %
                                  (scope, name, transform_id, relation_type, error))
    except Exception as error:
        raise error


@transactional_session
def update_collection(coll_id, parameters, session=None):
    """
    update a collection.

    :param coll_id: the collection id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        parameters['updated_at'] = datetime.datetime.utcnow()
        session.query(models.Collection).filter_by(coll_id=coll_id)\
               .update(parameters, synchronize_session=False)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Collection %s cannot be found: %s' % (coll_id, error))


@transactional_session
def update_collections(parameters, session=None):
    """
    update collections.

    :param parameters: list of dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        for parameter in parameters:
            parameter['updated_at'] = datetime.datetime.utcnow()

        session.bulk_update_mappings(models.Collection, parameters)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Collection cannot be found: %s' % (error))


@transactional_session
def delete_collection(coll_id=None, session=None):
    """
    delete a collection.

    :param coll_id: The id of the collection.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        session.query(models.Collection).filter_by(coll_id=coll_id).delete()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Collection %s cannot be found: %s' % (coll_id, error))


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    params = {'locking': 0}
    session.query(models.Collection).filter(models.Collection.locking == CollectionLocking.Locking)\
           .filter(models.Collection.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period))\
           .update(params, synchronize_session=False)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the collection
    """
    if not isinstance(status, (list, tuple)):
        status = [status]
    if len(status) == 1:
        status = [status[0], status[0]]

    params = {'next_poll_at': datetime.datetime.utcnow()}
    session.query(models.Collection).filter(models.Collection.status.in_(status))\
           .update(params, synchronize_session=False)
