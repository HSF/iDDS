#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
operations related to Requests.
"""

import datetime
import json

import sqlalchemy
from sqlalchemy import BigInteger, Integer
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql import text, bindparam, outparam

from idds.common import exceptions
from idds.common.constants import CollectionType, CollectionStatus, CollectionSubStatus, CollectionRelationType
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base.utils import row2dict


@transactional_session
def add_collection(scope, name, coll_type=CollectionType.Dataset, transform_id=None,
                   relation_type=CollectionRelationType.Input, coll_size=0, status=CollectionStatus.New,
                   substatus=CollectionSubStatus.Idle, total_files=0, retries=0, expired_at=None, coll_metadata=None, session=None):
    """
    Add a collection.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param type: The type of dataset as dataset or container.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param size: The size of the collection.
    :param status: The status.
    :param substatus: The substatus.
    :param total_files: Number of total files.
    :param retries: Number of retries.
    :param expired_at: The datetime when it expires.
    :param coll_metadata: The metadata as json.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: collection id.
    """
    if isinstance(coll_type, CollectionType):
        coll_type = coll_type.value
    if isinstance(status, CollectionStatus):
        status = status.value
    if isinstance(substatus, CollectionSubStatus):
        substatus = substatus.value
    if isinstance(relation_type, CollectionRelationType):
        relation_type = relation_type.value
    if coll_metadata:
        coll_metadata = json.dumps(coll_metadata)

    insert_coll_sql = """insert into atlas_idds.collections(scope, name, coll_type, transform_id,
                                                            relation_type, coll_size, status, substatus, total_files,
                                                            retries, created_at, updated_at, expired_at,
                                                            coll_metadata)
                         values(:scope, :name, :coll_type, :transform_id, :relation_type, :coll_size,
                                :status, :substatus, :total_files, :retries, :created_at, :updated_at, :expired_at,
                                :coll_metadata) returning coll_id into :coll_id
                      """
    stmt = text(insert_coll_sql)
    stmt = stmt.bindparams(outparam("coll_id", type_=BigInteger().with_variant(Integer, "sqlite")))
    try:
        coll_id = None
        ret = session.execute(stmt, {'scope': scope, 'name': name, 'coll_type': coll_type,
                                     'transform_id': transform_id, 'relation_type': relation_type, 'coll_size': coll_size,
                                     'status': status, 'substatus': substatus, 'total_files': total_files, 'retries': retries,
                                     'created_at': datetime.datetime.utcnow(), 'updated_at': datetime.datetime.utcnow(),
                                     'expired_at': expired_at, 'coll_metadata': coll_metadata, 'coll_id': coll_id})
        coll_id = ret.out_parameters['coll_id'][0]

        return coll_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Collection scope:name(%s:%s) with transform_id(%s) already exists!: %s' %
                                          (scope, name, transform_id, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_collection_id(transform_id=None, relation_type=None, session=None):
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
        if relation_type is not None and isinstance(relation_type, CollectionRelationType):
            relation_type = relation_type.value
        select = "select coll_id from atlas_idds.collections where transform_id=:transform_id and relation_type=:relation_type"
        stmt = text(select)
        result = session.execute(stmt, {'transform_id': transform_id, 'relation_type': relation_type})
        collection_id = result.fetchone()

        if collection_id is None:
            raise exceptions.NoObject('collection(transform_id: %s, relation_type: %s) cannot be found' %
                                      (transform_id, relation_type))
        collection_id = collection_id[0]

        return collection_id
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('collection(transform_id: %s, relation_type: %s) cannot be found: %s' %
                                  (transform_id, relation_type, error))
    except Exception as error:
        raise error


@read_session
def get_collection(coll_id=None, transform_id=None, relation_type=None, session=None):
    """
    Get a collection or raise a NoObject exception.

    :param coll_id: The id of the collection.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Collection.
    """

    try:
        if coll_id:
            coll_select = """select * from atlas_idds.collections where coll_id=:coll_id
                          """
            stmt = text(coll_select)
            result = session.execute(stmt, {'coll_id': coll_id})
        else:
            coll_select = """select * from atlas_idds.collections
                             where transform_id=:transform_id and relation_type=:relation_type
                          """
            stmt = text(coll_select)
            result = session.execute(stmt, {'transform_id': transform_id, 'relation_type': relation_type})
        collection = result.fetchone()

        if collection is None:
            raise exceptions.NoObject('collection(coll_id: %s, transform_id: %s, relation_type: %s) cannot be found' %
                                      (coll_id, transform_id, relation_type))

        collection = row2dict(collection)
        if collection['coll_type'] is not None:
            collection['coll_type'] = CollectionType(collection['coll_type'])
        if collection['relation_type'] is not None:
            collection['relation_type'] = CollectionRelationType(collection['relation_type'])
        if collection['status'] is not None:
            collection['status'] = CollectionStatus(collection['status'])
        if collection['substatus'] is not None:
            collection['substatus'] = CollectionSubStatus(collection['substatus'])
        if collection['coll_metadata']:
            collection['coll_metadata'] = json.loads(collection['coll_metadata'])

        return collection
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('collection(coll_id: %s, transform_id: %s, relation_type: %s) cannot be found: %s' %
                                  (coll_id, transform_id, relation_type, error))
    except Exception as error:
        raise error


@read_session
def get_collections_by_transform_id(transform_id=None, session=None):
    """
    Get collections by transform id or raise a NoObject exception.

    :param transform_id: The transform id related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if transform_id:
            select = """select * from atlas_idds.collections where transform_id=:transform_id"""
            stmt = text(select)
            result = session.execute(stmt, {'transform_id': transform_id})
        else:
            raise exceptions.WrongParamterException("Transform_id is None.")

        collections = result.fetchall()
        ret = []
        for collection in collections:
            collection = row2dict(collection)
            if collection['coll_type'] is not None:
                collection['coll_type'] = CollectionType(collection['coll_type'])
            if collection['relation_type'] is not None:
                collection['relation_type'] = CollectionRelationType(collection['relation_type'])
            if collection['status'] is not None:
                collection['status'] = CollectionStatus(collection['status'])
            if collection['substatus'] is not None:
                collection['substatus'] = CollectionSubStatus(collection['substatus'])
            if collection['coll_metadata']:
                collection['coll_metadata'] = json.loads(collection['coll_metadata'])
            ret.append(collection)
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collections with  transform_id(%s): %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_collections_by_transform_ids(transform_ids=None, session=None):
    """
    Get collections by transform id or raise a NoObject exception.

    :param transform_id: list of transform ids related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if transform_ids:
            select = """select * from atlas_idds.collections where transform_id in :transform_ids"""
            stmt = text(select)
            stmt = stmt.bindparams(bindparam('transform_ids', expanding=True))
            result = session.execute(stmt, {'transform_ids': transform_ids})
        else:
            raise exceptions.WrongParamterException("Transform_ids are None.")

        collections = result.fetchall()
        ret = []
        for collection in collections:
            collection = row2dict(collection)
            if collection['coll_type'] is not None:
                collection['coll_type'] = CollectionType(collection['coll_type'])
            if collection['relation_type'] is not None:
                collection['relation_type'] = CollectionRelationType(collection['relation_type'])
            if collection['status'] is not None:
                collection['status'] = CollectionStatus(collection['status'])
            if collection['substatus'] is not None:
                collection['substatus'] = CollectionSubStatus(collection['substatus'])
            if collection['coll_metadata']:
                collection['coll_metadata'] = json.loads(collection['coll_metadata'])
            ret.append(collection)
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collections with  transform_ids(%s): %s' %
                                  (transform_ids, error))
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
        if transform_id:
            select = """select coll_id from atlas_idds.collections where transform_id=:transform_id"""
            stmt = text(select)
            result = session.execute(stmt, {'transform_id': transform_id})
        else:
            raise exceptions.WrongParamterException("Transform_id is None.")

        collection_ids = result.fetchall()
        ret = [row[0] for row in collection_ids]
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collections with  transform_id(%s): %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_collections_by_status(status, relation_type=CollectionRelationType.Input, time_period=None, locking=False, session=None):
    """
    Get collections by status, relation_type and time_period or raise a NoObject exception.

    :param status: The collection status.
    :param relation_type: The relation_type of the collection to the transform.
    :param time_period: time period in seconds since last update.
    :param locking: Wheter to retrieve unlocked files.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if status is None:
            raise exceptions.WrongParameterException("status should not be None")
        if not isinstance(status, (list, tuple)):
            status = [status]
        new_status = []
        for st in status:
            if isinstance(st, CollectionStatus):
                st = st.value
            new_status.append(st)
        status = new_status

        select = """select * from atlas_idds.collections where status in :status"""
        params = {'status': status}

        if relation_type is not None:
            if isinstance(relation_type, CollectionRelationType):
                relation_type = relation_type.value
            select = select + " and relation_type=:relation_type"
            params['relation_type'] = relation_type
        if time_period is not None:
            select = select + " and updated_at < :updated_at"
            params['updated_at'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period)
        if locking:
            select = select + " and substatus=:substatus"
            params['substatus'] = CollectionSubStatus.Idle.value

        stmt = text(select)
        stmt = stmt.bindparams(bindparam('status', expanding=True))
        result = session.execute(stmt, params)

        collections = result.fetchall()
        ret = []
        for collection in collections:
            collection = row2dict(collection)
            if collection['coll_type'] is not None:
                collection['coll_type'] = CollectionType(collection['coll_type'])
            if collection['relation_type'] is not None:
                collection['relation_type'] = CollectionRelationType(collection['relation_type'])
            if collection['status'] is not None:
                collection['status'] = CollectionStatus(collection['status'])
            if collection['substatus'] is not None:
                collection['substatus'] = CollectionSubStatus(collection['substatus'])
            if collection['coll_metadata']:
                collection['coll_metadata'] = json.loads(collection['coll_metadata'])
            ret.append(collection)
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collections with  status(%s), relation_type(%s), time_period(%s): %s' %
                                  (status, relation_type, time_period, error))
    except Exception as error:
        raise error


@read_session
def get_collections(scope, name, transform_ids=None, session=None):
    """
    Get collections by request id or raise a NoObject exception.

    :param scope: collection scope.
    :param name: collection name, can be wildcard.
    :param transform_id: list of transform id related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if transform_ids:
            select = """select * from atlas_idds.collections where scope=:scope
                        and name like :name and transform_id in :transform_ids"""
            stmt = text(select)
            stmt = stmt.bindparams(bindparam('transform_ids', expanding=True))
            result = session.execute(stmt, {'scope': scope, 'name': '%' + name + '%',
                                            'transform_ids': transform_ids})
        else:
            select = """select * from atlas_idds.collections where scope=:scope
                        and name like :name"""
            stmt = text(select)
            result = session.execute(stmt, {'scope': scope, 'name': '%' + name + '%'})

        collections = result.fetchall()
        ret = []
        for collection in collections:
            collection = row2dict(collection)
            if collection['coll_type'] is not None:
                collection['coll_type'] = CollectionType(collection['coll_type'])
            if collection['relation_type'] is not None:
                collection['relation_type'] = CollectionRelationType(collection['relation_type'])
            if collection['status'] is not None:
                collection['status'] = CollectionStatus(collection['status'])
            if collection['substatus'] is not None:
                collection['substatus'] = CollectionSubStatus(collection['substatus'])
            if collection['coll_metadata']:
                collection['coll_metadata'] = json.loads(collection['coll_metadata'])
            ret.append(collection)
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collection with  scope(%s), name(%s), transform_ids(%s): %s' %
                                  (scope, name, transform_ids, error))
    except Exception as error:
        raise error


@read_session
def get_collection_id_by_scope_name(scope, name, transform_id=None, relation_type=None, session=None):
    """
    Get collection id by scope, name, transform id or raise a NoObject exception.

    :param scope: collection scope.
    :param name: collection name, should not be wildcards.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation type between this collection and the transform: Input, Ouput and Log.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if transform_id is not None:
            if relation_type is None:
                select = """select * from atlas_idds.collections where scope=:scope
                            and name=:name and transform_id=:transform_id"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': name,
                                                'transform_id': transform_id})
            else:
                if isinstance(relation_type, CollectionRelationType):
                    relation_type = relation_type.value
                select = """select * from atlas_idds.collections where scope=:scope
                            and name=:name and transform_id=:transform_id and relation_type=:relation_type"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': name,
                                                'transform_id': transform_id, 'relation_type': relation_type})
        else:
            raise exceptions.WrongParameterException("transform_id should not be None")

        collection_id = result.fetchone()
        if collection_id is None:
            raise sqlalchemy.orm.exc.NoResultFound()
        return collection_id[0]
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collection with  scope(%s), name(%s), transform_id(%s): %s' %
                                  (scope, name, transform_id, error))
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
        if 'coll_type' in parameters and isinstance(parameters['coll_type'], CollectionType):
            parameters['coll_type'] = parameters['coll_type'].value
        if 'status' in parameters and isinstance(parameters['status'], CollectionStatus):
            parameters['status'] = parameters['status'].value
        if 'substatus' in parameters and isinstance(parameters['substatus'], CollectionSubStatus):
            parameters['substatus'] = parameters['substatus'].value
        if 'relation_type' in parameters and isinstance(parameters['relation_type'], CollectionRelationType):
            parameters['relation_type'] = parameters['relation_type'].value
        if 'coll_metadata' in parameters:
            parameters['coll_metadata'] = json.dumps(parameters['coll_metadata'])

        parameters['updated_at'] = datetime.datetime.utcnow()

        coll_update = "update atlas_idds.collections set "
        for key in parameters.keys():
            coll_update += key + "=:" + key + ","
        coll_update = coll_update[:-1]
        coll_update += " where coll_id=:coll_id"

        stmt = text(coll_update)
        parameters['coll_id'] = coll_id
        session.execute(stmt, parameters)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Collection %s cannot be found: %s' % (coll_id, error))


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
        delete = "delete from atlas_idds.collections where coll_id=:coll_id"
        stmt = text(delete)
        session.execute(stmt, {'coll_id': coll_id})
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Collection %s cannot be found: %s' % (coll_id, error))
