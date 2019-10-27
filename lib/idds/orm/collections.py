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
from sqlalchemy.sql import text, outparam

from idds.common import exceptions
from idds.common.constants import CollectionType, CollectionStatus, CollectionRelationType
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base.utils import row2dict


@transactional_session
def add_collection(scope, name, coll_type=CollectionType.Dataset, request_id=None, transform_id=None,
                   relation_type=CollectionRelationType.Input, coll_size=0, coll_status=CollectionStatus.New,
                   total_files=0, retries=0, expired_at=None, coll_metadata=None, session=None):
    """
    Add a collection.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param type: The type of dataset as dataset or container.
    :param request_id: The request id related to this collection.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param size: The size of the collection.
    :param status: The status.
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
    if isinstance(coll_status, CollectionStatus):
        coll_status = coll_status.value
    if isinstance(relation_type, CollectionRelationType):
        relation_type = relation_type.value
    if coll_metadata:
        coll_metadata = json.dumps(coll_metadata)

    insert_coll_sql = """insert into atlas_idds.collections(scope, name, coll_type, request_id, transform_id,
                                                            in_out_type, coll_size, coll_status, total_files,
                                                            retries, created_at, updated_at, expired_at,
                                                            coll_metadata)
                         values(:scope, :name, :coll_type, :request_id, :transform_id, :in_out_type, :coll_size,
                                :coll_status, :total_files, :retries, :created_at, :updated_at, :expired_at,
                                :coll_metadata) returning coll_id into :coll_id
                      """
    stmt = text(insert_coll_sql)
    stmt = stmt.bindparams(outparam("coll_id", type_=BigInteger().with_variant(Integer, "sqlite")))
    try:
        coll_id = None
        ret = session.execute(stmt, {'scope': scope, 'name': name, 'coll_type': coll_type, 'request_id': request_id,
                                     'transform_id': transform_id, 'in_out_type': relation_type, 'coll_size': coll_size,
                                     'coll_status': coll_status, 'total_files': total_files, 'retries': retries,
                                     'created_at': datetime.datetime.utcnow(), 'updated_at': datetime.datetime.utcnow(),
                                     'expired_at': expired_at, 'coll_metadata': coll_metadata, 'coll_id': coll_id})
        coll_id = ret.out_parameters['coll_id'][0]

        return coll_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Collection scope:name(%s:%s) with request_id:transform_id(%s:%s) already exists!: %s' %
                                          (scope, name, request_id, transform_id, error))
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
        select = "select coll_id from atlas_idds.collections where transform_id=:transform_id and in_out_type=:in_out_type"
        stmt = text(select)
        result = session.execute(stmt, {'transform_id': transform_id, 'in_out_type': relation_type})
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
                             where transform_id=:transform_id and in_out_type=:in_out_type
                          """
            stmt = text(coll_select)
            result = session.execute(stmt, {'transform_id': transform_id, 'in_out_type': relation_type})
        collection = result.fetchone()

        if collection is None:
            raise exceptions.NoObject('collection(coll_id: %s, transform_id: %s, relation_type: %s) cannot be found' %
                                      (coll_id, transform_id, relation_type))

        collection = row2dict(collection)
        if collection['coll_type'] is not None:
            collection['coll_type'] = CollectionType(collection['coll_type'])
        if collection['in_out_type'] is not None:
            collection['in_out_type'] = CollectionRelationType(collection['in_out_type'])
            collection['relation_type'] = collection['in_out_type']
        if collection['coll_status'] is not None:
            collection['coll_status'] = CollectionStatus(collection['coll_status'])
        if collection['coll_metadata']:
            collection['coll_metadata'] = json.loads(collection['coll_metadata'])

        return collection
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('collection(coll_id: %s, transform_id: %s, relation_type: %s) cannot be found: %s' %
                                  (coll_id, transform_id, relation_type, error))
    except Exception as error:
        raise error


@read_session
def get_collections_by_request_transform_id(request_id=None, transform_id=None, session=None):
    """
    Get collections by request_id and transform id or raise a NoObject exception.

    :param request_id: The request id related to this collection.
    :param transform_id: The transform id related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if request_id:
            if transform_id:
                select = """select * from atlas_idds.collections where request_id=:request_id
                            and transform_id=:transform_id"""
                stmt = text(select)
                result = session.execute(stmt, {'request_id': request_id, 'transform_id': transform_id})
            else:
                select = """select * from atlas_idds.collections where request_id=:request_id"""
                stmt = text(select)
                result = session.execute(stmt, {'request_id': request_id})
        else:
            if transform_id:
                select = """select * from atlas_idds.collections where transform_id=:transform_id"""
                stmt = text(select)
                result = session.execute(stmt, {'transform_id': transform_id})
            else:
                raise exceptions.WrongParamterException("Both request_id and workload_id are None.")

        collections = result.fetchall()
        ret = []
        for collection in collections:
            collection = row2dict(collection)
            if collection['coll_type'] is not None:
                collection['coll_type'] = CollectionType(collection['coll_type'])
            if collection['in_out_type'] is not None:
                collection['in_out_type'] = CollectionRelationType(collection['in_out_type'])
                collection['relation_type'] = collection['in_out_type']
            if collection['coll_status'] is not None:
                collection['coll_status'] = CollectionStatus(collection['coll_status'])
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
def get_collection_ids_by_request_transform_id(request_id=None, transform_id=None, session=None):
    """
    Get collection ids by request_id and transform id or raise a NoObject exception.

    :param request_id: The request id related to this collection.
    :param transform_id: The transform id related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if request_id:
            if transform_id:
                select = """select coll_id from atlas_idds.collections where request_id=:request_id
                            and transform_id=:transform_id"""
                stmt = text(select)
                result = session.execute(stmt, {'request_id': request_id, 'transform_id': transform_id})
            else:
                select = """select coll_id from atlas_idds.collections where request_id=:request_id"""
                stmt = text(select)
                result = session.execute(stmt, {'request_id': request_id})
        else:
            if transform_id:
                select = """select coll_id from atlas_idds.collections where transform_id=:transform_id"""
                stmt = text(select)
                result = session.execute(stmt, {'transform_id': transform_id})
            else:
                raise exceptions.WrongParamterException("Both request_id and workload_id are None.")

        collection_ids = result.fetchall()
        ret = [row[0] for row in collection_ids]
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collections with  transform_id(%s): %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_collections(scope, name, request_id=None, workload_id=None, session=None):
    """
    Get collections by request id or raise a NoObject exception.

    :param scope: collection scope.
    :param name: collection name, can be wildcard.
    :param request_id: The request id related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if request_id is not None:
            if workload_id is not None:
                select = """select * from atlas_idds.collections where soope=:scope
                            and name like :name and request_id=:request_id
                            and workload_id=:workload_id"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': '%' + name + '%',
                                                'request_id': request_id,
                                                'workload_id': workload_id})
            else:
                select = """select * from atlas_idds.collections where soope=:scope
                            and name like :name and request_id=:request_id"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': '%' + name + '%',
                                                'request_id': request_id})
        else:
            if workload_id is not None:
                select = """select * from atlas_idds.collections where soope=:scope
                            and name like :name and workload_id=:workload_id"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': '%' + name + '%',
                                                'workload_id': workload_id})
            else:
                select = """select * from atlas_idds.collections where soope=:scope
                            and name like :name"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': '%' + name + '%'})

        collections = result.fetchall()
        ret = []
        for collection in collections:
            collection = row2dict(collection)
            if collection['coll_type'] is not None:
                collection['coll_type'] = CollectionType(collection['coll_type'])
            if collection['in_out_type'] is not None:
                collection['in_out_type'] = CollectionRelationType(collection['in_out_type'])
                collection['relation_type'] = collection['in_out_type']
            if collection['coll_status'] is not None:
                collection['coll_status'] = CollectionStatus(collection['coll_status'])
            if collection['coll_metadata']:
                collection['coll_metadata'] = json.loads(collection['coll_metadata'])
            ret.append(collection)
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collection with  scope(%s), name(%s), request_id(%s), workload_id(%s): %s' %
                                  (scope, name, request_id, workload_id, error))
    except Exception as error:
        raise error


@read_session
def get_collection_id_by_scope_name(scope, name, request_id=None, workload_id=None, session=None):
    """
    Get collection id by scope, name, request id or raise a NoObject exception.

    :param scope: collection scope.
    :param name: collection name, should not be wildcards.
    :param request_id: The request id related to this collection.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    try:
        if request_id is not None:
            if workload_id is not None:
                select = """select * from atlas_idds.collections where soope=:scope
                            and name=:name and request_id=:request_id
                            and workload_id=:workload_id"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': name,
                                                'request_id': request_id,
                                                'workload_id': workload_id})
            else:
                select = """select * from atlas_idds.collections where soope=:scope
                            and name=:name and request_id=:request_id"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': name,
                                                'request_id': request_id})
        else:
            if workload_id is not None:
                select = """select * from atlas_idds.collections where soope=:scope
                            and name=:name and workload_id=:workload_id"""
                stmt = text(select)
                result = session.execute(stmt, {'scope': scope, 'name': name,
                                                'workload_id': workload_id})
            else:
                raise exceptions.WrongParameterException("Either request_id or workload_id should not be None")

        collection_id = result.fetchone()
        if collection_id is None:
            raise sqlalchemy.orm.exc.NoResultFound()
        return collection_id[0]
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No collection with  scope(%s), name(%s), request_id(%s), workload_id(%s): %s' %
                                  (scope, name, request_id, workload_id, error))
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
        if 'coll_status' in parameters and isinstance(parameters['coll_status'], CollectionStatus):
            parameters['coll_status'] = parameters['coll_status'].value
        if 'relation_type' in parameters and isinstance(parameters['relation_type'], CollectionRelationType):
            relation_type = parameters['relation_type'].value
            del parameters['relation_type']
            parameters['in_out_type'] = relation_type

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

    :param request_id: The id of the request.
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
