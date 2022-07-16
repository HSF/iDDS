
from idds.common.utils import json_dumps                 # noqa F401
from idds.common.constants import ContentStatus, ContentType, ContentRelationType, ContentLocking          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401
from idds.core import catalog as core_catalog            # noqa F401
from idds.orm.contents import get_input_contents         # noqa F401
from idds.core.transforms import release_inputs_by_collection, release_inputs_by_collection_old     # noqa F401


def get_input_dep(contents, request_id, transform_id, coll_id, scope, name):
    # print(request_id, transform_id, coll_id, scope, name)
    map_id = None
    for content in contents:
        if content['transform_id'] == transform_id and content['name'] == name and content['content_relation_type'] == ContentRelationType.Output:
            # print(content)
            print("output name: %s, status: %s" % (content['name'], content['status']))
            map_id = content['map_id']
        # elif coll_id and content['coll_id'] == coll_id and content['name'] == name and content['content_relation_type'] == ContentRelationType.Output:
        #    # print(content)
        #    map_id = content['map_id']

    print(map_id)
    deps = []
    for content in contents:
        if content['transform_id'] == transform_id and content['map_id'] == map_id and content['content_relation_type'] == ContentRelationType.InputDependency:
            # print(content)
            print("Input dependency name: %s, status: %s" % (content['name'], content['status']))
            deps.append({'request_id': content['request_id'],
                         'transform_id': content['transform_id'],
                         'coll_id': content['coll_id'],
                         'scope': content['scope'],
                         'name': content['name']})
    return deps


def get_transform_id(collections, coll_id):
    for coll in collections:
        if coll['coll_id'] == coll_id:
            return coll['transform_id']


def get_dep_link(collections, contents, request_id, transform_id, coll_id, scope, name, step=1):
    deps = get_input_dep(contents, request_id, transform_id, coll_id, scope, name)
    print("Step: %s" % step)
    print("(%s, %s, %s, %s, %s) depents on %s" % (request_id, transform_id, coll_id, scope, name, deps))
    step += 1
    for dep in deps:
        coll_id = dep['coll_id']
        transform_id = get_transform_id(collections, coll_id)
        dep['transform_id'] = transform_id
        get_dep_link(collections=collections, contents=contents, step=step, **dep)


def get_dep_links(request_id, transform_id, coll_id, scope, name, step=1):
    collections = core_catalog.get_collections(request_id=request_id)
    contents = core_catalog.get_contents(request_id=request_id)
    get_dep_link(collections, contents, request_id, transform_id, coll_id, scope, name, step=step)


if __name__ == '__main__':
    request_id = 1689
    transform_id = 14713
    coll_id = None
    scope = 'pseudo_dataset'
    name = '94248742-a255-4541-ab38-ab69364a4c88_transformPreSourceTable_23224_72+qgraphNodeId:94248742-a255-4541-ab38-ab69364a4c88+qgraphId:1657127232.749359-46373'

    # get_input_dep(request_id, transform_id, scope, name)
    get_dep_links(request_id, transform_id, coll_id, scope, name)
