#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


"""
Test workflow.
"""

import inspect     # noqa F401
import logging
import os          # noqa F401
import shutil      # noqa F401
import sys         # noqa F401

# from nose.tools import assert_equal
# from idds.common.imports import get_func_name
from idds.common.utils import setup_logging, get_unique_id_for_dict

from idds.iworkflow.workflow import Workflow       # workflow
from idds.iworkflow.work import work


setup_logging(__name__)


def get_initial_parameter():
    opt_params = {
        'colsample_bytree': (0.1, 1),
        'scale_pos_weight': (0, 10),
        'max_delta_step': (0, 10),
        'seed': (1, 50),
        'min_child_weight': (0, 100),
        'subsample': (0.1, 1),
        'eta': (0, 0.1),
        'alpha': (0, 1),
        # 'lambda': (0, 100),
        'max_depth': (0, 50),
        'gamma': (0, 1),
        # 'num_boost_round': (100000, 1000000)
    }
    return opt_params


@work(map_results=True)
def optimize_work(opt_params, retMethod=None, hist=True, saveModel=False, input_weight=None, **kwargs):
    from optimize import evaluate_bdt, load_data

    data, label = load_data()
    train, val = data
    y_train_cat, y_val_cat = label
    input_x = [train, val]
    input_y = [y_train_cat, y_val_cat]

    ret = evaluate_bdt(input_x=input_x, input_y=input_y, opt_params=opt_params, retMethod=retMethod, hist=hist,
                       saveModel=saveModel, input_weight=input_weight, **kwargs)
    return ret


def optimize_workflow():
    from optimize import evaluate_bdt, get_bayesian_optimizer_and_util

    opt_method = 'auc'
    params = {'num_boost_round': 1000}

    opt_params = get_initial_parameter()

    print("To optimize with parameters: %s" % opt_params)

    optFunc = lambda **z: evaluate_bdt(input_x=None, input_y=None,
                                       opt_params=params, opt_method=opt_method,
                                       hist=True, saveModel=False, input_weight=None, **z)   # noqa F731
    bayesopt, util = get_bayesian_optimizer_and_util(optFunc, opt_params)

    n_iterations, n_points_per_iteration = 10, 20
    for i in range(n_iterations):
        print("Iteration %s" % i)
        points = {}
        multi_jobs_kwargs_list = []
        for j in range(n_points_per_iteration):
            x_probe = bayesopt.suggest(util)
            u_id = get_unique_id_for_dict(x_probe)
            print('x_probe (%s): %s' % (u_id, x_probe))
            points[u_id] = {'kwargs': x_probe}
            multi_jobs_kwargs_list.append(x_probe)

        results = optimize_work(opt_params=params, opt_method=opt_method, hist=True, saveModel=False, input_weight=None,
                                retMethod=opt_method, multi_jobs_kwargs_list=multi_jobs_kwargs_list)
        print("points: %s" % str(points))

        for u_id in points:
            points[u_id]['ret'] = results.get_result(name=None, args=points[u_id]['kwargs'])
            print('ret :%s, kwargs: %s' % (points[u_id]['ret'], points[u_id]['kwargs']))
            bayesopt.register(points[u_id]['kwargs'], points[u_id]['ret'])

    print(bayesopt.res)
    p = bayesopt.max
    print("Best params: %s" % p)
    return p


if __name__ == '__main__':
    logging.info("start")
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    # wf = Workflow(func=test_workflow, service='idds', distributed=False)
    # init_env = 'singularity exec /eos/user/w/wguan/idds_ml/singularity/idds_ml_al9.simg '
    init_env = 'singularity exec /afs/cern.ch/user/w/wguan/workdisk/iDDS/test/eic/idds_ml_al9.simg '
    wf = Workflow(func=optimize_workflow, service='idds', init_env=init_env)

    # wf.queue = 'BNL_OSG_2'
    wf.queue = 'FUNCX_TEST'
    wf.cloud = 'US'

    logging.info("prepare workflow")
    wf.prepare()
    logging.info("prepared workflow")

    wf.submit()
