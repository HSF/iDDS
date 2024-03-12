
import json
import hashlib
import os
import sys
import time
import traceback

import numpy as np     # noqa F401

from sklearn.metrics import roc_curve, auc, confusion_matrix, brier_score_loss, mean_squared_error, log_loss, roc_auc_score   # noqa F401
from sklearn import metrics                            # noqa F401
from sklearn.preprocessing import label_binarize       # noqa F401
from sklearn.neighbors import KNeighborsClassifier     # noqa F401

from sklearn.model_selection import cross_val_score    # noqa F401
from sklearn.model_selection import KFold              # noqa F401
from sklearn.preprocessing import LabelEncoder         # noqa F401
from sklearn import model_selection                    # noqa F401
from sklearn.metrics import roc_curve, auc, confusion_matrix           # noqa F401
from sklearn.model_selection import train_test_split                   # noqa F401

from sklearn.preprocessing import StandardScaler                       # noqa F401
from sklearn.model_selection import StratifiedKFold                    # noqa F401

from tabulate import tabulate                                          # noqa F401

import xgboost as xgb

from bayes_opt import BayesianOptimization, UtilityFunction


def load_data(workdir='/opt/bdt_0409/ttHyyML_had_single', analysis_type='had'):
    currentDir = os.path.dirname(os.path.realpath(__file__))

    print("CurrentDir: %s" % currentDir)
    print("WorkDir: %s" % workdir)
    workdir = os.path.abspath(workdir)
    print("Absolute WorkDir: %s" % workdir)
    os.chdir(workdir)
    sys.path.insert(0, workdir)
    sys.argv = ['test']

    if analysis_type == 'had':
        from load_data_real_auto import load_data_real_hadronic
        data, label = load_data_real_hadronic()
    elif analysis_type == 'lep':
        from load_data_real_auto import load_data_real_leptonic
        data, label = load_data_real_leptonic()
    sys.path.remove(workdir)
    os.chdir(currentDir)
    return data, label


def get_param(params, name, default):
    if params and name in params:
        return params[name]
    return default


def getAUC(y_test, score, y_val_weight=None):
    # fpr, tpr, _ = roc_curve(y_test, score, sample_weight=y_val_weight)
    # roc_auc = auc(fpr, tpr, True)
    print(y_test.shape)
    print(score.shape)
    if y_val_weight:
        print(y_val_weight.shape)
    return roc_auc_score(y_test, score, sample_weight=y_val_weight)


def getBrierScore(y_test, score):
    return 1-brier_score_loss(y_test, score)


def evaluateBrierScore(y_pred, data):
    label = data.get_label()
    return 'brierLoss', 1-brier_score_loss(y_pred, label)


def getRMSE(y_test, score):
    return mean_squared_error(y_test, score) ** 0.5


def getLogLoss(y_test, score):
    return log_loss(y_test, score)


def xgb_callback_save_model(model_name, period=1000):
    def callback(env):
        try:
            bst, i, _ = env.model, env.iteration, env.end_iteration
            if (i % period == 0):
                bst.save_model(model_name)
        except Exception:
            print(traceback.format_exc())
    return callback


def train_bdt(input_x, input_y, params=None, retMethod=None, hist=True, saveModel=False, input_weight=None):

    train, val = input_x
    y_train_cat, y_val_cat = input_y
    if input_weight:
        y_train_weight, y_val_weight = input_weight
    else:
        y_train_weight = None
        y_val_weight = None

    train = train.reshape((train.shape[0], -1))
    val = val.reshape((val.shape[0], -1))

    dTrain = xgb.DMatrix(train, label=y_train_cat, weight=y_train_weight)
    dVal = xgb.DMatrix(val, label=y_val_cat, weight=y_val_weight)

    # train model
    print('Train model.')

    # param = {'max_depth':10, 'eta':0.1, 'min_child_weight': 60, 'silent':1, 'objective':'binary:logistic', 'eval_metric': ['logloss', 'auc' ]}
    # param = {'max_depth':10, 'eta':0.1, 'min_child_weight': 1, 'silent':1, 'objective':'rank:pairwise', 'eval_metric': ['auc','logloss']}
    # def_params = {'max_depth':10, 'eta':0.005, 'min_child_weight': 15, 'silent':1, 'objective':'binary:logistic', 'eval_metric': ['auc','logloss']}
    # def_params = {'colsample_bytree': 0.7, 'silent': 0, 'eval_metric': ['auc', 'logloss'], 'scale_pos_weight': 1.4, 'max_delta_step': 0, 'nthread': 8, 'min_child_weight': 160, 'subsample': 0.8, 'eta': 0.04, 'objective': 'binary:logistic', 'alpha': 0.1, 'lambda': 10, 'seed': 10, 'max_depth': 10, 'gamma': 0.03, 'booster': 'gbtree'}
    # def_params = {'colsample_bytree': 0.7, 'silent': 0, 'eval_metric': ['auc', 'logloss'], 'scale_pos_weight': 1.4, 'max_delta_step': 0, 'nthread': 8, 'min_child_weight': 160, 'subsample': 0.8, 'eta': 0.04, 'objective': 'binary:logistic', 'alpha': 0.1, 'lambda': 10, 'seed': 10, 'max_depth': 10, u'gamma': 0.5, 'booster': 'gbtree'}
    # def_params = {'eval_metric': ['logloss', 'auc'], 'scale_pos_weight': 5.1067081406104631, 'max_delta_step': 4.6914331907848759, 'seed': 10, 'alpha': 0.1, 'booster': 'gbtree', 'colsample_bytree': 0.64067554676687111, 'nthread': 4, 'min_child_weight': 58, 'subsample': 0.76111573761360196, 'eta': 0.1966696564443787, 'objective': 'binary:logistic', 'max_depth': 10, 'gamma': 0.74055129530012553}
    def_params = {}
    if not params:
        params = {}
    if 'num_boost_round' not in params:
        params['num_boost_round'] = 100000
    if 'objective' not in params:
        params['objective'] = 'binary:logistic'

    for key in def_params:
        if key not in params:
            params[key] = def_params[key]

    if 'silent' not in params:
        params['silent'] = 0

    if hist:
        params['tree_method'] = 'hist'
        params['booster'] = 'gbtree'
        params['grow_policy'] = 'lossguide'
    params['nthread'] = 4
    params['booster'] = 'gbtree'

    start = time.time()
    evallist = [(dTrain, 'train'), (dVal, 'eval')]
    evals_result = {}

    try:
        save_model_callback = xgb_callback_save_model(params['model']+"temp" if params and 'model' in params else 'models/default_bdt_temp.h5')
        # with early stop
        if not saveModel:
            # bst = xgb.train(params, dTrain, params['num_boost_round'], evals=evallist, early_stopping_rounds=10, evals_result=evals_result, verbose_eval=False, callbacks=[save_model_callback])
            bst = xgb.train(params, dTrain, params['num_boost_round'], evals=evallist, early_stopping_rounds=10, evals_result=evals_result, verbose_eval=True)
        else:
            bst = xgb.train(params, dTrain, params['num_boost_round'], evals=evallist, early_stopping_rounds=10, evals_result=evals_result, callbacks=[save_model_callback])
        # bst = xgb.train(params, dTrain, params['num_boost_round'], evals=evallist, early_stopping_rounds=10, evals_result=evals_result, feval=evaluateBrierScore, callbacks=[save_model_callback])
        # bst = xgb.train(params, dTrain, params['num_boost_round'], evals=evallist, evals_result=evals_result, callbacks=[save_model_callback])
    except KeyboardInterrupt:
        print('Finishing on SIGINT.')
    print("CPU Training time: %s" % (time.time() - start))

    # test model
    print('Test model.')

    score = bst.predict(dVal)
    rmse = None
    logloss = None
    if 'num_class' in params and params['num_class'] == 3:
        y_val_cat_binary = label_binarize(y_val_cat, classes=[0, 1, 2])
        aucValue = getAUC(y_val_cat_binary[:, 0], score[:, 0])
        aucValue1 = getAUC(y_val_cat_binary[:, 1], score[:, 1])
        aucValue2 = getAUC(y_val_cat_binary[:, 2], score[:, 2])

        print("AUC: %s, %s, %s" % (aucValue, aucValue1, aucValue2))

        bslValue = getBrierScore(y_val_cat_binary[:, 0], score[:, 0])
        bslValue1 = getBrierScore(y_val_cat_binary[:, 1], score[:, 1])
        bslValue2 = getBrierScore(y_val_cat_binary[:, 2], score[:, 2])

        print("BrierScoreLoss: %s, %s, %s" % (bslValue, bslValue1, bslValue2))

        rmse = getRMSE(y_val_cat_binary[:, 0], score[:, 0])
        logloss = getLogLoss(y_val_cat_binary[:, 0], score[:, 0])
    else:
        aucValue = getAUC(y_val_cat, score)
        bslValue = getBrierScore(y_val_cat, score)
        rmse = None
        logloss = None
        auc = None                                      # noqa F811
        if 'auc' in evals_result['eval']:
            auc = evals_result['eval']['auc'][-1]
        if 'rmse' in evals_result['eval']:
            rmse = evals_result['eval']['rmse'][-1]
        if 'logloss' in evals_result['eval']:
            logloss = evals_result['eval']['logloss'][-1]
        print("params: %s #, Val AUC: %s, BrierScoreLoss: %s, xgboost rmse: %s, xgboost logloss: %s, xgboost auc: %s" % (params, aucValue, bslValue, rmse, logloss, auc))
        rmse = getRMSE(y_val_cat, score)
        logloss = getLogLoss(y_val_cat, score)
        print("params: %s #, Val AUC: %s, BrierScoreLoss: %s, Val rmse: %s, Val logloss: %s" % (params, aucValue, bslValue, rmse, logloss))

    # bst.save_model(params['model'] if params and 'model' in params else 'models/default_bdt.h5')

    print(bst.get_fscore())
    # print(bst.get_score())

    try:
        pass
        # from matplotlib import pyplot
        # print("Plot importance")
        # xgb.plot_importance(bst)
        # pyplot.savefig('plots/' + params['name'] if 'name' in params else 'default' + '_feature_importance.png')
        # pyplot.savefig('plots/' + params['name'] if 'name' in params else 'default' + '_feature_importance.eps')
    except Exception:
        print(traceback.format_exc())

    try:
        history = {'loss': evals_result['train']['logloss'], 'val_loss': evals_result['eval']['logloss'],
                   'acc': evals_result['train']['auc'], 'val_acc': evals_result['eval']['auc']}
    except Exception:
        print(traceback.format_exc())
        history = {}

    if retMethod:
        if retMethod == 'auc':
            return aucValue
        if retMethod == 'rmse':
            return rmse
        if retMethod == 'brier':
            return bslValue
        if retMethod == 'logloss':
            return logloss
    return score, history


def evaluate_bdt(input_x, input_y, opt_params, retMethod=None, hist=True, saveModel=False, input_weight=None, **kwargs):
    params = kwargs
    if not params:
        params = {}
    if params and 'max_depth' in params:
        params['max_depth'] = int(params['max_depth'])
    if params and 'num_boost_round' in params:
        params['num_boost_round'] = int(params['num_boost_round'])
    if params and 'seed' in params:
        params['seed'] = int(params['seed'])
    if params and 'max_bin' in params:
        params['max_bin'] = int(params['max_bin'])
    # params[''] = int(params[''])

    if opt_params:
        for opt in opt_params:
            if opt not in params:
                params[opt] = opt_params[opt]

    if retMethod and retMethod == 'auc':
        params['eval_metric'] = ['rmse', 'logloss', 'auc']
    elif retMethod and retMethod == 'logloss':
        params['eval_metric'] = ['rmse', 'auc', 'logloss']
    elif retMethod and retMethod == 'rmse':
        params['eval_metric'] = ['logloss', 'auc', 'rmse']
    elif retMethod:
        params['eval_metric'] = [retMethod]

    print(params)
    auc = train_bdt(input_x, input_y, params=params, retMethod=retMethod, hist=hist, saveModel=saveModel, input_weight=input_weight)   # noqa F811
    print("params: %s, ret: %s" % (params, auc))
    return auc


def optimize_bdt(input_x, input_y, opt_params, opt_method='auc', opt_ranges=None, hist=True, input_weight=None):
    eval_params = {
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
        # 'num_boost_round': (100000, 1000000),
        }

    explore_params1 = {                      # noqa F841
                      # 'eta': [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08],
                      'eta': [0.001, 0.004, 0.007, 0.03, 0.05, 0.07],
                      # 'scale_pos_weight': [1, 2, 3, 5, 7, 8],
                      'colsample_bytree': [0.67, 0.79, 0.76, 0.5, 0.4, 0.85],
                      'scale_pos_weight': [4.9, 2.8, 2.1, 4.7, 1.7, 4],
                      'max_delta_step': [5, 2.2, 1.67, 2.53, 8.8, 8],
                      'seed': [10, 20, 30, 40, 50, 60],
                      'min_child_weight': [50, 74, 53, 14, 45, 30],
                      'subsample': [0.78, 0.82, 0.6, 0.87, 0.5, 0.7],
                      'alpha': [0.1, 0.2, 0.3, 0.05, 0.15, 0.25],
                      # 'lambda': [50],
                      'max_depth': [6, 7, 10, 14, 19, 28],
                      'gamma': [0.47, 0.19, 0.33, 0.43, 0.5, 0.76],
                      # 'num_boost_round': [1000000]
                     }

    explore_params = {                        # noqa F841
                      'eta': [0.004, 0.03],
                      # 'scale_pos_weight': [3, 7],
                      'colsample_bytree': [0.67, 0.4],
                      'scale_pos_weight': [4.9, 1.7],
                      'max_delta_step': [2.2, 8],
                      'seed': [10, 50],
                      'min_child_weight': [50, 30],
                      'subsample': [0.78, 0.5],
                      'alpha': [0.2, 0.25],
                      # 'lambda': [50],
                      'max_depth': [10, 28],
                      'gamma': [0.47, 0.76],
                      # 'num_boost_round': [1000000]
                     }

    print("Eval: %s" % eval_params)
    optFunc = lambda **z: evaluate_bdt(input_x, input_y, opt_params, opt_method, hist=hist, saveModel=False, input_weight=input_weight, **z)   # noqa F731
    bayesopt = BayesianOptimization(optFunc, eval_params)
    # bayesopt.explore(explore_params)
    bayesopt.maximize(init_points=3, n_iter=5)
    # bayesopt.maximize(init_points=30, n_iter=200)
    # bayesopt.maximize(init_points=2, n_iter=2)
    # print(bayesopt.res)
    p = bayesopt.max
    print("Best params: %s" % p)


def optimize():
    data, label = load_data()
    train, val = data
    y_train_cat, y_val_cat = label

    opt_method = 'auc'
    opt_ranges = {"subsample": [0.10131415926000001, 1],
                  "eta": [0.0100131415926, 0.03],
                  "colsample_bytree": [0.10131415926000001, 1],
                  "gamma": [0.00131415926, 1],
                  "alpha": [0.00131415926, 1],
                  "max_delta_step": [0.00131415926, 10],
                  "max_depth": [5.00131415926, 50],
                  "min_child_weight": [0.00131415926, 100]}
    params = {'num_boost_round': 10}

    optimize_bdt(input_x=[train, val], input_y=[y_train_cat, y_val_cat], opt_params=params,
                 opt_ranges=opt_ranges, opt_method=opt_method, hist=True, input_weight=None)


def get_unique_id_for_dict(dict_):
    ret = hashlib.sha1(json.dumps(dict_, sort_keys=True).encode()).hexdigest()
    return ret


def optimize_bdt1(input_x, input_y, opt_params, opt_method='auc', opt_ranges=None, hist=True, input_weight=None):
    eval_params = {
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
        # 'num_boost_round': (100000, 1000000),
        }

    print("Eval: %s" % eval_params)
    optFunc = lambda **z: evaluate_bdt(input_x, input_y, opt_params, opt_method, hist=hist, saveModel=False, input_weight=input_weight, **z)   # noqa F731
    bayesopt = BayesianOptimization(optFunc, eval_params)
    util = UtilityFunction(kind='ucb',
                           kappa=2.576,
                           xi=0.0,
                           kappa_decay=1,
                           kappa_decay_delay=0)

    n_iterations, n_points_per_iteration = 3, 5
    for i in range(n_iterations):
        points = {}
        for j in range(n_points_per_iteration):
            x_probe = bayesopt.suggest(util)
            u_id = get_unique_id_for_dict(x_probe)
            print('x_probe (%s): %s' % (u_id, x_probe))
            points[u_id] = {'kwargs': x_probe}
            ret = evaluate_bdt(input_x, input_y, opt_params, retMethod=opt_method, hist=hist, saveModel=False, input_weight=input_weight, **x_probe)
            print('ret :%s' % ret)
            points[u_id]['ret'] = ret
            bayesopt.register(x_probe, ret)

    # bayesopt.explore(explore_params)
    # bayesopt.maximize(init_points=3, n_iter=5)
    # bayesopt.maximize(init_points=30, n_iter=200)
    # bayesopt.maximize(init_points=2, n_iter=2)
    print(bayesopt.res)
    p = bayesopt.max
    print("Best params: %s" % p)


def optimize1():
    data, label = load_data()
    train, val = data
    y_train_cat, y_val_cat = label

    opt_method = 'auc'
    opt_ranges = {"subsample": [0.10131415926000001, 1],
                  "eta": [0.0100131415926, 0.03],
                  "colsample_bytree": [0.10131415926000001, 1],
                  "gamma": [0.00131415926, 1],
                  "alpha": [0.00131415926, 1],
                  "max_delta_step": [0.00131415926, 10],
                  "max_depth": [5.00131415926, 50],
                  "min_child_weight": [0.00131415926, 100]}
    params = {'num_boost_round': 10}

    optimize_bdt1(input_x=[train, val], input_y=[y_train_cat, y_val_cat], opt_params=params,
                  opt_ranges=opt_ranges, opt_method=opt_method, hist=True, input_weight=None)


if __name__ == '__main__':
    optimize1()
