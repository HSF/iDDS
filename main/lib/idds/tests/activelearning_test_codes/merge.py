import sys
import json
# import numpy as np
# import pyhf
import time


# This is Rivet or SimpleAnalysis after merge / hadd
def merge_parallel_runs_and_analyze(r, nevents):
    import numpy as np
    import pyhf
    eff = np.sum(r) / nevents
    xsec = 30
    lumi = 1
    nevents = xsec * lumi * eff
    m = pyhf.simplemodels.hepdata_like([nevents], [50], [1])
    d = [50] + m.config.auxdata
    return np.log(pyhf.infer.hypotest(1.0, d, m)) - np.log(0.05)


if __name__ == '__main__':
    outputfile = sys.argv[1]
    nevents = int(sys.argv[2])
    # results = [json.load(open(x))['accepted'] for x in sys.argv[3:]]
    # result = {'objective': merge_parallel_runs_and_analyze(results,nevents)[0]}
    rand = int(time.time())
    if rand % 3 in [0, 1]:
        result = {'m1': 0.5, 'm2': 0.5, 'nevents': 100, 'output': 'output.json'}
    else:
        result = {}
    json.dump(result, open(outputfile, 'w'))
