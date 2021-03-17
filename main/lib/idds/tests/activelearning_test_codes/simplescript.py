import random
# import numpy as np
import math
import time
import json
import sys


def generator():
    while True:
        yield random.random()


def evgen_single_point(x, nevents=10000, outputfile='out.json'):
    # generate n events and fill "histo"
    m1, m2 = x
    thresh = (1 + math.tan(m1 - m2)) / 2.
    accepted_events = []
    for i, rn in enumerate(generator()):
        if i == nevents:
            break
        time.sleep(0.001)
        rnd = random.random()
        if rnd < thresh:
            accepted_events.append(1)
    json.dump({'accepted': float(sum(accepted_events))}, open(outputfile, 'w'))


if __name__ == '__main__':
    evgen_single_point([float(sys.argv[1]), float(sys.argv[2])], nevents=int(sys.argv[3]), outputfile=sys.argv[4])
