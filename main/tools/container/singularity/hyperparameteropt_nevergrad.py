import argparse
import json
import nevergrad as ng


parser = argparse.ArgumentParser()
parser.add_argument('--max_points', action='store', required=True, help='max number of points to be generated')
parser.add_argument('--num_points', action='store', required=True, help='number of points to be generated')
parser.add_argument('--input', action='store', required=True, help='input json file which includes all pre-generated points')
parser.add_argument('--output', action='store', required=True, help='output json file where outputs will be wrote')

args = parser.parse_args()


def get_input_points(input):
    points = None
    opt_space = None
    with open(input) as input_json:
        opt_points = json.load(input_json)
    if 'points' in opt_points:
        points = opt_points['points']
    if 'opt_space' in opt_points:
        opt_space = opt_points['opt_space']
    return points, opt_space


def write_output_points(new_points, output):
    with open(args.output) as output_json:
        json.dump(new_points, output_json)


def get_ng_parameter(param):
    if type(param) in [tuple]:
        return ng.p.Scalar(param[0], param[0], param[1])
    elif type(param) in [list]:
        return ng.p.Choice(param)
    else:
        return None


def generate_new_points(input_points, opt_space, max_points, num_points):
    if len(input_points) > max_points:
        return []
    num_points = min(num_points, max_points - len(input_points))

    ng_opt_space = {}
    for opt in opt_space:
        value = get_ng_parameter(opt_space[opt])
        if value:
            ng_opt_space[opt] = value

    instrum = ng.p.Instrumentation(**ng_opt_space)

    optimizer = ng.optimizers.DiscreteOnePlusOne(parametrization=instrum, budget=max_points, num_workers=1)

    unfinished_points = []
    for input_point in input_points:
        point, loss = input_point
        if loss:
            optimizer.tell(point, loss)
        else:
            unfinished_points.append(point)

    new_points = []
    for _ in range(num_points):
        x = optimizer.ask()
        # TODO: check duplication
        if x in unfinished_points:
            continue
        new_points.append(x)
    # recommendation = optimizer.provide_recommendation()
    return new_points


input_points, opt_space = get_input_points(args.input)
new_points = generate_new_points(input_points, opt_space, args.max_points, args.num_points)
write_output_points(new_points, args.output)
