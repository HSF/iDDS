import argparse
import json
# import nevergrad as ng


parser = argparse.ArgumentParser()
parser.add_argument('--max_points', action='store', type=int, required=True, help='max number of points to be generated')
parser.add_argument('--num_points', action='store', type=int, required=True, help='number of points to be generated')
parser.add_argument('--input', action='store', required=True, help='input json file which includes all pre-generated points')
parser.add_argument('--output', action='store', required=True, help='output json file where outputs will be wrote')

args = parser.parse_args()


def get_input_points(input):
    points = []
    opt_space = {}
    with open(input) as input_json:
        opt_points = json.load(input_json)
    if 'points' in opt_points:
        points = opt_points['points']
    if 'opt_space' in opt_points:
        opt_space = opt_points['opt_space']
    return points, opt_space


def write_output_points(new_points, output):
    with open(args.output, 'w') as output_json:
        json.dump(new_points, output_json)


def generate_new_points(input_points, opt_space, max_points, num_points):
    if len(input_points) >= max_points:
        return []
    elif len(input_points) >= max_points - 1:
        return [{'point_type': 'stat'}]

    num_points = min(num_points, max_points - 1 - len(input_points))

    new_points = []
    for _ in range(num_points):
        point = {'point_type': 'toy'}
        new_points.append(point)
    # recommendation = optimizer.provide_recommendation()
    return new_points


input_points, opt_space = get_input_points(args.input)
new_points = generate_new_points(input_points, opt_space, args.max_points, args.num_points)
write_output_points(new_points, args.output)
