import argparse
import yaml
import os
import pathlib
import subprocess


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def generate_new_files(src_file, dest_dir):
    # Load src_file
    with open(src_file, "r") as f:
        template = f.read()

    with open(src_file, "r") as f:
        full_config = yaml.safe_load(f)

    src_path = pathlib.Path(src_file)

    group_params = full_config.get("groupParameters", [])

    new_files = []
    if not group_params:
        new_file = os.path.join(dest_dir, f"{src_path.stem}_group_none.yaml")
        with open(new_file, "w") as f:
            f.write(template)
        new_files.append(new_file)
    else:
        # Generate files
        for i, group in enumerate(group_params, 1):
            filled = template.format_map(SafeDict(group))
            new_file = os.path.join(dest_dir, f"{src_path.stem}_group_{i}.yaml")
            with open(new_file, "w") as f:
                f.write(filled)
            new_files.append(new_file)
    return new_files


def generate_new_commands(new_files, command):
    new_commands = []
    if "{new_file}" in command:
        for new_file in new_files:
            temp_command = command
            new_command = temp_command.format_map(SafeDict({"new_file": new_file}))
            new_commands.append(new_command)
    else:
        for new_file in new_files:
            temp_command = command
            new_command = f"{temp_command} {new_file}"
            new_commands.append(new_command)
    return new_commands


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src_file", type=str, required=True,
                        help="The source YAML file with template + groupParameters")
    parser.add_argument("--dest_dir", type=str, default="outputs",
                        help="Directory where generated files are stored")
    parser.add_argument("--command", type=str, required=True,
                        help="The command to run. If '{new_file}' is in the command, "
                             "it will be replaced. Otherwise, the new file path will be appended.")
    parser.add_argument("--run_command", action="store_true",
                        help="Only print the generated command without executing if not setting")
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.dest_dir, exist_ok=True)

    new_files = generate_new_files(args.src_file, args.dest_dir)
    new_commands = generate_new_commands(new_files, args.command)

    if not args.run_command:
        print("[DRY RUN]:")
        for cmd in new_commands:
            print(f"{cmd}")
    else:
        for cmd in new_commands:
            print(f"[RUN] {cmd}")
            subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    """
    examples:
        python group_submission.py --src_file test_cloud_us_group.yaml --command "bps submit" --run_command
        python group_submission.py --src_file test_cloud_us_group.yaml --command "bps submit {new_file}" --run_command
    """
    main()
