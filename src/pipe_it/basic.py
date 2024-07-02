import argparse
from copy import deepcopy
import importlib
import yaml
from pprint import pprint
from munch import Munch
import collections
from pathlib import Path
import subprocess
import importlib.util
import sys
import os


def get_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "pipeline_config",
        help="",
    )
    parser.add_argument(
        "--output_dir", help="save outputs to this directory", default=None
    )
    parser.add_argument(
        "--skip",
        help="",
        nargs="+",
        default=[],
    )
    return parser.parse_args()


def get_git_revision_hash() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("ascii").strip()


def print_pipeline(scripts):
    print("==== Pipeline steps")
    for i, script in enumerate(scripts):
        print(f"({i})=>  {script['script']} {' '.join(script['args'])}")
        # pprint(script["args"], indent=3)
    print("====")


def _format_cfg(cfg, config_for_templating=None):
    if config_for_templating is None:
        config_for_templating = cfg

    if isinstance(cfg, str):
        return cfg.format(**config_for_templating)

    if isinstance(cfg, dict):
        cfg = Munch(cfg)
    for key, value in cfg.items():
        if isinstance(value, (dict, Munch)):
            cfg[key] = format_cfg(value, config_for_templating)
        elif isinstance(value, list):
            cfg[key] = [format_cfg(v, config_for_templating) for v in value]
        if isinstance(value, str):
            cfg[key] = value.format(**config_for_templating)

    return cfg


def format_cfg(cfg, config_for_templating=None):
    for _ in range(10):
        cfg = _format_cfg(cfg, config_for_templating=config_for_templating)
    return cfg


def recursive_to_dict(munch_obj: Munch):
    munch_obj = deepcopy(munch_obj)
    for key, item in munch_obj.items():
        if isinstance(item, Munch):
            munch_obj[key] = recursive_to_dict(item)
    return munch_obj.toDict()


def recursive_to_munch(d):
    d = Munch(deepcopy(d))
    for key, item in d.items():
        if isinstance(item, dict):
            d[key] = recursive_to_munch(item)
    return d


def force_import(file):
    name = Path(file).stem
    spec = importlib.util.spec_from_file_location(name, file)
    foo = importlib.util.module_from_spec(spec)
    sys.modules[name] = foo
    spec.loader.exec_module(foo)

    return foo


def get_argument_types(parser):
    arg_types = collections.OrderedDict()
    for a in parser._actions:
        if a.nargs is None:
            if len(a.option_strings) == 0:
                arg_types[a.dest] = "positional"
            else:
                arg_types[a.dest] = "value"
        elif a.nargs == 0:
            arg_types[a.dest] = "store_true"
        elif a.nargs == "+":
            if len(a.option_strings) == 0:
                arg_types[a.dest] = "positional_list"
            else:
                arg_types[a.dest] = "list"
    return arg_types


def prep_script(script, arguments):
    script_out = []
    arg_types = {}

    if isinstance(script, list):
        script_out = script
    elif isinstance(script, str) and script.endswith(".py"):
        path = Path(script).absolute()

        if not path.is_file():
            raise RuntimeError(f"ERROR: {path} doesn't exist")

        try:
            import_script = force_import(script)
            parser = import_script.get_parser()
            arg_types = get_argument_types(parser)
        except Exception as e:
            print(
                f"WARNING: not checking arguments for script {path}.\n It needs to define a function called get_parser that returns an argparse.ArgumentParser object"
            )
            print(e)

        script_out = ["python", str(path)]
    elif isinstance(script, str):
        script_out = script.split(" ")
    else:
        raise RuntimeError(f"`script` must be str or list, got {script}")

    if isinstance(arguments, (dict, Munch)):
        arguments = [{k: v} for k, v in arguments.__dict__.items()]

    final_args = []
    add_arg = lambda k: k and final_args.append(f"--{ k }")
    add_value = lambda v: final_args.append(str(v))
    for data in arguments:
        if isinstance(data, dict):
            key, value = list(data.items())[0]
        elif isinstance(data, Munch):
            key, value = list(data.__dict__.items())[0]
        else:
            key = None
            value = data

        if isinstance(value, list):
            if arg_types and key and arg_types.get(key, "") != "list":
                print(
                    f"WARNING: arg {key} is not defined as list in argparser for {script}!"
                )

            # if key:
            add_arg(key)
            for v in value:
                add_value(v)
        elif isinstance(value, bool):
            if arg_types and arg_types.get(key, "") != "store_true":
                print(
                    f"WARNING: arg {key} is not defined as store true in argparser for {script}!"
                )

            if value:
                add_arg(key)
        else:
            if arg_types and not arg_types.get(key, ""):
                print(
                    f"WARNING: argument {key} not found in argument definition in argparser for {script}!"
                )

            # if key:
            add_arg(key)
            add_value(value)

    return {"script": script_out, "args": final_args}


def main():
    # Command line arguments
    args = get_args()

    # .yaml-file configs
    cfg = Munch()
    # Save our commit
    cfg.git_hash = get_git_revision_hash()
    # Overwrite output dir if needed
    if args.output_dir is not None:
        cfg.output_dir = args.output_dir

    # Replace patterns with variable values on config
    cfg = format_cfg(cfg, config_for_templating=Munch({**cfg, **cfg.stages}))

    # Save config on output directory
    Path(cfg.output_dir).mkdir(exist_ok=True, parents=True)  # pragma:ignore
    with open(f"{cfg.output_dir}/pipeline.yaml", "w") as f:
        f.write(yaml.safe_dump(cfg))

    # Prepare script paths and configs
    scripts = []
    for stage_name in cfg.stages:
        if stage_name not in args.skip:
            script = cfg.stages[stage_name]["path"]
            script_args = cfg.stages[stage_name]["args"]
            scripts.append(prep_script(script, script_args))

    print_pipeline(scripts)

    # Execute scripts
    for script_info in scripts:
        script = script_info["script"]
        args = script_info["args"]
        command = script + args
        print(f"|==\ Executing {script}.")
        subprocess.run(command, check=True)
        print(f"|==/ Script {script} execution complete!")


if __name__ == "__main__":
    main()
