import logging

import v2.utils.utils as utils
from v2.lib.exceptions import TestExecError
from v2.tests.s3_swift import reusable
from itertools import permutations

log = logging.getLogger()


def execute_command_with_permutations(sample_cmd, config):
    """executes command and checks for"""
    special_characters = [
        "a",
        "~",
        "!",
        "@",
        "#",
        "$",
        "%",
        "^",
        "-",
        "_",
        "/",
        "?",
        "+",
        "=",
        ":",
        ",",
        ".",
    ]
    random_strings_list = [
        "".join(p) for p in permutations(special_characters, config.permutation_count)
    ]
    random_strings = "("
    for r in random_strings_list:
        random_strings = random_strings + "'" + r + "' "
    random_strings = random_strings + ")"

    # execute the command with malformed s3uri, refer this bz https://bugzilla.redhat.com/show_bug.cgi?id=2138921
    s3uri = "s3://https:///example.com/%2f.."
    cmd = f"/home/cephuser/venv/bin/{sample_cmd.replace('s3uri', s3uri)};"

    # execute the command with special characters at the end
    utils.exec_long_running_shell_cmd(cmd)
    cmd = (
        f"random_strings={random_strings};"
        + "for i in ${random_strings[@]};"
        + f"do echo {sample_cmd.replace('s3uri', 's3://http${i}')};"
        + f"/home/cephuser/venv/bin/{sample_cmd.replace('s3uri', 's3://http${i}')};"
        + "done;"
    )
    out = utils.exec_long_running_shell_cmd(cmd)
    log.info(out)

    # execute the command with special characters at the start
    cmd = (
        f"random_strings={random_strings};"
        + "for i in ${random_strings[@]};"
        + f"do echo {sample_cmd.replace('s3uri', 's3://${i}http')};"
        + f"/home/cephuser/venv/bin/{sample_cmd.replace('s3uri', 's3://${i}http')};"
        + "done;"
    )
    out = utils.exec_long_running_shell_cmd(cmd)
    log.info(out)

    # check for any crashes during the execution
    crash_info = reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")
