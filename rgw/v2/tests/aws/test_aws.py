"""
Usage: test_aws.py -c <input_yaml>

<input_yaml>
    Note: Following yaml can be used
    configs/test_aws_versioned_bucket_creation.yaml
    configs/test_complete_multipart_upload_etag_not_empty.yaml

Operation:

"""


import argparse
import json
import logging
import os
import random
import sys
import traceback

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))


from v2.lib import resource_op
from v2.lib.aws import auth as aws_auth
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.aws import reusable as aws_reusable
from v2.tests.s3_swift import reusable as s3_reusable
from v2.utils import utils
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo

log = logging.getLogger(__name__)
TEST_DATA_PATH = None


def test_exec(config, ssh_con):
    """
    Executes test based on configuration passed
    Args:
        config(object): Test configuration
    """
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())
    if config.test_ops.get("user_name", False):
        user_info = resource_op.create_users(
            no_of_users_to_create=config.user_count,
            user_names=config.test_ops["user_name"],
        )
    else:
        user_info = resource_op.create_users(no_of_users_to_create=config.user_count)

    for user in user_info:
        user_name = user["user_id"]
        log.info(user_name)
        endpoint = aws_reusable.get_endpoint(ssh_con)
        aws_auth.do_auth_aws(user)

        for bc in range(config.bucket_count):
            if config.test_ops.get("bucket_name", False):
                bkt_suffix = bc + 1
                bucket_name = config.test_ops["bucket_name"] + f"{bkt_suffix}"
            else:
                bucket_name = utils.gen_bucket_name_from_userid(user_name, rand_no=bc)
            aws_reusable.create_bucket(bucket_name, endpoint)
            log.info(f"Bucket {bucket_name} created")

            if config.test_ops.get("enable_version", False):
                log.info(f"bucket versioning test on bucket: {bucket_name}")
                aws_reusable.put_get_bucket_versioning(bucket_name, endpoint)
        if config.test_ops.get("verify_etag_for_complete_multipart_upload", False):
            log.info(
                f"Verifying ETag element for complete multipart upload is not empty string"
            )
            for oc, size in list(config.mapped_sizes.items()):
                config.obj_size = size
                key_name = utils.gen_s3_object_name(bucket_name, 1)
                complete_multipart_upload_resp = aws_reusable.multipart_object_upload_with_failed_upload_parts(
                    bucket_name,
                    key_name,
                    TEST_DATA_PATH,
                    endpoint,
                    config,
                )
                if not complete_multipart_upload_resp["ETag"]:
                    raise AssertionError(
                        "Etag not generated during complete multipart upload operation"
                    )
                log.info(f"Download multipart object {key_name}")
                aws_reusable.get_object(bucket_name, key_name, endpoint)
                aws_reusable.delete_object(bucket_name, key_name, endpoint)

        if config.test_ops.get("verify_non_ascii_character_upload", False):
            log.info(f"Object name and body containing non ascii character upload")
            object_name = "ˍ´--øÆ.txt"
            utils.exec_shell_cmd(f"fallocate -l 1K {object_name}")
            aws_reusable.put_object(bucket_name, object_name, endpoint)
            log.info("Object upload successful")
            aws_reusable.get_object(bucket_name, object_name, endpoint)
            log.info("Object download successful")

        if config.user_remove is True:
            s3_reusable.remove_user(user)

    # check for any crashes during the execution
    crash_info = s3_reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")


if __name__ == "__main__":

    test_info = AddTestInfo("test bucket creation through awscli")

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(description="RGW S3 bucket creation using AWS")
        parser.add_argument(
            "-c", dest="config", help="RGW S3 bucket creation using AWS"
        )
        parser.add_argument(
            "-log_level",
            dest="log_level",
            help="Set Log Level [DEBUG, INFO, WARNING, ERROR, CRITICAL]",
            default="info",
        )
        parser.add_argument(
            "--rgw-node", dest="rgw_node", help="RGW Node", default="127.0.0.1"
        )
        args = parser.parse_args()
        yaml_file = args.config
        rgw_node = args.rgw_node
        ssh_con = None
        if rgw_node != "127.0.0.1":
            ssh_con = utils.connect_remote(rgw_node)
        log_f_name = os.path.basename(os.path.splitext(yaml_file)[0])
        configure_logging(f_name=log_f_name, set_level=args.log_level.upper())
        config = resource_op.Config(yaml_file)
        config.read()
        if config.mapped_sizes is None:
            config.mapped_sizes = utils.make_mapped_sizes(config)
        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)

    finally:
        utils.cleanup_test_data_path(TEST_DATA_PATH)
