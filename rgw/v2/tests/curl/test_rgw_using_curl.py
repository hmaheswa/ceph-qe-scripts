"""
Usage: test_rgw_using_curl.py -c <input_yaml>

<input_yaml>
    Note: Following yaml can be used
    test_curl_transfer_encoding_chunked.yaml
    test_rgw_using_curl.yaml

Operation:

"""


import argparse
import logging
import os
import sys
import traceback

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))


from v2.lib import resource_op
from v2.lib.curl.resource_op import CURL
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.curl import reusable as curl_reusable
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

    curl_reusable.install_curl(version="7.88.1")
    all_users_info = resource_op.create_users(no_of_users_to_create=config.user_count)

    for each_user in all_users_info:
        user_name = each_user["user_id"]
        log.info(user_name)

        curl_auth = CURL(each_user, ssh_con)

        for bc in range(config.bucket_count):
            bucket_name = utils.gen_bucket_name_from_userid(user_name, rand_no=bc)
            curl_reusable.create_bucket(curl_auth, bucket_name)
            log.info(f"Bucket {bucket_name} created")

            # create objects
            objects_created_list = []
            if config.test_ops.get("create_object", False):
                # uploading data
                log.info("s3 objects to create: %s" % config.objects_count)
                for oc, size in list(config.mapped_sizes.items()):
                    config.obj_size = size
                    log.info(f"--------------iteration number {oc+1}--------------")
                    s3_object_name = utils.gen_s3_object_name(bucket_name, 1)
                    log.info("s3 object name: %s" % s3_object_name)
                    s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
                    log.info("s3 object path: %s" % s3_object_path)
                    log.info("upload type: normal")
                    # curl_reusable.upload_object(
                    #     curl_auth,
                    #     bucket_name,
                    #     s3_object_name,
                    #     TEST_DATA_PATH,
                    #     config,
                    #     Transfer_Encoding=config.test_ops.get("Transfer_Encoding"),
                    # )
                    curl_reusable.upload_multipart_object_with_failed_part_upload(
                        curl_auth,
                        bucket_name,
                        s3_object_name,
                        TEST_DATA_PATH,
                        config
                    )
                    objects_created_list.append(s3_object_name)
                    if config.test_ops["download_object"] is True:
                        curl_reusable.download_object(
                            curl_auth,
                            bucket_name,
                            s3_object_name,
                            TEST_DATA_PATH,
                            s3_object_path,
                        )
                    if config.local_file_delete is True:
                        log.info("deleting local file created after the upload")
                        utils.exec_shell_cmd("rm -rf %s" % s3_object_path)

                    curl_reusable.delete_object(curl_auth, bucket_name, s3_object_name)
            if config.test_ops.get("delete_bucket_object") is True:
                for obj in objects_created_list:
                    curl_reusable.delete_object(curl_auth, bucket_name, obj)
                curl_reusable.delete_bucket(curl_auth, bucket_name)
        if config.user_remove:
            s3_reusable.remove_user(each_user)

    # check for any crashes during the execution
    crash_info = s3_reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")


if __name__ == "__main__":
    test_info = AddTestInfo("test rgw operations using curl")

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(description="RGW S3 operations using Curl")
        parser.add_argument("-c", dest="config", help="RGW Test yaml configuration")
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
        config.read(ssh_con)
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
