"""
test sse-s3 encryption with vault backend at per-bucket or per-object level.
Additionally, also test sse-kms with per-object
Usage: test_sse_s3_with_vault.py -c <input_yaml>
<input_yaml>
    Note: any one of these yamls can be used
    test_sse_s3_per_bucket_encryption_normal_object_upload.yaml
    test_sse_s3_per_bucket_encryption_multipart_object_upload.yaml
    test_sse_s3_per_bucket_encryption_version_enabled.yaml
    test_sse_s3_per_object.yaml
    test_sse_s3_per_object_versioninig_enabled.yaml
    test_sse_kms_per_object.yaml
    test_sse_kms_per_object_versioninig_enabled.yaml

Operation:
    Create a user and create a bucket with user credentials
    enable per-bucket or per-object sse-s3 encryption with vault backend
    test objects uploaded are encrypted with AES256.

"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import argparse
import hashlib
import json
import logging
import random
import time
import traceback
import uuid

import v2.lib.manage_data as manage_data
import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.lib.rgw_config_opts import CephConfOp, ConfigOpts
from v2.lib.s3.auth import Auth
from v2.lib.s3.write_io_info import BasicIOInfoStructure, BucketIoInfo, IOInfoInitialize
from v2.tests.s3_swift import reusable
from v2.tests.s3_swift.reusables import server_side_encryption_s3 as sse_s3
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo
from v2.utils.utils import RGWService
from v2.lib.s3 import lifecycle_validation as lc_ops

log = logging.getLogger()
TEST_DATA_PATH = None


def test_exec(config, ssh_con):
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    write_bucket_io_info = BucketIoInfo()
    io_info_initialize.initialize(basic_io_structure.initial())
    ceph_conf = CephConfOp(ssh_con)
    rgw_service = RGWService()

    if config.test_lc_transition:
        log.info("making changes to ceph.conf")
        ceph_conf.set_to_ceph_conf(
            "global",
            ConfigOpts.rgw_lc_debug_interval,
            str(config.rgw_lc_debug_interval),
            ssh_con,
        )
        if not config.rgw_enable_lc_threads:
            ceph_conf.set_to_ceph_conf(
                "global",
                ConfigOpts.rgw_enable_lc_threads,
                str(config.rgw_enable_lc_threads),
                ssh_con,
            )
        ceph_conf.set_to_ceph_conf(
            "global",
            ConfigOpts.rgw_lifecycle_work_time,
            str(config.rgw_lifecycle_work_time),
            ssh_con,
        )
        _, version_name = utils.get_ceph_version()
        if "nautilus" in version_name:
            ceph_conf.set_to_ceph_conf(
                "global",
                ConfigOpts.rgw_lc_max_worker,
                str(config.rgw_lc_max_worker),
                ssh_con,
            )
        else:
            ceph_conf.set_to_ceph_conf(
                section=None,
                option=ConfigOpts.rgw_lc_max_worker,
                value=str(config.rgw_lc_max_worker),
                ssh_con=ssh_con,
            )

        log.info("Set the Bucket LC transitions pre-requisites.")
        reusable.prepare_for_bucket_lc_transition(config)

    # create user
    all_users_info = s3lib.create_users(config.user_count, "hsm")
    for each_user in all_users_info:
        # authenticate
        auth = Auth(each_user, ssh_con, ssl=config.ssl)
        rgw_conn = auth.do_auth()
        rgw_conn2 = auth.do_auth_using_client()

        # authenticate with s3 client
        s3_client = auth.do_auth_using_client()

        # # get ceph version
        # ceph_version_id, ceph_version_name = utils.get_ceph_version()
        # is_multisite = utils.is_cluster_multisite()
        # if config.encryption_keys == "s3" and not is_multisite:
        #     log.info("sse-s3 configuration will be added now.")
        #     ceph_conf.set_to_ceph_conf(
        #         "global",
        #         ConfigOpts.rgw_crypt_require_ssl,
        #         str(config.rgw_crypt_require_ssl),
        #         ssh_con,
        #     )
        #     ceph_conf.set_to_ceph_conf(
        #         "global",
        #         ConfigOpts.rgw_crypt_sse_s3_backend,
        #         str(config.rgw_crypt_sse_s3_backend),
        #         ssh_con,
        #     )
        #     ceph_conf.set_to_ceph_conf(
        #         "global",
        #         ConfigOpts.rgw_crypt_sse_s3_vault_auth,
        #         str(config.rgw_crypt_sse_s3_vault_auth),
        #         ssh_con,
        #     )
        #     ceph_conf.set_to_ceph_conf(
        #         "global",
        #         ConfigOpts.rgw_crypt_sse_s3_vault_prefix,
        #         str(config.rgw_crypt_sse_s3_vault_prefix),
        #         ssh_con,
        #     )
        #     ceph_conf.set_to_ceph_conf(
        #         "global",
        #         ConfigOpts.rgw_crypt_sse_s3_vault_secret_engine,
        #         str(config.rgw_crypt_sse_s3_vault_secret_engine),
        #         ssh_con,
        #     )
        #     ceph_conf.set_to_ceph_conf(
        #         "global",
        #         ConfigOpts.rgw_crypt_sse_s3_vault_addr,
        #         str(config.rgw_crypt_sse_s3_vault_addr),
        #         ssh_con,
        #     )
        #     log.info("trying to restart services")
        #     srv_restarted = rgw_service.restart(ssh_con)
        #     time.sleep(30)
        #     if srv_restarted is False:
        #         raise TestExecError("RGW service restart failed")
        #     else:
        #         log.info("RGW service restarted")
        objects_created_list = []
        if config.test_ops["create_bucket"] is True:
            log.info("no of buckets to create: %s" % config.bucket_count)
            bucket_count = int(utils.exec_shell_cmd("radosgw-admin bucket list --uid=hsm | grep hsm | wc -l"))
            for bc in range(config.bucket_count):
                bucket_name_to_create = utils.gen_bucket_name_from_userid(
                    each_user["user_id"], rand_no=bc+bucket_count
                )
                bucket = reusable.create_bucket(
                    bucket_name_to_create, rgw_conn, each_user
                )
                if config.test_ops.get("enable_version", False):
                    log.info("enable bucket version")
                    reusable.enable_versioning(
                        bucket, rgw_conn, each_user, write_bucket_io_info
                    )

                upload_start_time = time.time()
                # create objects
                if config.test_ops["create_object"] is True:
                    # uploading data
                    log.info("s3 objects to create: %s" % config.objects_count)
                    for oc, size in list(config.mapped_sizes.items()):
                        log.info(f"print {oc}")
                        config.obj_size = size
                        s3_object_name = utils.gen_s3_object_name(
                            bucket_name_to_create, oc
                        )
                        log.info("s3 object name: %s" % s3_object_name)
                        s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
                        log.info("s3 object path: %s" % s3_object_path)

                        if config.test_lc_transition is False:
                            # write a few objects and then enabled encryption on the bucket
                            reusable.get_object_upload_type(
                                s3_object_name,
                                bucket,
                                TEST_DATA_PATH,
                                config,
                                each_user,
                            )

                        # Choose encryption type, per-object or per-bucket:
                        log.info("Choose encryption type, per-object or per-bucket")
                        # Choose the encryption_method sse-s3 or sse-kms
                        encryption_method = config.encryption_keys
                        if config.test_ops["sse_s3_per_bucket"] is True:
                            log.info(
                                f"Encryption type is per-bucket, enable it on bucket : {bucket_name_to_create}"
                            )
                            sse_s3.put_bucket_encryption(
                                s3_client, bucket_name_to_create, encryption_method
                            )
                            # get bucket encryption
                            log.info(
                                f"get bucket encryption for bucket : {bucket_name_to_create}"
                            )
                            sse_s3.get_bucket_encryption(
                                s3_client, bucket_name_to_create
                            )
                            reusable.get_object_upload_type(
                                s3_object_name,
                                bucket,
                                TEST_DATA_PATH,
                                config,
                                each_user,
                            )

                        else:
                            log.info(f"Encryption type is per-object.")
                            log.info(
                                f"Test sse with encryption keys {config.encryption_keys}"
                            )
                            sse_s3.put_object_encryption(
                                s3_client,
                                bucket_name_to_create,
                                s3_object_name,
                                encryption_method,
                                TEST_DATA_PATH,
                                config,
                                each_user,
                            )
                        objects_created_list.append(s3_object_name)
                        # test the object uploaded is encrypted with AES256
                        sse_s3.get_object_encryption(
                            s3_client, bucket_name_to_create, s3_object_name
                        )
                        if oc == 0 and config.test_ops.get(
                            "download_object_at_remote_site", False
                        ):
                            log.info(f"the object count is {oc}")
                            log.info(
                                "Wait for sync lease to catch up on the remote site."
                            )
                            time.sleep(60)
                        if config.test_ops.get("download_object_at_remote_site", False):
                            reusable.test_object_download_at_replicated_site(
                                bucket_name_to_create, s3_object_name, each_user, config
                            )
                        if config.test_ops["delete_bucket_object"]:
                            reusable.delete_objects(bucket)
                        elif config.test_ops["delete_bucket_object_version"]:
                            reusable.delete_versioned_object(
                                bucket,
                                s3_object_name,
                                s3_object_path,
                                rgw_conn,
                                each_user,
                            )
                    if config.test_sync_consistency_bucket_stats:
                        ##Verify the bugs 2236643 and 2247742
                        log.info("Test consistency in size(via bucket stats).")
                        reusable.test_bucket_stats_across_sites(
                            bucket_name_to_create, config
                        )
                upload_end_time = time.time()
                if config.test_lc_transition:
                    life_cycle_rule = {"Rules": config.lifecycle_conf}
                    reusable.put_get_bucket_lifecycle_test(
                        bucket,
                        rgw_conn,
                        rgw_conn2,
                        life_cycle_rule,
                        config,
                        upload_start_time,
                        upload_end_time,
                    )
                    log.info("sleeping for 30 seconds")
                    time.sleep(30)
                    lc_ops.validate_prefix_rule(bucket, config)
                if config.test_ops.get("download_object_after_transition", False):
                    for s3_object_name in objects_created_list:
                        s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
                        log.info("trying to download object: %s" % s3_object_name)
                        s3_object_download_name = s3_object_name + "." + "download"
                        s3_object_download_path = os.path.join(
                            TEST_DATA_PATH, s3_object_download_name
                        )
                        log.info(
                            "s3_object_download_path: %s" % s3_object_download_path
                        )
                        log.info(
                            "downloading to filename: %s" % s3_object_download_name
                        )
                        object_downloaded_status = s3lib.resource_op(
                            {
                                "obj": bucket,
                                "resource": "download_file",
                                "args": [
                                    s3_object_name,
                                    s3_object_download_path,
                                ],
                            }
                        )
                        if object_downloaded_status is False:
                            raise TestExecError(
                                "Resource execution failed: object download failed"
                            )
                        if object_downloaded_status is None:
                            log.info("object downloaded")
                        s3_object_downloaded_md5 = utils.get_md5(
                            s3_object_download_path
                        )
                        s3_object_uploaded_md5 = utils.get_md5(s3_object_path)
                        log.info(
                            "s3_object_downloaded_md5: %s"
                            % s3_object_downloaded_md5
                        )
                        log.info(
                            "s3_object_uploaded_md5: %s" % s3_object_uploaded_md5
                        )
                        if str(s3_object_uploaded_md5) == str(
                                s3_object_downloaded_md5
                        ):
                            log.info("md5 match")
                            utils.exec_shell_cmd(
                                "rm -rf %s" % s3_object_download_path
                            )
                        else:
                            raise TestExecError("md5 mismatch")

    # check sync status if a multisite cluster
    reusable.check_sync_status()

    # check for any crashes during the execution
    crash_info = reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")


if __name__ == "__main__":
    test_info = AddTestInfo("test server-side-encryption with s3 and kms")
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        rgw_service = RGWService()
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info("TEST_DATA_PATH: %s" % TEST_DATA_PATH)
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(description="RGW S3 Automation")
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
        config = Config(yaml_file)
        ceph_conf = CephConfOp(ssh_con)
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
