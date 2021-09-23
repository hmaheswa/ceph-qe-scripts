"""
core_workflows module is a rados layer configuration module for Ceph cluster.
It allows us to perform various day1 and day2 operations such as
1. Creating , modifying, setting , getting, writing, scrubbing, reading various pools like EC and replicated
2. Increase decrease PG counts, enable - disable - configure modules that do this
3. Enable logging to file, set and reset config params and cluster checks
4. Set-up email alerts and other cluster operations
More operations to be added as needed

"""

import json
import logging
import re
import time

from ceph.ceph_admin import CephAdmin

log = logging.getLogger(__name__)


class RadosOrchestrator:
    """
    RadosOrchestrator class contains various methods that perform various day1 and day2 operations on the cluster
    Usage: The class is initialized with the CephAdmin object for various operations
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run rados commands
        Args:
            node: CephAdmin object
        """
        self.node = node
        self.ceph_cluster = node.cluster

    def enable_email_alerts(self, **kwargs) -> bool:
        """
        Enables the email alerts module and configures alerts to be sent
        References : https://docs.ceph.com/en/latest/mgr/alerts/
        Args:
            **kwargs: Any other param that needs to be set
            Various args that can be passed are :
            1. smtp_host
            2. smtp_sender
            3. smtp_ssl
            4. smtp_port
            5. interval
            6. smtp_from_name
            7. smtp_destination
        Returns: True -> pass, False -> fail
        """
        alert_cmds = {
            "smtp_host": f"ceph config set mgr mgr/alerts/smtp_host "
            f"{kwargs.get('smtp_host', 'smtp.corp.redhat.com')}",
            "smtp_sender": f"ceph config set mgr mgr/alerts/smtp_sender "
            f"{kwargs.get('smtp_sender', 'ceph-iad2-c01-lab.mgr@redhat.com')}",
            "smtp_ssl": f"ceph config set mgr mgr/alerts/smtp_ssl {kwargs.get('smtp_ssl', 'false')}",
            "smtp_port": f"ceph config set mgr mgr/alerts/smtp_port {kwargs.get('smtp_port', '25')}",
            "interval": f"ceph config set mgr mgr/alerts/interval {kwargs.get('interval', '5')}",
            "smtp_from_name": f"ceph config set mgr mgr/alerts/smtp_from_name "
            f"'{kwargs.get('smtp_from_name', 'Rados 5.0 sanity Cluster')}'",
        }
        cmd = "ceph mgr module enable alerts"
        self.node.shell([cmd])

        for cmd in alert_cmds.values():
            self.node.shell([cmd])

        if kwargs.get("smtp_destination"):
            for email in kwargs.get("smtp_destination"):
                cmd = f"ceph config set mgr mgr/alerts/smtp_destination {email}"
                self.node.shell([cmd])
        else:
            log.error("email addresses not provided")
            return False

        # Printing all the configuration set
        cmd = "ceph config dump"
        log.info(self.run_ceph_command(cmd))

        # Disabling and enabling the email alert module after setting all the config
        states = ["disable", "enable"]
        for state in states:
            cmd = f"ceph mgr module {state} alerts"
            self.node.shell([cmd])
            time.sleep(1)

        # Triggering email alert
        try:
            cmd = "ceph alerts send"
            self.node.shell([cmd])
        except Exception:
            log.error("Error while Sending email alerts")
            return False

        log.info("Email alerts configured on the cluster")
        return True

    def run_ceph_command(self, cmd: str) -> dict:
        """
        Runs ceph commands with json tag for the action specified otherwise treats action as command
        and returns formatted output
        Args:
            cmd: Command that needs to be run
        Returns: dictionary of the output
        """

        cmd = f"{cmd} -f json"
        out, err = self.node.shell([cmd])
        status = json.loads(out)
        return status

    def pool_inline_compression(self, pool_name: str, **kwargs) -> bool:
        """
        BlueStore supports inline compression using snappy, zlib, or lz4.
        This module sets various compression modes and other related configs
        Args:
            pool_name: pool name on which compression needs to be enabled and configured
            **kwargs: Various args that can be passed:
                1. compression_mode : Whether data in BlueStore is compressed is determined by  compression mode.
                    The modes are:
                        none: Never compress data.
                        passive: Do not compress data unless the write operation has a compressible hint set.
                        aggressive: Compress data unless the write operation has an incompressible hint set.
                        force: Try to compress data no matter what.
                2. compression_algorithm : compression algorithm to be used.
                    Supported:
                        <empty string>
                        snappy
                        zlib
                        zstd
                        lz4
                3. compression_required_ratio : The ratio of the size of the data chunk after compression.
                    eg : 0.7
                4. compression_min_blob_size : Chunks smaller than this are never compressed.
                    eg : 10B
                5. compression_max_blob_size : Chunks larger than this value are broken into smaller blobs
                    eg : 10G
        Returns: Pass -> true , Fail -> false
        """

        if pool_name not in self.list_pools():
            log.error(f"requested pool {pool_name} is not present on the cluster")
            return False

        value_map = {
            "compression_algorithm": kwargs.get("compression_algorithm", "snappy"),
            "compression_mode": kwargs.get("compression_mode", "none"),
            "compression_required_ratio": kwargs.get(
                "compression_required_ratio", 0.875
            ),
            "compression_min_blob_size": kwargs.get("compression_min_blob_size", "0B"),
            "compression_max_blob_size": kwargs.get("compression_max_blob_size", "0B"),
        }

        # Adding the config values
        for val in value_map.keys():
            if kwargs.get(val, False):
                cmd = f"ceph osd pool set {pool_name} {val} {value_map[val]}"
                self.node.shell([cmd])

        details = self.run_ceph_command(cmd="ceph osd dump")
        for detail in details["pools"]:
            if detail["pool_name"] == pool_name:
                compression_conf = detail["options"]
                if (
                    not compression_conf["compression_algorithm"]
                    == value_map["compression_algorithm"]
                ):
                    log.error("Compression algorithm not set")
                    return False
        # tbd: Verify if compression set is working as expected. Compression ratio to be maintained
        log.info(f"compression set on pool {pool_name} successfully")
        return True

    def list_pools(self) -> list:
        """
        Collect the list of pools present on the cluster
        Returns: list of pool names
        """
        cmd = "ceph df"
        out = self.run_ceph_command(cmd=cmd)
        return [entry["name"] for entry in out["pools"]]

    def get_pool_property(self, pool, props):
        """
        Used to fetch a given property set on the pool
        Args:
            pool: name of the pool
            props: property to be fetched.
            Allowed values :
            size|min_size|pg_num|pgp_num|crush_rule|hashpspool|nodelete|nopgchange|nosizechange|
            write_fadvise_dontneed|noscrub|nodeep-scrub|hit_set_type|hit_set_period|hit_set_count|
            hit_set_fpp|use_gmt_hitset|target_max_objects|target_max_bytes|cache_target_dirty_ratio|
            cache_target_dirty_high_ratio|cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|
            erasure_code_profile|min_read_recency_for_promote|all|min_write_recency_for_promote|fast_read|
            hit_set_grade_decay_rate|hit_set_search_last_n|scrub_min_interval|scrub_max_interval|
            deep_scrub_interval|recovery_priority|recovery_op_priority|scrub_priority|compression_mode|
            compression_algorithm|compression_required_ratio|compression_max_blob_size|
            compression_min_blob_size|csum_type|csum_min_block|csum_max_block|allow_ec_overwrites|
            fingerprint_algorithm|pg_autoscale_mode|pg_autoscale_bias|pg_num_min|target_size_bytes|
            target_size_ratio|dedup_tier|dedup_chunk_algorithm|dedup_cdc_chunk_size
        Returns: key value pair for the requested property
        Note : Trying to fetch the value for property, which has not been set will error out
        """
        # checking if the pool exists
        if pool not in self.list_pools():
            log.error(f"requested pool {pool} is not present on the cluster")
            return False

        cmd = f"ceph osd pool get {pool} {props} -f json"
        out, err = self.node.shell([cmd])
        if err:
            log.info(f"The property : {props} not set on the cluster. Error: {err}")
            return False
        prop_details = json.loads(out)
        return prop_details

    def set_pool_property(self, pool, props, value):
        """
        Used to fetch a given property set on the pool
        Args:
            pool: name of the pool
            props: property to be set on pool.
                Allowed values :
                size|min_size|pg_num|pgp_num|crush_rule|hashpspool|nodelete|nopgchange|nosizechange|
                write_fadvise_dontneed|noscrub|nodeep-scrub|hit_set_type|hit_set_period|hit_set_count|
                hit_set_fpp|use_gmt_hitset|target_max_objects|target_max_bytes|cache_target_dirty_ratio|
                cache_target_dirty_high_ratio|cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|
                erasure_code_profile|min_read_recency_for_promote|all|min_write_recency_for_promote|fast_read|
                hit_set_grade_decay_rate|hit_set_search_last_n|scrub_min_interval|scrub_max_interval|
                deep_scrub_interval|recovery_priority|recovery_op_priority|scrub_priority|compression_mode|
                compression_algorithm|compression_required_ratio|compression_max_blob_size|
                compression_min_blob_size|csum_type|csum_min_block|csum_max_block|allow_ec_overwrites|
                fingerprint_algorithm|pg_autoscale_mode|pg_autoscale_bias|pg_num_min|target_size_bytes|
                target_size_ratio|dedup_tier|dedup_chunk_algorithm|dedup_cdc_chunk_size
            value: value to be set for the property
        Returns: Pass -> True, Fail -> False
        """
        # checking if the pool exists
        if pool not in self.list_pools():
            log.error(f"requested pool {pool} is not present on the cluster")
            return False

        cmd = f"ceph osd pool set {pool} {props} {value}"
        out, err = self.node.shell([cmd])
        # sleeping for 2 seconds for the values to reflect
        time.sleep(2)
        log.info(f"property {props} set on pool {pool}")
        return True

    def bench_write(self, pool_name: str, **kwargs) -> bool:
        """
        Method to trigger Write operations via the Rados Bench tool
        Args:
            pool_name: pool on which the operation will be performed
            kwargs: Any other param that needs to passed
            1. rados_write_duration -> duration of write operation (int)
            2. byte_size -> size of objects to be written (str)
                eg : 10KB, 4096
        Returns: True -> pass, False -> fail
        """
        duration = kwargs.get("rados_write_duration", 200)
        byte_size = kwargs.get("byte_size", 4096)
        cmd = f"sudo rados --no-log-to-stderr -b {byte_size} -p {pool_name} bench {duration} write --no-cleanup"
        try:
            self.node.shell([cmd])
            return True
        except Exception as err:
            log.error(f"Error running rados bench write on pool : {pool_name}")
            log.error(err)
            return False

    def bench_read(self, pool_name: str, **kwargs) -> bool:
        """
        Method to trigger Read operations via the Rados Bench tool
        Args:
            pool_name: pool on which the operation will be performed
            kwargs: Any other param that needs to passed
                1. rados_read_duration -> duration of read operation (int)
        Returns: True -> pass, False -> fail
        """
        duration = kwargs.get("rados_read_duration", 80)
        try:
            cmd = f"rados --no-log-to-stderr -p {pool_name} bench {duration} seq"
            self.node.shell([cmd])
            cmd = f"rados --no-log-to-stderr -p {pool_name} bench {duration} rand"
            self.node.shell([cmd])
            return True
        except Exception as err:
            log.error(f"Error running rados bench write on pool : {pool_name}")
            log.error(err)
            return False

    def create_pool(self, pool_name: str, **kwargs) -> bool:
        """
        Create a pool named from the pool_name parameter.
         Args:
            pool_name: name of the pool being created.
            kwargs: Any other args that need to be passed
                1. pg_num -> number of PG's and PGP's
                2. ec_profile_name -> name of EC profile if pool being created is a EC pool
                3. min_size -> min replication size for pool for pool to serve data
                4. size -> min replication size for pool for pool to write data
                5. erasure_code_use_overwrites -> allows overrides in an erasure coded pool
                6. allow_ec_overwrites -> This lets RBD and CephFS store their data in an erasure coded pool
                7. disable_pg_autoscale -> sets auto-scale mode off on the pool
                8. crush_rule -> custom crush rule for the pool
                9. pool_quota -> limit the maximum number of objects or the maximum number of bytes stored
         Returns: True -> pass, False -> fail
        """

        log.info(f"creating pool_name {pool_name}")
        pg_num = kwargs.get("pg_num", 64)
        cmd = f"ceph osd pool create {pool_name} {pg_num} {pg_num}"
        if kwargs.get("ec_profile_name"):
            cmd = f"{cmd} erasure {kwargs['ec_profile_name']}"
        try:
            self.node.shell([cmd])
        except Exception as err:
            log.error(f"Error creating pool : {pool_name}")
            log.error(err)
            return False

        # Enabling rados application on the pool
        enable_app_cmd = f"sudo ceph osd pool application enable {pool_name} {kwargs.get('app_name', 'rados')}"
        self.node.shell([enable_app_cmd])

        cmd_map = {
            "min_size": f"ceph osd pool set {pool_name} min_size {kwargs.get('min_size')}",
            "size": f"ceph osd pool set {pool_name} size {kwargs.get('size')}",
            "erasure_code_use_overwrites": f"ceph osd pool set {pool_name} "
            f"allow_ec_overwrites {kwargs.get('erasure_code_use_overwrites')}",
            "disable_pg_autoscale": f"ceph osd pool set {pool_name} pg_autoscale_mode off",
            "crush_rule": f"sudo ceph osd pool set {pool_name} crush_rule {kwargs.get('crush_rule')}",
            "pool_quota": f"ceph osd pool set-quota {pool_name} {kwargs.get('pool_quota')}",
        }
        for key in kwargs:
            if cmd_map.get(key):
                try:
                    self.node.shell([cmd_map[key]])
                except Exception as err:
                    log.error(
                        f"Error setting the property : {key} for pool : {pool_name}"
                    )
                    log.error(err)
                    return False

        log.info(f"Created pool {pool_name} successfully")
        return True

    def change_recover_threads(self, config: dict, action: str):
        """
        increases or decreases the recovery threads based on the action sent
        Args:
            config: Config from the suite file for the run
            action: Set or remove increase the backfill / recovery threads
                Values : "set" -> set the threads to specified value
                         "rm" -> remove the config changes made
        """

        cfg_map = {
            "osd_max_backfills": f"ceph config {action} osd osd_max_backfills",
            "osd_recovery_max_active": f"ceph config {action} osd osd_recovery_max_active",
        }
        for cmd in cfg_map:
            if action == "set":
                command = f"{cfg_map[cmd]} {config.get(cmd, 8)}"
            else:
                command = cfg_map[cmd]
            self.node.shell([command])

    def get_pg_acting_set(self, **kwargs) -> list:
        """
        Fetches the PG details about the given pool and then returns the acting set of OSD's from sample PG of the pool
        Args:
            kwargs: Args that can be passed to fetch acting set
                pool_name: name of the pool whose one of the acting OSD set is needed.
                pg_num: pg whose acting set needs to be fetched
                None: Collects the acting set of pool with ID 1
            eg:
        Returns: list osd's part of acting set
        eg : [3,15,20]
        """
        if kwargs.get("pool_name"):
            pool_name = kwargs["pool_name"]
            # Collecting details about the cluster
            cmd = "ceph osd dump"
            out = self.run_ceph_command(cmd=cmd)
            for val in out["pools"]:
                if val["pool_name"] == pool_name:
                    pool_id = val["pool"]
                    break
            # Collecting the details of the 1st PG in the pool <ID>.0
            pg_num = f"{pool_id}.0"

        elif kwargs.get("pg_num"):
            pg_num = kwargs["pg_num"]

        else:
            # Collecting the acting set for a random pool ID 1 from cluster
            pg_num = "1.0"

        cmd = f"ceph pg map {pg_num}"
        out = self.run_ceph_command(cmd=cmd)
        return out["up"]

    def run_scrub(self, **kwargs):
        """
        Run scrub on the given OSD or on all OSD's
         Args:
            kwargs:
            1. osd : if a OSD id is passed , scrub to be triggered on that osd
                    eg: obj.run_scrub(osd=3)
         Returns: True -> pass, False -> fail
        """
        if kwargs.get("osd"):
            cmd = f"ceph osd scrub {kwargs.get('osd')}"
        else:
            # scrubbing all the OSD's
            cmd = "ceph osd scrub all"
        self.node.shell([cmd])

    def run_deep_scrub(self, **kwargs):
        """
        Run scrub on the given OSD or on all OSD's
            Args:
            kwargs:
            1. osd : if a OSD id is passed , scrub to be triggered on that osd
                    eg: obj.run_deep_scrub(osd=3)
            Returns: True -> pass, False -> fail
        """
        if kwargs.get("osd"):
            cmd = f"ceph osd deep-scrub {kwargs.get('osd')}"
        else:
            # scrubbing all the OSD's
            cmd = "ceph osd deep-scrub all"
        self.node.shell([cmd])

    def collect_osd_daemon_ids(self, osd_node) -> dict:
        """
        The method is used to collect the various OSD daemons present on a particular node
        :param osd_node: name of the OSD node on which osd daemon details are collected (ceph.ceph.CephNode): ceph node
        :return: list of OSD ID's
        """
        cmd = f"sudo ceph osd ls-tree {osd_node.hostname}"
        return self.run_ceph_command(cmd=cmd)

    def enable_balancer(self, **kwargs) -> bool:
        """
        Enables the balancer module with the given mode
        Args:
            kwargs: Any other args that need to be passed
            Supported kw args :
                1. balancer_mode: There are currently two supported balancer modes (str)
                   -> crush-compat
                   -> upmap (default )
                2. target_max_misplaced_ratio : the percentage of PGs that are allowed to misplaced by balancer (float)
                    target_max_misplaced_ratio = .07
                3. sleep_interval : number of seconds to sleep in between runs (int)
                    sleep_interval = 60
        Returns: True -> pass, False -> fail
        """
        # balancer is always enabled module, There is no need to enable the module via mgr.
        # To verify the same run ` ceph mgr module ls `, which would list all modules.
        # if found to be disabled, can be enabled by ` ceph mgr module enable balancer `
        mgr_modules = self.run_ceph_command(cmd="ceph mgr module ls")
        if not (
            "balancer" in mgr_modules["always_on_modules"]
            or "balancer" in mgr_modules["enabled_modules"]
        ):
            log.error(
                f"Balancer is not enabled. Enabled modules on cluster are:"
                f"{mgr_modules['always_on_modules']} & "
                f"{mgr_modules['enabled_modules']}"
            )

        # Setting the mode for the balancer. Available modes: none|crush-compat|upmap
        balancer_mode = kwargs.get("balancer_mode", "upmap")
        cmd = f"ceph balancer mode {balancer_mode}"
        self.node.shell([cmd])
        # Turning on the balancer on the system
        cmd = "ceph balancer on"
        self.node.shell([cmd])

        if kwargs.get("target_max_misplaced_ratio"):
            cmd = f"ceph config set mgr target_max_misplaced_ratio {kwargs.get('target_max_misplaced_ratio')}"
            self.node.shell([cmd])

        if kwargs.get("sleep_interval"):
            cmd = f"ceph config set mgr mgr/balancer/sleep_interval {kwargs.get('sleep_interval')}"
            self.node.shell([cmd])

        # Sleeping for 10 seconds after enabling balancer and then collecting the evaluation status
        time.sleep(10)
        cmd = "ceph balancer status"
        out = self.run_ceph_command(cmd)
        if not out["active"]:
            log.error("Exception balancer is not active")
            return False
        log.info(f"the balancer status is \n {out}")
        return True

    def configure_pg_autoscaler(self, **kwargs) -> bool:
        """
        Configures pg_Autoscaler as a global global parameter and on pools
        Args:
            **kwargs: Any other param that needs to be set
                1. mon_target_pg_per_osd -> Sets the target number of PG's per OSD
                2. pool_config -> Config to be changed on the given pool (dict)
                    for supported args, look autoscaler_pool_settings() doc
                3. pg_autoscale_value -> Mode of pg auto-scaling to be set, if pool name is provided (str)
                    the allowed values are :
                    1. off -> turns off PG autoscaler on the given pool
                    2. warn -> displays warnings in ceph status, but does not trigger autoscale
                    3. on -> automatically autoscale based on PG count in pool
                4. default_mode -> Default mode to be set for all the newly created pools on the cluster (str)
                    the allowed values are :
                    1. off -> turns off PG autoscaler on the given pool
                    2. warn -> displays warnings in ceph status, but does not trigger autoscale
                    3. on -> automatically autoscale based on PG count in pool
        Returns: True -> pass, False -> fail
        """

        mgr_modules = self.run_ceph_command(cmd="ceph mgr module ls")
        if "pg_autoscaler" not in mgr_modules["enabled_modules"]:
            cmd = "ceph mgr module enable pg_autoscaler"
            self.node.shell([cmd])

        if kwargs.get("pool_config"):
            pool_conf = kwargs.get("pool_config")
            if not self.autoscaler_pool_settings(**pool_conf):
                return False

        if kwargs.get("default_mode"):
            cmd = f"ceph config set global osd_pool_default_pg_autoscale_mode {kwargs.get('default_mode')}"
            self.node.shell([cmd])

        if kwargs.get("mon_target_pg_per_osd"):
            cmd = f"ceph config set global mon_target_pg_per_osd {kwargs['mon_target_pg_per_osd']}"
            self.node.shell([cmd])

        cmd = "ceph osd pool autoscale-status"
        log.info(self.run_ceph_command(cmd))
        return True

    def autoscaler_pool_settings(self, **kwargs):
        """
        Sets various options on pools wrt PG Autoscaler
        Args:
            **kwargs: various kwargs to be sent
                Supported kw args:
                1. pg_autoscale_mode: PG saler mode for the indivudial pool. Values-> on, warn, off. (str)
                2. target_size_ratio: ratio of cluster pool will utilize. Values -> 0 - 1. (float)
                3. target_size_bytes: size the pool is assumed to utilize. eg: 10T (str)
                4. pg_num_min: minimum pg's for a pool. (int)
        Returns:
        """
        pool_name = kwargs["pool_name"]
        value_map = {
            "pg_autoscale_mode": kwargs.get("pg_autoscale_mode"),
            "target_size_ratio": kwargs.get("target_size_ratio"),
            "target_size_bytes": kwargs.get("target_size_bytes"),
            "pg_num_min": kwargs.get("pg_num_min"),
        }
        for val in value_map.keys():
            if val in kwargs.keys():
                if not self.set_pool_property(
                    pool=pool_name, props=val, value=value_map[val]
                ):
                    log.error(f"failed to set property {val} on pool {pool_name}")
                    return False
        return True

    def set_cluster_configuration_checks(self, **kwargs) -> bool:
        """
        Sets up Cephadm to periodically scan each of the hosts in the cluster, and to understand the state of the OS,
         disks, NICs etc
         ref doc : https://docs.ceph.com/en/latest/cephadm/operations/#cluster-configuration-checks
        Args:
            kwargs: Any other param that needs to passed
            The various args that can be sent are :
            1. disable_check_list : list of config checks that need to be disabled. (list)
            2. enable_check_list : list of config checks that need to be Enabled. (list)
            The allowed list of configuration values that can be sent are :
            1. kernel_security : checks SELINUX/Apparmor profiles are consistent across cluster hosts
            2. os_subscription : checks subscription states are consistent for all cluster hosts
            3. public_network : check that all hosts have a NIC on the Ceph public_netork
            4. osd_mtu_size : check that OSD hosts share a common MTU setting
            5. osd_linkspeed : check that OSD hosts share a common linkspeed
            6. network_missing : checks that the cluster/public networks defined exist on the Ceph hosts
            7. ceph_release : check for Ceph version consistency - ceph daemons should be on the same release
            8. kernel_version :  checks that the MAJ.MIN of the kernel on Ceph hosts is consistent
        Returns: True -> pass, False -> fail
        """

        # Checking if the checks are enabled on cluster
        cmd = "ceph cephadm config-check status"
        out, err = self.node.shell([cmd])
        if not re.search("Enabled", out):
            log.info("Cluster config checks not enabled, Proceeding to enable them")
            cmd = "ceph config set mgr mgr/cephadm/config_checks_enabled true"
            self.node.shell([cmd])

        if kwargs.get("disable_check_list"):
            if not self.disable_configuration_checks(kwargs.get("disable_check_list")):
                log.error("failed to disable the given checks")
                return False

        if kwargs.get("enable_check_list"):
            if not self.enable_configuration_checks(kwargs.get("enable_check_list")):
                log.error("failed to enable the given checks")
                return False
        log.info("Completed setting the config checks ")
        return True

    def enable_configuration_checks(self, configs: list) -> bool:
        """
        Enables checks for the configs provided
        Note: Once enabled the module, all the config checks are enabled by default
        Args:
            configs: list of config checks that need to be Enabled. (list)
        Returns: True -> Pass, False -> fail
        """
        for check in configs:
            cmd = f"ceph cephadm config-check enable {check}"
            self.node.shell([cmd])

        cmd = "ceph cephadm config-check ls"
        all_conf_checks = self.run_ceph_command(cmd)

        changed = [entry for entry in all_conf_checks if entry["name"] in configs]
        for check in changed:
            if check["status"] != "enabled":
                return False
        return True

    def disable_configuration_checks(self, configs: list) -> bool:
        """
        disables checks for the configs provided
        Note: Once enabled the module, all the config checks are enabled by default
        Args:
            configs: list of config checks that need to be disabled. (list)
        Returns: True -> Pass, False -> fail
        """
        for check in configs:
            cmd = f"ceph cephadm config-check disable {check}"
            self.node.shell([cmd])

        cmd = "ceph cephadm config-check ls"
        all_conf_checks = self.run_ceph_command(cmd)

        changed = [entry for entry in all_conf_checks if entry["name"] in configs]
        for check in changed:
            if check["status"] == "enabled":
                return False
        return True
