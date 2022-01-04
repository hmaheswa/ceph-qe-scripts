import datetime
import json
import logging
import random
import string
from time import sleep

from ceph.ceph import CommandFailed

log = logging.getLogger(__name__)


class Rbd:
    def __init__(self, **kw):
        self.ceph_args = ""
        self.ceph_nodes = kw.get("ceph_nodes")
        self.config = kw.get("config")
        self.ceph_version = int(self.config.get("rhbuild")[0])
        self.datapool = None
        self.flag = 0
        self.k_m = self.config.get("ec-pool-k-m", False)

        # Identifying Monitor And Client node
        for node in self.ceph_nodes:
            if node.role == "mon":
                self.ceph_mon = node
                continue
            if node.role == "client":
                self.ceph_client = node
                continue

        if self.ceph_version > 2 and self.k_m:
            self.datapool = "rbd_datapool"
            self.ec_profile = "rbd_ec_profile"
            self.set_ec_profile(profile=self.ec_profile)

    def exec_cmd(self, **kw):
        try:
            cmd = kw.get("cmd")
            node = kw.get("node") if kw.get("node") else self.ceph_client
            if self.k_m and "rbd create" in cmd and "--data-pool" not in cmd:
                cmd = cmd + " --data-pool {}".format(self.datapool)

            out, err = node.exec_command(
                sudo=True,
                cmd=cmd,
                long_running=kw.get("long_running", False),
                check_ec=kw.get("check_ec", True),
            )

            if kw.get("output", False):
                return out.read().decode()

            return 0

        except CommandFailed as e:
            log.info(e)
            self.flag = 1
            return 1

    def random_string(self):
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(10)])
        return temp_str

    def create_pool(self, poolname):
        if self.ceph_version > 2 and self.k_m:
            self.create_ecpool(profile=self.ec_profile, poolname=self.datapool)
        self.exec_cmd(cmd="ceph osd pool create {} 64 64".format(poolname))
        if not self.check_pool_exists(pool_name=poolname):
            log.error("Pool not created")
            return False
        if self.ceph_version >= 3:
            self.exec_cmd(cmd="rbd pool init {}".format(poolname))
        return True

    def set_ec_profile(self, profile):
        self.exec_cmd(cmd="ceph osd erasure-code-profile rm {}".format(profile))
        self.exec_cmd(
            cmd="ceph osd erasure-code-profile set {} k={} m={}".format(
                profile, self.k_m[0], self.k_m[2]
            )
        )

    def create_ecpool(self, **kw):
        poolname = kw.get("poolname", self.datapool)
        profile = kw.get("profile", self.ec_profile)
        self.exec_cmd(
            cmd="ceph osd pool create {} 12 12 erasure {}".format(poolname, profile)
        )
        if not self.check_pool_exists(pool_name=poolname):
            log.error("Pool not created")
            return False
        self.exec_cmd(cmd="rbd pool init {}".format(poolname))
        self.exec_cmd(
            cmd="ceph osd pool set {} allow_ec_overwrites true".format(poolname)
        )
        return True

    def check_pool_exists(self, pool_name: str) -> bool:
        """
        recursively checks if the specified pool exists in the cluster
        Args:
            pool_name: Name of the pool to be checked

        Returns:  True -> pass, False -> fail

        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=200)
        while end_time > datetime.datetime.now():
            out = self.exec_cmd(cmd="ceph df -f json", output=True)
            existing_pools = json.loads(out)
            if pool_name not in [ele["name"] for ele in existing_pools["pools"]]:
                log.error(
                    f"Pool:{pool_name} not populated yet\n"
                    f"sleeping for 2 seconds and checking status again"
                )
                sleep(2)
            else:
                log.info(f"pool {pool_name} exists in the cluster")
                return True
        log.info(f"pool {pool_name} does not exist on cluster")
        return False

    def clean_up(self, **kw):
        if kw.get("dir_name"):
            self.exec_cmd(cmd="rm -rf {}".format(kw.get("dir_name")))

        if kw.get("pools"):
            pool_list = kw.get("pools")
            if self.datapool:
                pool_list.append(self.datapool)

            # mon_allow_pool_delete must be True for removing pool
            if self.ceph_version == 5:
                self.exec_cmd(cmd="ceph config set mon mon_allow_pool_delete true")
                self.exec_cmd(cmd="ceph orch restart mon")

                # ToDo: Unable to confirm service restart
                sleep(60)

            for pool in pool_list:
                self.exec_cmd(
                    cmd="ceph osd pool delete {pool} {pool} "
                    "--yes-i-really-really-mean-it".format(pool=pool)
                )