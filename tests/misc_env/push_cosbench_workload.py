"""
This module contains the workflows for creating and pushing workloads to cosbench

Sample test script

    - test:
        abort-on-fail: true
        config:
          haproxy_client:
            - node6
          controllers:
            - node6
        desc: Start COS Bench controller and driver
        module: cosbench.py
        name: deploy cosbench
"""
import math


from json import loads
from typing import Dict, List

from jinja2 import Template

from xml.etree import ElementTree as ET

from ceph.ceph import Ceph, CephNode, CommandFailed
from ceph.utils import get_nodes_by_ids
from utility.log import Log
from utility import utils

LOG = Log(__name__)


fill_workload = """
<?xml version="1.0" encoding="UTF-8" ?>
<workload name="fillCluster-s3" description="RGW testing">
<!-- Initialization -->
  <storage type="s3" config="timeout=900000;accesskey=x;secretkey=y;endpoint=workload_endpoint;path_style_access=true"/>
  <auth type="none"/>
  <workflow>

<!-- Initialization -->
    <workstage name="init_containers">
        <work type="init" workers="400" config="cprefix=pri-bkt;containers=r(1,6)"/>
    </workstage>

    <workstage name="preparing_cluster">
        <work type="prepare" workers="400" config="cprefix=pri-bkt;containers=r(1,6);oprefix=pri-obj;objects=r(1,objects_count);sizes=h(1|5|25,5|50|40,50|256|25,256|512|5,512|1024|3,1024|5120|1,5120|51200|1)KB"/>
    </workstage>
  </workflow>
</workload>"""

avail_storage = 1


def prepare_fill_workload(ceph_cluster, config, fill_percent=30):
    client = ceph_cluster.get_nodes(role="client")[0]
    rgw = ceph_cluster.get_nodes(role="rgw")[0]
    keys = get_or_create_user(client)
    fill_workload.replace("accesskey=x", f"accesskey={keys['access_key']}")
    fill_workload.replace("secretkey=y", f"secretkey={keys['secret_key']}")

    avail_storage = utils.calculate_available_storage(client)
    LOG.info(f"Total available storage: {avail_storage}")
    bytes_to_fill = math.floor(avail_storage / 100 * fill_percent)
    LOG.info(f"no of bytes to fill {fill_percent} percent: {bytes_to_fill}")
    objects_count = bytes_to_fill * 100 / (40456 * 1024)
    LOG.info(f"no of objects for an average of sizes in workload: {objects_count}")
    fill_workload.replace("objects_count", f"{objects_count}")

    workload_endpoint = "http://localhost:5000"
    if config.get("haproxy_clients"):
        haproxy_client = get_nodes_by_ids(
            ceph_cluster, config["haproxy_clients"]
        )[0]
        ip = haproxy_client.ip_address
        workload_endpoint = f"http://{ip}:5000"
    else:
        ip = rgw.ip_address
        stdin, stdout, stderr = rgw.exec_command(sudo=True, cmd="ceph orch ls --format json --service-type rgw")
        rgw_service = json.loads(stdout)
        port = rgw_service[0]["status"]["ports"][0]
        workload_endpoint = f"http://{ip}:{port}"
    fill_workload.replace("workload_endpoint", workload_endpoint)

    LOG.info(fill_workload)
    workload_id = utils.generate_unique_id(length=5)
    workload_file_name = f"fill-workload-{workload_id}.xml"
    wl_xml_tree = ET.XML(fill_workload)
    controller_node = get_nodes_by_ids(ceph_cluster, config["controllers"])[0]
    controller_node.exec_command(cmd=f"echo '{ET.tostring(wl_xml_tree)}' > {workload_file_name}")
    f.write(ET.tostring(wl_xml_tree))
    return workload_file_name


def push_workload(controller_node, workload_file_name):
    controller_node.exec_command(cmd=f"sh /opt/cosbench/cli.sh submit ~/{workload_file_name}")
    updated_avail_storage = utils.calculate_available_storage(client)
    actual_bytes_filled = avail_storage - updated_avail_storage
    fill_percentage = actual_bytes_filled * 100 / avail_storage
    LOG.info(f"Actual percentage of cluster filled by workload: {fill_percentage} %")
    LOG.info(f"Actual number of bytes filled by workload: {actual_bytes_filled}")
    LOG.info(f"Available storage after workload: {updated_avail_storage} %")
    pass


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    preparing and pushing cosbench workload

    Args:
        ceph_cluster:   Cluster participating in the test.

    Returns:
        0 on Success and 1 on Failure.
    """
    LOG.info("preaparing and pushing cosbench workload to fill 30% of the cluster")
    controller = get_nodes_by_ids(ceph_cluster, kwargs["config"]["controllers"])[0]

    workload_file_name = prepare_fill_workload(ceph_cluster, kwargs["config"], 20)
    push_workload(controller, workload_file_name)

    LOG.info("Workload completed successfully!!!")
    return 0
