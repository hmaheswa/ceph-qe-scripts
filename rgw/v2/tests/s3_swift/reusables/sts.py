import os
import sys

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import json
import logging
import time

import v2.utils.utils as utils
from v2.lib.exceptions import TestExecError
from v2.lib.rgw_config_opts import ConfigOpts
from v2.lib.s3.auth import Auth

log = logging.getLogger()
TEST_DATA_PATH = None


obtain_oidc_thumbprint_sh = """
#!/bin/bash

# Get the 'x5c' from this response to turn into an IDP-cert
KEY1_RESPONSE=$(curl -k -v \
     -X GET \
     -H 'Content-Type: application/x-www-form-urlencoded' \
     'http://localhost:8180/realms/master/protocol/openid-connect/certs' 2>/dev/null \
     | jq -r .keys[0].x5c)

KEY2_RESPONSE=$(curl -k -v \
     -X GET \
     -H 'Content-Type: application/x-www-form-urlencoded' \
     'http://localhost:8180/realms/master/protocol/openid-connect/certs' 2>/dev/null \
     | jq -r .keys[1].x5c)

# Assemble Cert1
echo '-----BEGIN CERTIFICATE-----' > certificate1.crt
echo $(echo $KEY1_RESPONSE) | sed 's/^.//;s/.$//;s/^.//;s/.$//;s/^.//;s/.$//' >> certificate1.crt
echo '-----END CERTIFICATE-----' >> certificate1.crt

# Assemble Cert2
echo '-----BEGIN CERTIFICATE-----' > certificate2.crt
echo $(echo $KEY2_RESPONSE) | sed 's/^.//;s/.$//;s/^.//;s/.$//;s/^.//;s/.$//' >> certificate2.crt
echo '-----END CERTIFICATE-----' >> certificate2.crt

# Create Thumbprint for both certs
PRETHUMBPRINT1=$(openssl x509 -in certificate1.crt -fingerprint -noout)
PRETHUMBPRINT2=$(openssl x509 -in certificate2.crt -fingerprint -noout)

PRETHUMBPRINT1=$(echo $PRETHUMBPRINT1 | awk '{ print substr($0, 18) }')
PRETHUMBPRINT2=$(echo $PRETHUMBPRINT2 | awk '{ print substr($0, 18) }')

echo ${PRETHUMBPRINT1//:}
echo ${PRETHUMBPRINT2//:}

#clean up the temp files
rm certificate1.crt
rm certificate2.crt
"""


def add_sts_config_to_ceph_conf(
    ceph_config_set, rgw_service, sesison_encryption_token="abcdefghijklmnoq"
):
    """adding sts config to ceph conf
       this should be done initialay to have sts feature tested

    Args:
        ceph_config_set (object): ceph config class object
        rgw_service (object): rgw service object
        sesison_encryption_token (str, optional): Defaults to "abcdefghijklmnoq".

    Raises:
        TestExecError: if restart fails
    """
    log.info("adding sts config to ceph.conf")
    ceph_config_set.set_to_ceph_conf(
        "global", ConfigOpts.rgw_sts_key, sesison_encryption_token
    )
    ceph_config_set.set_to_ceph_conf("global", ConfigOpts.rgw_s3_auth_use_sts, "True")
    srv_restarted = rgw_service.restart()
    time.sleep(30)
    if srv_restarted is False:
        raise TestExecError("RGW service restart failed")
    else:
        log.info("RGW service restarted")


def add_caps(user_info, caps="roles=*"):
    """for RGW STS, we need to enable caps on user_1

    Args:
        user_info (dict): user info dict
        caps (str, optional): Defaults to "roles=*".
    """
    log.info("adding caps to user info")
    add_caps_cmd = 'sudo radosgw-admin caps add --uid="{user_id}" --caps={caps}'.format(
        user_id=user_info["user_id"], caps=caps
    )
    utils.exec_shell_cmd(add_caps_cmd)


def create_role(iam_client, policy_document, role_name):
    """create role

    Args:
        iam_client (auth): auth object using from iam
        policy_document (string): policy document string
        role_name (string): role to be used in the document

    Returns:
        http role_response
    """
    log.info("creating role")
    role_response = iam_client.create_role(
        AssumeRolePolicyDocument=policy_document,
        Path="/",
        RoleName=role_name,
    )
    log.info(f"role_response\n:{role_response}")
    return role_response


def put_role_policy(iam_client, role_name, policy_name, role_policy):
    """put policy to the role

    Args:
        iam_client (auth): iam auth object
        role_name (sting): role name created using create_role
        policy_name (string): policy name
        role_policy (string): a dict like string, role policy document

    Returns:
        put policy http response
    """
    log.info("putting role policy")
    put_policy_response = iam_client.put_role_policy(
        RoleName=role_name, PolicyName=policy_name, PolicyDocument=role_policy
    )

    log.info(f"put_policy\n:{put_policy_response}")
    return put_policy_response


def assume_role(sts_client, **kwargs):
    """assuming role

    Args:
        sts_client (auth): sts client auth
        kwargs (dict): assume role params

    Returns:
         assume role http response
    """
    log.info("assuming role")
    assume_role_response = sts_client.assume_role(**kwargs)
    log.info(f"assume_role_response:\n{assume_role_response}")
    return assume_role_response


class Keycloak:
    def __init__(
        self,
        client_id="sts_client",
        client_secret="client_secret1",
        ip_addr="localhost",
        attributes=None,
    ):
        """
        Constructor for curl class
        user_info(dict) : user details
        ssh_con(str) : rgw ip address
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.ip_addr = ip_addr
        self.install_keycloak()
        out = utils.exec_shell_cmd("sudo yum install -y jq")
        if out is False:
            raise Exception("jq installation failed")
        self.create_client()
        self.add_service_account_roles_to_client(client_name=self.client_id)
        if attributes:
            self.add_keycloak_user_attributes(attributes=attributes, username="admin")

    # todo: add --show-error and --fail flags to curl commands
    def install_keycloak(self):
        out = utils.exec_shell_cmd("sudo podman ps")
        if "keycloak" in out:
            log.info("Keycloak is already running. skipping deployment..")
            return True
        out = utils.exec_shell_cmd(
            "sudo podman run -d --name keycloak -p 8180:8180 -e KEYCLOAK_ADMIN=admin -e KEYCLOAK_ADMIN_PASSWORD=admin quay.io/keycloak/keycloak:22.0.0-0 start-dev --http-port 8180"
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        log.info("sleeping for 15 seconds")
        time.sleep(15)
        return out

    def get_keycloak_web_acccess_token(self, initial_token=False):
        if initial_token:
            out = utils.exec_shell_cmd(
                f'curl --data "username=admin&password=admin&grant_type=password&client_id=admin-cli" http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/token | jq -r .access_token'
            )
        else:
            out = utils.exec_shell_cmd(
                f'curl -k -v -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "scope=openid" -d "grant_type=client_credentials" -d "client_id={self.client_id}" -d "client_secret={self.client_secret}" "http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/token" | jq -r .access_token'
            )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out.strip()

    def introspect_token(self, access_token):
        out = utils.exec_shell_cmd(
            f'curl -d "token={access_token}" -u "{self.client_id}:{self.client_secret}" http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/token/introspect | jq .'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        log.info(out)
        json_out = json.loads(out)
        return json_out

    def create_client(self, client_representation=None):
        default_client_representation = {
            "clientId": "sts_client",
            "enabled": "true",
            "consentRequired": "false",
            "protocol": "openid-connect",
            "standardFlowEnabled": "true",
            "implicitFlowEnabled": "false",
            "directAccessGrantsEnabled": "true",
            "publicClient": "false",
            "secret": "client_secret1",
            "serviceAccountsEnabled": "true",
        }
        if client_representation:
            default_client_representation.update(client_representation)
        access_token = self.get_keycloak_web_acccess_token(initial_token=True)
        out = utils.exec_shell_cmd(
            f'curl -X POST -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients  -d \'{json.dumps(default_client_representation)}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def get_keycloak_roles(self):
        access_token = self.get_keycloak_web_acccess_token(initial_token=True)
        out = utils.exec_shell_cmd(
            f'curl -X GET -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/roles | jq .'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        json_out = json.loads(out)
        return json_out

    def get_service_account_user_id(self, client_name):
        access_token = self.get_keycloak_web_acccess_token(initial_token=True)
        out = utils.exec_shell_cmd(
            f'curl -X GET -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/?username=service-account-{client_name}'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        json_out = json.loads(out)
        return json_out

    def add_service_account_roles_to_client(self, client_name):
        service_account_details = self.get_service_account_user_id(client_name)
        service_account_user_id = service_account_details[0]["id"]

        roles = self.get_keycloak_roles()
        # for role in roles:
        #     access_token = self.get_keycloak_web_acccess_token(initial_token=True)
        #     out = utils.exec_shell_cmd(
        #         f'curl -X POST -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/{service_account_user_id}/role-mappings/realm --data-raw \'{json.dumps(role)}\''
        #     )
        #     if out is False:
        #         raise Exception("keycloack deployment failed")
        access_token = self.get_keycloak_web_acccess_token(initial_token=True)
        out = utils.exec_shell_cmd(
            f'curl -X POST -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/{service_account_user_id}/role-mappings/realm --data-raw \'{json.dumps(roles)}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return True

    def get_keycloack_openid_configuration(self):
        out = utils.exec_shell_cmd(
            f"curl http://{self.ip_addr}:8180/realms/master/.well-known/openid-configuration | jq -r .jwks_uri"
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def get_keycloack_certs(self):
        out = utils.exec_shell_cmd(
            f"curl http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/certs | jq ."
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def get_keycloak_user(self, username="admin"):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/?username={username}'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def add_keycloak_user_attributes(self, attributes, username="admin"):
        access_token = self.get_keycloak_web_acccess_token()
        admin_user = self.get_keycloak_user(username)[0]
        user_id = admin_user["id"]
        existing_attributes = admin_user["attributes"]
        existing_attributes.update(attributes)
        out = utils.exec_shell_cmd(
            f'curl -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/{user_id} -d \'{{"attributes":{json.dumps(existing_attributes)}}}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def enable_client_authentication(self, client_name="account"):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients/65e70a29-629f-4c53-9c56-4e8b26fe9f1c -d \'{{"publicClient":false, "secret": "client_secret1"}}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def disable_client_authentication(self, client_name="account"):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients/65e70a29-629f-4c53-9c56-4e8b26fe9f1c -d \'{{"publicClient":true}}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def enable_client_direct_access_grants(self, client_name="account"):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients/65e70a29-629f-4c53-9c56-4e8b26fe9f1c -d \'{{"directAccessGrantsEnabled":true}}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def set_audience_in_token(self, client_name="account"):
        client_scope_representation = {
            "attributes": {
                "display.on.consent.screen": "true",
                "include.in.token.scope": "true",
            },
            "name": "audience_scope1",
            "description": "scope to set audience in token",
            "protocol": "openid-connect",
        }
        self.create_client_scope(client_scope_representation)
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients/65e70a29-629f-4c53-9c56-4e8b26fe9f1c -d \'{{"directAccessGrantsEnabled":true}}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def create_client_scope(self, client_scope_representation):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -X POST -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/client-scopes -d \'{client_scope_representation}\''
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def set_session_tags_in_token(self, client_name):
        client_scope_representation = {
            "attributes": {
                "display.on.consent.screen": "true",
                "include.in.token.scope": "true",
            },
            "name": "session_tags_scope",
            "description": "scope to set session tags in token",
            "protocol": "openid-connect",
        }
        self.create_client_scope(client_scope_representation)
        client_scope_id = self.get_client_scope("session_tags_scope")["id"]
        protocol_mapper_representation = {
            "protocol": "openid-connect",
            "protocolMapper": "oidc-usermodel-attribute-mapper",
            "name": "https://aws.amazon.com/tags",
            "config": {
                "user.attribute": "https://aws.amazon.com/tags",
                "claim.name": "https://aws\\.amazon\\.com/tags",
                "jsonType.label": "JSON",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "multivalued": "true",
                "aggregate.attrs": "true",
            },
        }
        self.create_protocol_mapper(client_scope_id, protocol_mapper_representation)
        client_id = self.get_keycloak_client(client_name)
        self.add_client_scope_to_client(client_id, client_scope_id)
        return True

    def get_client_scope(self, client_scope_name=None):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/client-scopes/'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        if client_scope_name:
            client_scope_json = json.loads(out)
            for scope in client_scope_json:
                if scope["name"] == client_scope_name:
                    return scope
            raise Exception(f"client scope with name '{client_scope_name}' not found")
        return out

    def create_protocol_mapper(self, client_scope_id, protocol_mapper_representation):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -X POST -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://10.0.207.21:8180/admin/realms/master/client-scopes/{client_scope_id}/protocol-mappers/model -d "{protocol_mapper_representation}"'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def get_keycloak_client(self, client_name=None):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        if client_name:
            clients_json = json.loads(out)
            for client in clients_json:
                if client["name"] == client_name:
                    return client
            raise Exception(f"client scope with name '{client_name}' not found")
        return out

    def add_client_scope_to_client(self, client_id, client_scope_id):
        access_token = self.get_keycloak_web_acccess_token()
        out = utils.exec_shell_cmd(
            f'curl -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {access_token}" http://10.0.207.21:8180/admin/realms/master/clients/{client_id}/default-client-scopes/{client_scope_id}'
        )
        if out is False:
            raise Exception("keycloack deployment failed")
        return out

    def create_open_id_connect_provider(self, iam_client):
        # obtain oidc idp thumbprint
        global obtain_oidc_thumbprint_sh
        # utils.exec_shell_cmd(
        #     f'echo "{obtain_oidc_thumbprint_sh}" > obtain_oidc_thumbprint.sh'
        # )
        with open("obtain_oidc_thumbprint.sh", "w") as rsh:
            rsh.write(f"{obtain_oidc_thumbprint_sh}")
        utils.exec_shell_cmd("chmod +rwx obtain_oidc_thumbprint.sh")
        thumbprints = utils.exec_shell_cmd(f"./obtain_oidc_thumbprint.sh")
        thumbprints = thumbprints.strip().split("\n")
        try:
            # create openid connect provider
            oidc_response = iam_client.create_open_id_connect_provider(
                Url=f"http://{self.ip_addr}:8180/realms/master",
                ClientIDList=[self.client_id],
                ThumbprintList=thumbprints,
            )
            log.info(f"create oidc response: {oidc_response}")
        except Exception as e:
            log.info(f"Exception {e} occured")
            log.info("Provider already exists")
        # out = utils.exec_shell_cmd(
        #     "curl http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/certs | jq ."
        # )
        # if out is False:
        #     raise Exception("keycloack deployment failed")
        return True

    def list_open_id_connect_provider(self, iam_client):
        # list openid connect providers
        try:
            oidc_response = iam_client.list_open_id_connect_providers()
            log.info(f"list oidc response: {oidc_response}")
            return oidc_response
        except Exception as e:
            log.info("No openid connect providers")

    def delete_open_id_connect_provider(self, iam_client):
        # delete openid connect provider
        # oidc_response = iam_client.delete_open_id_connect_provider(
        #     OpenIDConnectProviderArn="arn:aws:iam:::oidc-provider/localhost"
        # )
        # log.info(f"oidc response: {oidc_response}")
        json_out = self.list_open_id_connect_provider(iam_client)
        if json_out:
            for provider in json_out["OpenIDConnectProviderList"]:
                arn = provider["Arn"]
                oidc_response = iam_client.delete_open_id_connect_provider(
                    OpenIDConnectProviderArn=arn
                )
                log.info(f"delete oidc response: {oidc_response}")
                time.sleep(5)
