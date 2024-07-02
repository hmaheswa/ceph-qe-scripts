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


# obtain_oidc_thumbprint_sh = """
# #!/bin/bash
#
# # Get the 'x5c' from this response to turn into an IDP-cert
# KEY1_RESPONSE=$(curl --show-error --fail -k -v \
#      -X GET \
#      -H 'Content-Type: application/x-www-form-urlencoded' \
#      'http://localhost:8180/realms/master/protocol/openid-connect/certs' 2>/dev/null \
#      | jq -r .keys[0].x5c)
#
# KEY2_RESPONSE=$(curl --show-error --fail -k -v \
#      -X GET \
#      -H 'Content-Type: application/x-www-form-urlencoded' \
#      'http://localhost:8180/realms/master/protocol/openid-connect/certs' 2>/dev/null \
#      | jq -r .keys[1].x5c)
#
# # Assemble Cert1
# echo '-----BEGIN CERTIFICATE-----' > certificate1.crt
# echo $(echo $KEY1_RESPONSE) | sed 's/^.//;s/.$//;s/^.//;s/.$//;s/^.//;s/.$//' >> certificate1.crt
# echo '-----END CERTIFICATE-----' >> certificate1.crt
#
# # Assemble Cert2
# echo '-----BEGIN CERTIFICATE-----' > certificate2.crt
# echo $(echo $KEY2_RESPONSE) | sed 's/^.//;s/.$//;s/^.//;s/.$//;s/^.//;s/.$//' >> certificate2.crt
# echo '-----END CERTIFICATE-----' >> certificate2.crt
#
# # Create Thumbprint for both certs
# PRETHUMBPRINT1=$(openssl x509 -in certificate1.crt -fingerprint -noout)
# PRETHUMBPRINT2=$(openssl x509 -in certificate2.crt -fingerprint -noout)
#
# PRETHUMBPRINT1=$(echo $PRETHUMBPRINT1 | awk '{ print substr($0, 18) }')
# PRETHUMBPRINT2=$(echo $PRETHUMBPRINT2 | awk '{ print substr($0, 18) }')
#
# echo ${PRETHUMBPRINT1//:}
# echo ${PRETHUMBPRINT2//:}
#
# #clean up the temp files
# rm certificate1.crt
# rm certificate2.crt
# """


obtain_oidc_thumbprint_sh = """
#!/bin/bash

# Get the 'x5c' from this response to turn into an IDP-cert
KEY1_RESPONSE=$(curl --show-error --fail -k -v \
     -X GET \
     -H 'Content-Type: application/x-www-form-urlencoded' \
     'https://cephlabs.verify.ibm.com/v1.0/endpoint/default/jwks' 2>/dev/null \
     | jq -r .keys[0].x5c)

# Assemble Cert1
echo '-----BEGIN CERTIFICATE-----' > certificate1.crt
echo $(echo $KEY1_RESPONSE) | sed 's/^.//;s/.$//;s/^.//;s/.$//;s/^.//;s/.$//' >> certificate1.crt
echo '-----END CERTIFICATE-----' >> certificate1.crt


# Create Thumbprint for both certs
PRETHUMBPRINT1=$(openssl x509 -in certificate1.crt -fingerprint -noout)

PRETHUMBPRINT1=$(echo $PRETHUMBPRINT1 | awk '{ print substr($0, 18) }')

echo ${PRETHUMBPRINT1//:}

#clean up the temp files
rm certificate1.crt
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
        Keycloak deployment and administration through curl
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.ip_addr = ip_addr
        out = utils.exec_shell_cmd("sudo podman ps")
        if "keycloak" in out:
            log.info("Keycloak is already running. skipping deployment..")
            self.access_token = self.get_web_access_token()
        else:
            self.install_keycloak()
            out = utils.exec_shell_cmd("sudo yum install -y jq")
            if out is False:
                raise Exception("jq installation failed")
            self.access_token = self.get_initial_web_access_token()
            log.info(self.access_token)
            self.update_realm_token_lifespan()
            self.access_token = self.get_initial_web_access_token()
            set_audience_scope_name = "set_audience_scope"
            set_audience_protocol_mapper_name = "set_audience_protocol_mapper"
            self.set_audience_in_token(
                "account", set_audience_scope_name, set_audience_protocol_mapper_name
            )
            self.create_client()
            self.add_service_account_roles_to_client(client_name=self.client_id)
            self.access_token = self.get_web_access_token()
            self.add_client_scope_to_client(
                client_name=self.client_id, client_scope_name=set_audience_scope_name
            )
            self.set_session_tags_in_token(self.client_id)
            self.realm_keys_workaround()
        if attributes is None:
            self.remove_user_attributes(username=f"service-account-{self.client_id}")
        else:
            self.set_user_attributes(
                attributes=attributes, username=f"service-account-{self.client_id}"
            )

    def install_keycloak(self):
        out = utils.exec_shell_cmd(
            "sudo podman run -d --name keycloak -p 8180:8180 -e KEYCLOAK_ADMIN=admin -e KEYCLOAK_ADMIN_PASSWORD=admin quay.io/keycloak/keycloak:22.0.0 start-dev --http-port 8180"
        )
        if out is False:
            raise Exception("keycloak deployment failed")
        log.info("sleeping for 60 seconds")
        time.sleep(60)
        return out

    def get_web_access_token(self):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "scope=openid" -d "grant_type=client_credentials" -d "client_id={self.client_id}" -d "client_secret={self.client_secret}" "http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/token" | jq -r .access_token'
        )
        if out is False:
            raise Exception("failed to get access token")
        return out.strip()

    def get_initial_web_access_token(self):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail --data "username=admin&password=admin&grant_type=password&client_id=admin-cli" http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/token | jq -r .access_token'
        )
        if out is False:
            raise Exception("failed to get access token")
        log.info(out)
        return out.strip()

    def introspect_token(self, access_token):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -d "token={self.access_token}" -u "{self.client_id}:{self.client_secret}" http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/token/introspect | jq .'
        )
        if out is False:
            raise Exception("token introspection failed")
        log.info(out)
        json_out = json.loads(out)
        return json_out

    def create_client(self, client_representation=None):
        default_client_representation = {
            "clientId": self.client_id,
            "enabled": "true",
            "consentRequired": "false",
            "protocol": "openid-connect",
            "standardFlowEnabled": "true",
            "implicitFlowEnabled": "false",
            "directAccessGrantsEnabled": "true",
            "publicClient": "false",
            "secret": self.client_secret,
            "serviceAccountsEnabled": "true",
        }
        if client_representation:
            default_client_representation.update(client_representation)
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X POST -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients  -d \'{json.dumps(default_client_representation)}\''
        )
        if out is False:
            raise Exception("client creation failed")
        return out

    def get_roles(self):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X GET -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/roles | jq .'
        )
        if out is False:
            raise Exception("failed to get realm roles")
        json_out = json.loads(out)
        return json_out

    def get_service_account_user_id(self, client_name):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X GET -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/?username=service-account-{client_name}'
        )
        if out is False:
            raise Exception("failed to get service account user")
        json_out = json.loads(out)
        return json_out

    def add_service_account_roles_to_client(self, client_name):
        service_account_details = self.get_service_account_user_id(client_name)
        service_account_user_id = service_account_details[0]["id"]
        roles = self.get_roles()
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X POST -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/{service_account_user_id}/role-mappings/realm --data-raw \'{json.dumps(roles)}\''
        )
        if out is False:
            raise Exception("failed to add service account roles to client")
        return True

    def get_openid_configuration(self):
        out = utils.exec_shell_cmd(
            f"curl --show-error --fail http://{self.ip_addr}:8180/realms/master/.well-known/openid-configuration | jq -r .jwks_uri"
        )
        if out is False:
            raise Exception("failed to get openid configuration")
        return out

    def get_certs(self):
        out = utils.exec_shell_cmd(
            f"curl --show-error --fail http://{self.ip_addr}:8180/realms/master/protocol/openid-connect/certs | jq ."
        )
        if out is False:
            raise Exception("failed to get keycloak certs")
        return out

    def get_user(self, username="admin"):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/?username={username}'
        )
        if out is False:
            raise Exception("failed to get users")
        out = json.loads(out)
        if len(out) == 0:
            raise Exception("No user found with the given name")
        return out

    def set_user_attributes(self, attributes, username):
        user = self.get_user(username)[0]
        user_id = user["id"]
        attributes = json.dumps(attributes)
        attributes = json.dumps(attributes)
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/{user_id} -d \'{{"id": "{user_id}","username":"{username}","attributes": {{"https://aws.amazon.com/tags":{attributes}}}}}\''
        )
        if out is False:
            raise Exception("failed to add attributes to user")
        return out

    def remove_user_attributes(self, username):
        user = self.get_user(username)[0]
        user_id = user["id"]
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/users/{user_id} -d \'{{"id": "{user_id}","username":"{username}","attributes": {{}}}}\''
        )
        if out is False:
            raise Exception("failed to add attributes to user")
        return out

    def set_audience_in_token(
        self, client_name, client_scope_name, protocol_mapper_name
    ):
        client_scope_representation = {
            "attributes": {
                "display.on.consent.screen": "true",
                "include.in.token.scope": "true",
            },
            "name": client_scope_name,
            "description": "scope to set audience in token",
            "protocol": "openid-connect",
            "type": "default",
        }
        self.create_client_scope(client_scope_representation)
        client_scope_id = self.get_client_scope(client_scope_name)["id"]
        protocol_mapper_representation = {
            "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper",
            "name": protocol_mapper_name,
            "config": {
                "included.client.audience": client_name,
                "included.custom.audience": "",
                "id.token.claim": "true",
                "access.token.claim": "true",
            },
        }
        self.create_protocol_mapper(client_scope_id, protocol_mapper_representation)
        self.add_client_scope_to_client(client_name, client_scope_name)
        return True

    def set_session_tags_in_token(self, client_name):
        client_scope_representation = {
            "attributes": {
                "display.on.consent.screen": "true",
                "include.in.token.scope": "true",
            },
            "name": "session_tags_scope",
            "description": "scope to set session tags in token",
            "protocol": "openid-connect",
            "type": "default",
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
                "aggregate.attrs": "false",
            },
        }
        self.create_protocol_mapper(client_scope_id, protocol_mapper_representation)
        self.add_client_scope_to_client(client_name, "session_tags_scope")
        return True

    def create_client_scope(self, client_scope_representation):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X POST -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/client-scopes -d \'{json.dumps(client_scope_representation)}\''
        )
        if out is False:
            raise Exception("client scope creation failed")
        return out

    def get_client_scope(self, client_scope_name=None):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/client-scopes/'
        )
        if out is False:
            raise Exception("failed to get client scopes")
        client_scope_json = json.loads(out)
        if client_scope_name:
            for scope in client_scope_json:
                if scope["name"] == client_scope_name:
                    return scope
            raise Exception(f"client scope with name '{client_scope_name}' not found")
        return client_scope_json

    def create_protocol_mapper(self, client_scope_id, protocol_mapper_representation):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X POST -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/client-scopes/{client_scope_id}/protocol-mappers/models -d \'{json.dumps(protocol_mapper_representation)}\''
        )
        if out is False:
            raise Exception("failed to create protocol mapper for client scope")
        return out

    def get_client(self, client_name=None):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients'
        )
        if out is False:
            raise Exception("failed to get clients")
        clients_json = json.loads(out)
        if client_name:
            for client in clients_json:
                if client["clientId"] == client_name:
                    return client
            raise Exception(f"client scope with name '{client_name}' not found")
        return clients_json

    def add_client_scope_to_client(self, client_name, client_scope_name):
        client_scope_id = self.get_client_scope(client_scope_name)["id"]
        client_id = self.get_client(client_name)["id"]
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/clients/{client_id}/default-client-scopes/{client_scope_id}'
        )
        if out is False:
            raise Exception("failed to add client scope to client")
        return out

    def realm_keys_workaround(self):
        """
        This is a workaround to update rsa-enc-generated realm key with priority 90 and keysize 1024
        to avoid unnecessary failures like using wrong certificate i.e., enc certificate instead of sig certificate
            that leads to "invalid  padding", "wrong signature length", "signature length long"..
        refer https://tracker.ceph.com/issues/54562
        """
        # updating rsa-enc-generated realm key keysize
        config_overrides = {"keySize": ["1024"], "priority": ["90"]}
        rsa_enc_generated_key_metadata_representation = self.get_realm_key(
            key_name="rsa-enc-generated"
        )
        rsa_enc_generated_key_id = rsa_enc_generated_key_metadata_representation["id"]
        rsa_enc_generated_key_metadata_representation["config"].update(config_overrides)
        self.update_realm_key(
            rsa_enc_generated_key_id, rsa_enc_generated_key_metadata_representation
        )
        return True

    def get_realm_key(self, key_name=None):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/components?type=org.keycloak.keys.KeyProvider'
        )
        if out is False:
            raise Exception("failed to get realm keys")
        keys_json = json.loads(out)
        if key_name:
            for key in keys_json:
                if key["name"] == key_name:
                    return key
            raise Exception(f"realm key with name '{key_name}' not found")
        return keys_json

    def get_realm_key_by_id(self, key_id):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/components/{key_id}'
        )
        if out is False:
            raise Exception("failed to get realm key by id")
        keys_json = json.loads(out)
        return keys_json

    def update_realm_key(self, key_id, key_metadata_representation):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master/components/{key_id} -d \'{json.dumps(key_metadata_representation)}\''
        )
        if out is False:
            raise Exception("failed to update realm key")
        return out

    def update_realm_token_lifespan(self):
        realm_representation = {
            "accessTokenLifespan": 3600,
            "accessTokenLifespanForImplicitFlow": 3600,
        }
        self.update_realm(realm_representation)
        return True

    def update_realm(self, realm_representation):
        out = utils.exec_shell_cmd(
            f'curl --show-error --fail -X PUT -H "Content-Type: application/json" -H "Authorization: bearer {self.access_token}" http://{self.ip_addr}:8180/admin/realms/master -d \'{json.dumps(realm_representation)}\''
        )
        if out is False:
            raise Exception("failed to update realm")
        return out


def create_open_id_connect_provider(iam_client):
    # obtain oidc idp thumbprint
    global obtain_oidc_thumbprint_sh
    with open("obtain_oidc_thumbprint.sh", "w") as rsh:
        rsh.write(f"{obtain_oidc_thumbprint_sh}")
    utils.exec_shell_cmd("chmod +rwx obtain_oidc_thumbprint.sh")
    thumbprints = utils.exec_shell_cmd("./obtain_oidc_thumbprint.sh")
    thumbprints = thumbprints.strip().split("\n")
    log.info(thumbprints)
    try:
        # create openid connect provider
        oidc_response = iam_client.create_open_id_connect_provider(
            Url=f"https://cephlabs.verify.ibm.com/v1.0/endpoint/default",
            ClientIDList=["ceph"],
            ThumbprintList=thumbprints,
        )
        log.info(f"create oidc response: {oidc_response}")
    except Exception as e:
        log.info(f"Exception {e} occured")
        log.info("Provider already exists")
    return True

def list_open_id_connect_provider(iam_client):
    # list openid connect providers
    try:
        oidc_response = iam_client.list_open_id_connect_providers()
        log.info(f"list oidc response: {oidc_response}")
        return oidc_response
    except Exception as e:
        log.info("No openid connect providers exists")

def delete_open_id_connect_provider(iam_client):
    json_out = list_open_id_connect_provider(iam_client)
    if json_out:
        for provider in json_out["OpenIDConnectProviderList"]:
            arn = provider["Arn"]
            oidc_response = iam_client.delete_open_id_connect_provider(
                OpenIDConnectProviderArn=arn
            )
            log.info(f"delete oidc response: {oidc_response}")
            time.sleep(5)
    else:
        log.info("No openid connect providers exists to delete")
