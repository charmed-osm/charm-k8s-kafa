#!/usr/bin/env python3

import sys

sys.path.append("lib")

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
    ModelError,
)


class KafkaCharm(CharmBase):
    state = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        # An example of setting charm state
        # that's persistent across events
        self.state.set_default(is_started=False)

        if not self.state.is_started:
            self.state.is_started = True

        # Register all of the events we want to observe
        for event in (
            # Charm events
            self.on.config_changed,
            self.on.start,
            self.on.upgrade_charm,
        ):
            self.framework.observe(event, self)

    def _apply_spec(self, spec):
        # Only apply the spec if this unit is a leader.
        if self.framework.model.unit.is_leader():
            self.framework.model.pod.set_spec(spec)
            self.state.spec = spec

    def make_pod_spec(self):
        config = self.framework.model.config
        spec = {
            "version": 2,
            "containers": [
                {
                    "name": self.framework.model.app.name,
                    "image": config["image"],
                    "ports": [{"name": "kafka", "containerPort": config["port"]},],
                    "config": {
                        "ENABLE_AUTO_EXTEND": "true",
                        "KAFKA_ADVERTISED_HOST_NAME": config["hostname"],
                        "KAFKA_ADVERTISED_PORT": config["port"],
                        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
                        "KAFKA_RESERVED_BROKER_MAX_ID": "999999999",
                    },
                    "kubernetes": {
                        "readinessProbe": {
                            "tcpSocket": {"port": config["port"]},
                            "initialDelaySeconds": 10,
                            "timeoutSeconds": 5,
                            "failureThreshold": 6,
                        },
                        "livenessProbe": {
                            "exec": {
                                "command": [
                                    "sh",
                                    "-c",
                                    "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server=localhost:{}".format(
                                        config["port"]
                                    ),
                                ]
                            },
                            "initialDelaySeconds": 10,
                        },
                    },
                    "command": [
                        "sh",
                        "-c",
                        "exec kafka-server-start.sh /opt/kafka/config/server.properties \
                            --override broker.id=${{HOSTNAME##*-}} \
                            --override listeners=PLAINTEXT://:{} \
                            --override zookeeper.connect=zookeeper-k8s \
                            --override log.dir=/var/lib/kafka \
                            --override auto.create.topics.enable=true \
                            --override auto.leader.rebalance.enable=true".format(
                            config["port"]
                        ),
                    ],
                }
            ],
        }

        return spec

    def on_config_changed(self, event):
        """Handle changes in configuration"""
        unit = self.model.unit

        new_spec = self.make_pod_spec()
        if self.state.spec != new_spec:
            unit.status = MaintenanceStatus("Applying new pod spec")

            self._apply_spec(new_spec)

            unit.status = ActiveStatus()

    def on_start(self, event):
        """Called when the charm is being installed"""
        unit = self.model.unit

        unit.status = MaintenanceStatus("Applying pod spec")

        new_pod_spec = self.make_pod_spec()
        self._apply_spec(new_pod_spec)

        unit.status = ActiveStatus()

    def on_upgrade_charm(self, event):
        """Upgrade the charm."""
        unit = self.model.unit

        # Mark the unit as under Maintenance.
        unit.status = MaintenanceStatus("Upgrading charm")

        self.on_start(event)

        # When maintenance is done, return to an Active state
        unit.status = ActiveStatus()


if __name__ == "__main__":
    main(KafkaCharm)
