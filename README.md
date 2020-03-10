# charm-k8s-kafka

This is the kafka-k8s charm using the [operator framework](https://github.com/canonical/operator).

## Usage

```bash
# Download the kafka from github
git clone https://github.com/charmed-osm/charm-k8s-kafka.git
cd charm-k8s-kafka/

# Install the submodules
git submodule update --init
juju deploy .
```
