package config

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/caarlos0/env/v6"
)

type ReplicationClass string

const (
	ReplicationClassSimpleStrategy          ReplicationClass = "SimpleStrategy"
	ReplicationClassNetworkTopologyStrategy                  = "NetworkTopologyStrategy"
)

type NetworkTopologyReplicationFactor struct {
	Datacenter        string
	ReplicationFactor int
}

type Keyspace struct {
	Name                             string                             `env:"KEYSPACE_NAME,notEmpty"`
	Class                            ReplicationClass                   `env:"KEYSPACE_REPLICATION_CLASS" envDefault:"SimpleStrategy"`
	SimpleReplicationFactor          int                                `env:"KEYSPACE_SIMPLE_REPLICATION_FACTOR" envDefault:"1"`
	NetworkTopologyReplicationFactor []NetworkTopologyReplicationFactor `env:"KEYSPACE_NETWORK_TOPOLOGY_REPLICATION_FACTOR"`
	DurableWrites                    bool                               `env:"KEYSPACE_DURABLE_WRITES" envDefault:"true"`
}

type Auth struct {
	Username string `env:"USERNAME"`
	Password string `env:"PASSWORD"`
}

type MigrationConfig struct {
	Hosts    []string `env:"HOSTS" envSeparator:","`
	Keyspace Keyspace
	Auth     Auth
}

func Build() (MigrationConfig, error) {
	var cfg MigrationConfig

	parsers := map[reflect.Type]env.ParserFunc{
		reflect.TypeOf(ReplicationClassSimpleStrategy):       replicationClassParser,
		reflect.TypeOf([]NetworkTopologyReplicationFactor{}): networkTopologyReplicationFactorParser,
	}

	if err := env.ParseWithFuncs(&cfg, parsers); err != nil {
		return cfg, fmt.Errorf("failed to parse configuration - %w", err)
	}

	return cfg, nil
}

func replicationClassParser(v string) (any, error) {
	class := ReplicationClass(v)
	if class != ReplicationClassSimpleStrategy && class != ReplicationClassNetworkTopologyStrategy {
		return v, fmt.Errorf("replication class must be one of %s, %s", ReplicationClassSimpleStrategy, ReplicationClassNetworkTopologyStrategy)
	}
	return class, nil
}

func networkTopologyReplicationFactorParser(v string) (any, error) {
	factors := make([]NetworkTopologyReplicationFactor, 0)

	replications := strings.Split(v, ",")
	for _, rep := range replications {
		parts := strings.Split(rep, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("%s must have foramt like datacenterName=replicationFactor (e. g. DC1=3)", ReplicationClassNetworkTopologyStrategy)
		}

		replFactor, err := strconv.Atoi(parts[1])
		if err != nil || replFactor <= 0 {
			return nil, errors.New("replication factor must be positive non-zero number")
		}

		factors = append(factors, NetworkTopologyReplicationFactor{
			Datacenter:        parts[0],
			ReplicationFactor: replFactor,
		})
	}

	return factors, nil
}
