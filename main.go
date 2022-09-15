package main

import (
	"context"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/umalmyha/scylla-migrator/internal/config"
	"github.com/umalmyha/scylla-migrator/internal/migrator"
)

const (
	clusterMaxWaitSchemaAgreement     = 2 * time.Minute
	clusterRetryPolicyNumOfRetries    = 5
	clusterRetryPolicyMinRetryBackoff = 2 * time.Second
	clusterRetryPolicyMaxRetryBackoff = 10 * time.Second
)

const runMigrationsTimeout = 2 * time.Minute

func main() {
	cfg, err := config.Build()
	if err != nil {
		log.Fatal(err)
	}

	cluster := cluster(cfg.Auth, cfg.Hosts...)

	ctx, cancel := context.WithTimeout(context.Background(), runMigrationsTimeout)
	defer cancel()

	log.Println("running migrations...")

	m := migrator.NewMigrator(cluster)
	if err := m.MigrateFromDir(ctx, "./migrations", cfg.Keyspace); err != nil {
		log.Fatal(err)
	}

	log.Println("migration is successful!")
}

func cluster(auth config.Auth, hosts ...string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.All
	cluster.MaxWaitSchemaAgreement = clusterMaxWaitSchemaAgreement
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: clusterRetryPolicyNumOfRetries,
		Min:        clusterRetryPolicyMinRetryBackoff,
		Max:        clusterRetryPolicyMaxRetryBackoff,
	}

	if auth.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username:              auth.Username,
			Password:              auth.Password,
			AllowedAuthenticators: nil,
		}
	}

	return cluster
}
