package migrator

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/scylladb/gocqlx/v2/migrate"

	"github.com/scylladb/gocqlx/v2"
	"github.com/umalmyha/scylla-migrator/internal/config"
)

const (
	initSessionNumOfRetries = 5
	initSessionBackoff      = 3 * time.Second
	initSessionBackoffGrow  = 2 * time.Second
)

type Migrator struct {
	cluster *gocql.ClusterConfig
}

func NewMigrator(cluster *gocql.ClusterConfig) *Migrator {
	return &Migrator{cluster: cluster}
}

func (m *Migrator) MigrateFromDir(ctx context.Context, dirPath string, keyspace config.Keyspace) error {
	// unfortunately, we have to open 2 separate connections for keyspace creation and migrations since gocqlx relies on keyspace provided in cluster
	if err := m.initKeyspace(ctx, keyspace); err != nil {
		return err
	}
	return m.runMigrations(ctx, dirPath)
}

func (m *Migrator) initKeyspace(ctx context.Context, keyspace config.Keyspace) error {
	session, err := m.retryOpenSession()
	if err != nil {
		return err
	}
	defer session.Close()

	keyspaceQuery := m.parseKeyspaceQuery(keyspace)
	log.Printf("try to init keyspace with query\n%s\n", keyspaceQuery)

	if err := session.ContextQuery(ctx, keyspaceQuery, nil).Exec(); err != nil {
		return fmt.Errorf("failed to create keyspace - %w", err)
	}

	log.Println("keyspace query has been executed successfully")
	m.cluster.Keyspace = keyspace.Name

	return nil
}

func (m *Migrator) runMigrations(ctx context.Context, dirPath string) error {
	session, err := m.retryOpenSession()
	if err != nil {
		return err
	}
	defer session.Close()

	log.Println("running migrations from files...")

	f := os.DirFS(dirPath)
	if err := migrate.FromFS(ctx, session, f); err != nil {
		return fmt.Errorf("migration failed - %w", err)
	}

	return nil
}

func (m *Migrator) parseKeyspaceQuery(k config.Keyspace) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`{ 'class' : '%s', `, k.Class))

	if k.Class == config.ReplicationClassSimpleStrategy {
		b.WriteString(fmt.Sprintf("'replication_factor': %d", k.SimpleReplicationFactor))
	} else {
		for i, factor := range k.NetworkTopologyReplicationFactor {
			b.WriteString(fmt.Sprintf(`'%s' : %d`, factor.Datacenter, factor.ReplicationFactor))
			if i != len(k.NetworkTopologyReplicationFactor)-1 {
				b.WriteString(", ")
			}
		}
	}

	b.WriteString(" }")
	replication := b.String()

	return fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s AND DURABLE_WRITES = %t`, k.Name, replication, k.DurableWrites)
}

func (m *Migrator) retryOpenSession() (gocqlx.Session, error) {
	backoff := initSessionBackoff
	for i := 0; i < initSessionNumOfRetries; i++ {
		session, err := gocqlx.WrapSession(m.cluster.CreateSession())
		if err == nil {
			return session, nil
		}

		log.Printf("failed to establish session, wait for %s - %v\n", backoff, err)
		<-time.After(backoff)
		backoff += initSessionBackoffGrow
	}
	return gocqlx.Session{}, fmt.Errorf("session hasn't been established, timeout exceeded")
}
