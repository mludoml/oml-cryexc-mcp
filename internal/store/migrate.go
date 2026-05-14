package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var migrationCommentPattern = regexp.MustCompile(`--[^\n]*`)

func (s *Store) RunMigrations(ctx context.Context, dir string) error {
	if err := s.ensureMigrationTable(ctx); err != nil {
		return err
	}

	files := []string{
		"001_init.sql",
		"002_continuous_aggregates.sql",
		"003_monitoring.sql",
		"004_monitoring_hypertables.sql",
		"005_retention.sql",
		"006_orderbook.sql",
	}

	for _, file := range files {
		applied, err := s.isMigrationApplied(ctx, file)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		sqlBytes, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}

		for _, stmt := range splitMigrationStatements(string(sqlBytes)) {
			if _, err := s.pool.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("apply migration %s: %w", file, err)
			}
		}

		if _, err := s.pool.Exec(ctx, `
			INSERT INTO schema_migrations (version, applied_at)
			VALUES ($1, $2)
		`, file, time.Now().UTC()); err != nil {
			return fmt.Errorf("record migration %s: %w", file, err)
		}
	}

	return nil
}

func (s *Store) ensureMigrationTable(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL
		)
	`)
	return err
}

func (s *Store) isMigrationApplied(ctx context.Context, version string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM schema_migrations WHERE version = $1
		)
	`, version).Scan(&exists)
	return exists, err
}

func splitMigrationStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		if migrationCommentPattern.ReplaceAllString(trimmed, "") == "" {
			continue
		}
		statements = append(statements, trimmed)
	}
	return statements
}
