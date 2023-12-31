package backup

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestBackuperDefault(t *testing.T) {
	t.Parallel()

	dir := backupDir(t)
	backuper, err := NewBackuper(createControlDatabase(t).Path(), dir)
	require.NoError(t, err)
	require.Equal(t, false, backuper.config.Vacuum)
	require.Equal(t, false, backuper.config.Pruning)
	require.Equal(t, false, backuper.config.Compression)

	// substitutes fileCreator to a mocked version
	backuper.fileCreator = func(dir string, _ time.Time) (string, error) {
		timestamp := time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)
		return createBackupFile(dir, timestamp)
	}

	result, err := backuper.Backup(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(311296), result.Size)
	require.Equal(t, int64(0), result.SizeAfterVacuum)
	require.Equal(t, time.Duration(0), result.VacuumElapsedTime)
	require.Equal(t, fmt.Sprintf("%s/tbl_backup_2009-11-17T20:34:58Z.db", dir), result.Path)
	require.FileExists(t, fmt.Sprintf("%s/tbl_backup_2009-11-17T20:34:58Z.db", dir))
	require.Greater(t, result.ElapsedTime, time.Duration(0))

	require.NoError(t, backuper.Close())
}

func TestBackuperWithVacuum(t *testing.T) {
	t.Parallel()

	dir := backupDir(t)
	backuper, err := NewBackuper(createControlDatabase(t).Path(), dir, []Option{WithVacuum(true)}...)
	require.NoError(t, err)
	require.Equal(t, true, backuper.config.Vacuum)
	require.Equal(t, false, backuper.config.Pruning)
	require.Equal(t, false, backuper.config.Compression)

	// substitutes fileCreator the to a mocked version
	backuper.fileCreator = func(dir string, _ time.Time) (string, error) {
		timestamp := time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)
		return createBackupFile(dir, timestamp)
	}

	result, err := backuper.Backup(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(311296), result.Size)
	require.Equal(t, int64(159744), result.SizeAfterVacuum)
	require.Greater(t, result.VacuumElapsedTime, time.Duration(0))
	require.Equal(t, fmt.Sprintf("%s/tbl_backup_2009-11-17T20:34:58Z.db", dir), result.Path)
	require.FileExists(t, fmt.Sprintf("%s/tbl_backup_2009-11-17T20:34:58Z.db", dir))
	require.Greater(t, result.ElapsedTime, time.Duration(0))

	require.NoError(t, backuper.Close())
}

func TestBackuperWithCompression(t *testing.T) {
	t.Parallel()

	dir := backupDir(t)
	backuper, err := NewBackuper(createControlDatabase(t).Path(), dir, []Option{
		WithVacuum(true),
		WithCompression(true),
	}...,
	)
	require.NoError(t, err)
	require.Equal(t, true, backuper.config.Vacuum)
	require.Equal(t, false, backuper.config.Pruning)
	require.Equal(t, true, backuper.config.Compression)

	// substitutes fileCreator the to a mocked version
	backuper.fileCreator = func(dir string, _ time.Time) (string, error) {
		timestamp := time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)
		return createBackupFile(dir, timestamp)
	}

	result, err := backuper.Backup(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(311296), result.Size)
	require.Equal(t, int64(159744), result.SizeAfterVacuum)
	require.Greater(t, result.VacuumElapsedTime, time.Duration(0))
	require.Equal(t, fmt.Sprintf("%s/tbl_backup_2009-11-17T20:34:58Z.db.zst", dir), result.Path)
	require.FileExists(t, fmt.Sprintf("%s/tbl_backup_2009-11-17T20:34:58Z.db.zst", dir))
	require.NoFileExists(t, fmt.Sprintf("%s/tbl_backup_2009-11-17T20:34:58Z.db", dir))
	require.Greater(t, result.ElapsedTime, time.Duration(0))
	require.Greater(t, result.CompressionElapsedTime, time.Duration(0))
	require.Equal(t, int64(39934), result.SizeAfterCompression)

	require.NoError(t, backuper.Close())
}

func TestBackuperWithPruning(t *testing.T) {
	t.Parallel()

	db, dir := createControlDatabase(t), backupDir(t)

	backuper, err := NewBackuper(db.Path(), dir, []Option{
		WithVacuum(true),
		WithPruning(true, 1),
		WithCompression(true),
	}...,
	)
	require.NoError(t, err)
	require.Equal(t, true, backuper.config.Vacuum)
	require.Equal(t, true, backuper.config.Pruning)
	require.Equal(t, true, backuper.config.Compression)
	require.Equal(t, 1, backuper.config.KeepFiles)

	_, err = backuper.Backup(context.Background())
	require.NoError(t, err)

	// executes second backup and check the number of files
	backuper, err = NewBackuper(db.Path(), dir, []Option{
		WithVacuum(true),
		WithPruning(true, 1),
	}...,
	)
	require.NoError(t, err)
	require.Equal(t, true, backuper.config.Vacuum)
	require.Equal(t, true, backuper.config.Pruning)
	require.Equal(t, false, backuper.config.Compression)
	require.Equal(t, 1, backuper.config.KeepFiles)

	_, err = backuper.Backup(context.Background())
	require.NoError(t, err)
	requireFileCount(t, dir, 1)

	require.NoError(t, backuper.Close())
}

func TestBackuperMultipleBackupCalls(t *testing.T) {
	t.Parallel()

	backuper, err := NewBackuper(createControlDatabase(t).Path(), backupDir(t))
	require.NoError(t, err)

	// first call
	_, err = backuper.Backup(context.Background())
	require.NoError(t, err)

	// second call
	result, err := backuper.Backup(context.Background())
	require.NoError(t, err)

	require.NoError(t, err)
	require.Equal(t, int64(311296), result.Size)
	require.Equal(t, int64(0), result.SizeAfterVacuum)
	require.Equal(t, time.Duration(0), result.VacuumElapsedTime)
	require.Greater(t, result.ElapsedTime, time.Duration(0))

	require.NoError(t, backuper.Close())
}
