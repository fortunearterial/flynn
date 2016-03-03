package mongodb

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/inconshreveable/log15.v2"
	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/mgo.v2"
	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/mgo.v2/bson"
	mongodbxlog "github.com/flynn/flynn/appliance/mongodb/xlog"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/shutdown"
	"github.com/flynn/flynn/pkg/sirenia/client"
	"github.com/flynn/flynn/pkg/sirenia/state"
	"github.com/flynn/flynn/pkg/sirenia/xlog"
)

const (
	DefaultPort        = "27017"
	DefaultBinDir      = "/usr/bin"
	DefaultDataDir     = "/data"
	DefaultPassword    = ""
	DefaultOpTimeout   = 5 * time.Minute
	DefaultReplTimeout = 1 * time.Minute

	BinName    = "mongod"
	ConfigName = "mongod.conf"

	checkInterval = 1000 * time.Millisecond
)

var (
	// ErrRunning is returned when starting an already running process.
	ErrRunning = errors.New("process already running")

	// ErrStopped is returned when stopping an already stopped process.
	ErrStopped = errors.New("process already stopped")

	ErrNoReplicationStatus = errors.New("no replication status")
)

// Process represents a MongoDB process.
type Process struct {
	mtx sync.Mutex

	events chan state.DatabaseEvent

	// Replication configuration
	configValue   atomic.Value // *Config
	configApplied bool

	runningValue          atomic.Value // bool
	syncedDownstreamValue atomic.Value // *discoverd.Instance

	ID           string
	Singleton    bool
	Port         string
	BinDir       string
	DataDir      string
	Password     string
	ServerID     uint32
	OpTimeout    time.Duration
	ReplTimeout  time.Duration
	WaitUpstream bool

	Logger log15.Logger

	// cmd is the running system command.
	cmd *Cmd

	// cancelSyncWait cancels the goroutine that is waiting for
	// the downstream to catch up, if running.
	cancelSyncWait func()
}

// NewProcess returns a new instance of Process.
func NewProcess() *Process {
	p := &Process{
		Port:        DefaultPort,
		BinDir:      DefaultBinDir,
		DataDir:     DefaultDataDir,
		Password:    DefaultPassword,
		OpTimeout:   DefaultOpTimeout,
		ReplTimeout: DefaultReplTimeout,
		Logger:      log15.New("app", "mongodb"),

		events:         make(chan state.DatabaseEvent, 1),
		cancelSyncWait: func() {},
	}
	p.runningValue.Store(false)
	p.configValue.Store((*state.Config)(nil))
	p.events <- state.DatabaseEvent{}
	return p
}

func (p *Process) running() bool         { return p.runningValue.Load().(bool) }
func (p *Process) config() *state.Config { return p.configValue.Load().(*state.Config) }

func (p *Process) syncedDownstream() *discoverd.Instance {
	if downstream, ok := p.syncedDownstreamValue.Load().(*discoverd.Instance); ok {
		return downstream
	}
	return nil
}

func (p *Process) ConfigPath() string { return filepath.Join(p.DataDir, "my.cnf") }

func (p *Process) Reconfigure(config *state.Config) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	switch config.Role {
	case state.RolePrimary:
		if !p.Singleton && config.Downstream == nil {
			return errors.New("missing downstream peer")
		}
	case state.RoleSync, state.RoleAsync:
		if config.Upstream == nil {
			return fmt.Errorf("missing upstream peer")
		}
	case state.RoleNone:
	default:
		return fmt.Errorf("unknown role %v", config.Role)
	}

	if !p.running() {
		p.configValue.Store(config)
		p.configApplied = false
		return nil
	}

	return p.reconfigure(config)
}

func (p *Process) Start() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.running() {
		return errors.New("process already running")
	}
	if p.config() == nil {
		return errors.New("unconfigured process")
	}
	if p.config().Role == state.RoleNone {
		return errors.New("start attempted with role 'none'")
	}

	return p.reconfigure(nil)
}

func (p *Process) Stop() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if !p.running() {
		return errors.New("process already stopped")
	}
	return p.stop()
}

func (p *Process) Ready() <-chan state.DatabaseEvent {
	return p.events
}

func (p *Process) XLog() xlog.XLog {
	return mongodbxlog.XLog{}
}

func (p *Process) reconfigure(config *state.Config) error {
	logger := p.Logger.New("fn", "reconfigure")

	if err := func() error {
		if config != nil && config.Role == state.RoleNone {
			logger.Info("nothing to do", "reason", "null role")
			return nil
		}

		// If we've already applied the same config, we don't need to do anything
		if p.configApplied && config != nil && p.config() != nil && config.Equal(p.config()) {
			logger.Info("nothing to do", "reason", "config already applied")
			return nil
		}

		// If we're already running and it's just a change from async to sync with the same node, we don't need to restart
		if p.configApplied && p.running() && p.config() != nil && config != nil &&
			p.config().Role == state.RoleAsync && config.Role == state.RoleSync && config.Upstream.Meta["MONGODB_ID"] == p.config().Upstream.Meta["MONGODB_ID"] {
			logger.Info("nothing to do", "reason", "becoming sync with same upstream")
			return nil
		}

		// Make sure that we don't keep waiting for replication sync while reconfiguring
		p.cancelSyncWait()
		p.syncedDownstreamValue.Store((*discoverd.Instance)(nil))

		// If we're already running and this is only a downstream change, just wait for the new downstream to catch up
		if p.running() && p.config().IsNewDownstream(config) {
			logger.Info("downstream changed", "to", config.Downstream.Addr)
			p.waitForSync(config.Downstream, false)
			return nil
		}

		if config == nil {
			config = p.config()
		}

		if config.Role == state.RolePrimary {
			return p.assumePrimary(config.Downstream)
		}

		return p.assumeStandby(config.Upstream, config.Downstream)
	}(); err != nil {
		return err
	}

	// Apply configuration.
	p.configValue.Store(config)
	p.configApplied = true

	return nil
}

func (p *Process) assumePrimary(downstream *discoverd.Instance) (err error) {
	logger := p.Logger.New("fn", "assumePrimary")
	if downstream != nil {
		logger = logger.New("downstream", downstream.Addr)
	}

	if p.running() && p.config().Role == state.RoleSync {
		logger.Info("promoting to primary")
		p.waitForSync(downstream, true)
		return nil
	}

	logger.Info("starting as primary")

	// Assert that the process is not running. This should not occur.
	if p.running() {
		panic(fmt.Sprintf("unexpected state running role=%s", p.config().Role))
	}

	if err := p.writeConfig(configData{ /*ReadOnly: downstream != nil*/ }); err != nil {
		logger.Error("error writing config", "path", p.ConfigPath(), "err", err)
		return err
	}

	// FIXME(benbjohnson): Initialize database?
	// if err := p.installDB(); err != nil {
	// 	return err
	// }

	if err := p.start(); err != nil {
		return err
	}

	if err := p.initPrimaryDB(); err != nil {
		if e := p.stop(); err != nil {
			logger.Debug("ignoring error stopping process", "err", e)
		}
		return err
	}

	if downstream != nil {
		p.waitForSync(downstream, true)
	}

	return nil
}

/*
// Backup returns a reader for streaming a backup in xbstream format.
func (p *Process) Backup() (io.ReadCloser, error) {
	r := &backupReadCloser{}

	cmd := exec.Command(
		filepath.Join(p.BinDir, "innobackupex"),
		"--defaults-file="+p.ConfigPath(),
		"--host=127.0.0.1",
		"--port="+p.Port,
		"--user=flynn",
		"--password="+p.Password,
		"--socket=",
		"--stream=xbstream",
		".",
	)
	cmd.Dir = p.DataDir
	cmd.Stderr = os.Stderr
	cmd.Stderr = &r.stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		stdout.Close()
		return nil, err
	}

	// Attach to reader wrapper.
	r.cmd = cmd
	r.stdout = stdout

	return r, nil
}

type BackupInfo struct {
	LogFile string
	LogPos  string
	GTID    string
}

func (p *Process) extractBackupInfo() (*BackupInfo, error) {
	buf, err := ioutil.ReadFile(filepath.Join(p.DataDir, "xtrabackup_binlog_info"))
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(buf))
	if len(fields) < 3 {
		return nil, fmt.Errorf("malformed xtrabackup_binlog_info, len", len(fields))
	}
	return &BackupInfo{LogFile: fields[0], LogPos: fields[1], GTID: fields[2]}, nil
}

// Restore restores the database from an xbstream backup.
func (p *Process) Restore(r io.Reader) (*BackupInfo, error) {
	if err := p.writeConfig(configData{}); err != nil {
		return nil, err
	}
	if err := p.unpackXbstream(r); err != nil {
		return nil, err
	}
	backupInfo, err := p.extractBackupInfo()
	if err != nil {
		return nil, err
	}
	if err := p.restoreApplyLog(); err != nil {
		return nil, err
	}
	return backupInfo, nil
}

func (p *Process) unpackXbstream(r io.Reader) error {
	cmd := exec.Command(filepath.Join(p.BinDir, "xbstream"), "-x", "--directory="+p.DataDir)
	cmd.Stdin = ioutil.NopCloser(r)

	if buf, err := cmd.CombinedOutput(); err != nil {
		p.Logger.Error("xbstream failed", "err", err, "output", string(buf))
		return err
	}

	return nil
}

func (p *Process) restoreApplyLog() error {
	cmd := exec.Command(
		filepath.Join(p.BinDir, "innobackupex"),
		"--defaults-file="+p.ConfigPath(),
		"--apply-log",
		p.DataDir,
	)
	if buf, err := cmd.CombinedOutput(); err != nil {
		p.Logger.Error("innobackupex apply-log failed", "err", err, "output", string(buf))
		return err
	}
	return nil
}
*/
func (p *Process) assumeStandby(upstream, downstream *discoverd.Instance) error {
	logger := p.Logger.New("fn", "assumeStandby", "upstream", upstream.Addr)
	logger.Info("starting up as standby")

	if err := p.writeConfig(configData{ /*ReadOnly: true*/ }); err != nil {
		logger.Error("error writing config", "path", p.ConfigPath(), "err", err)
		return err
	}

	/*var backupInfo *BackupInfo*/
	if p.running() {
		if err := p.stop(); err != nil {
			return err
		}
	} else {
		if err := p.waitForUpstream(upstream); err != nil {
			return err
		}

		/*
			if err := func() error {
				logger.Info("retrieving backup")
				resp, err := http.Get(fmt.Sprintf("http://%s/backup", httpAddr(upstream.Addr)))
				if err != nil {
					logger.Error("error connecting to upstream for backup", "err", err)
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					logger.Error("error code returned from backup", "status_code", resp.StatusCode)
					return err
				}

				hash := sha512.New()

				logger.Info("restoring backup")
				backupInfo, err = p.Restore(io.TeeReader(resp.Body, hash))
				if err != nil {
					logger.Error("error restoring backup", "err", err)
					return err
				}

				// Close response and confirm backup from trailer.
				if err := resp.Body.Close(); err != nil {
					logger.Error("error closing backup body", "err", err)
					return err
				}

				chk := hex.EncodeToString(hash.Sum(nil))
				logger.Error("verifying backup checksum", "hash", chk)
				if hdr := resp.Trailer.Get(backupChecksumTrailer); hdr != chk {
					logger.Error("invalid backup checksum", "hash", chk)
					return errors.New("invalid backup")
				}

				return nil
			}(); err != nil {
				if files, err := ioutil.ReadDir("/data"); err == nil {
					for _, file := range files {
						os.RemoveAll(filepath.Join("/data", file.Name()))
					}
				}
				return err
			}
		*/
	}

	if err := p.start(); err != nil {
		return err
	}

	if err := func() error {
		// Connect to local server and set up slave replication.
		session, err := p.connectLocal()
		if err != nil {
			logger.Error("error acquiring session", "err", err)
			return err
		}
		defer session.Close()

		/*
			// Stop the slave first before changing GTID & MASTER settings.
			if _, err := db.Exec(`STOP SLAVE`); err != nil {
				return err
			}

			// Enable semi-synchronous on slave.
			if _, err := db.Exec(`SET GLOBAL rpl_semi_sync_slave_enabled = 1`); err != nil {
				return err
			}
		*/

		/*
			// Only update the GTID if we read from a backup.
			if backupInfo != nil {
				logger.Info("updating gtid_slave_pos", "gtid", backupInfo.GTID)
				if _, err := db.Exec(fmt.Sprintf(`SET GLOBAL gtid_slave_pos = "%s";`, backupInfo.GTID)); err != nil {
					logger.Error("error updating slave gtid")
					return err
				}
			}
		*/

		/*
			host, port, _ := net.SplitHostPort(upstream.Addr)
			logger.Info("changing master", "host", host, "port", port)
			if _, err := db.Exec(fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%s, MASTER_USER='flynn', MASTER_PASSWORD='%s', MASTER_CONNECT_RETRY=10, MASTER_USE_GTID=current_pos;", host, port, p.Password)); err != nil {
				logger.Error("error changing master", "host", host, "port", port, "err", err)
				return err
			}
			if _, err := db.Exec(`STOP SLAVE IO_THREAD`); err != nil {
				logger.Error("error stopping slave io thread", "err", err)
				return err
			}
			if _, err := db.Exec(`START SLAVE IO_THREAD`); err != nil {
				logger.Error("error starting slave io thread", "err", err)
				return err
			}

			// Start slave.
			logger.Info("starting slave")
			if _, err := db.Exec(`START SLAVE`); err != nil {
				return err
			}
		*/

		return nil
	}(); err != nil {
		return err
	}

	if downstream != nil {
		p.waitForSync(downstream, false)
	}

	return nil
}

// initPrimaryDB initializes the local database with the correct users and plugins.
func (p *Process) initPrimaryDB() error {
	logger := p.Logger.New("fn", "initPrimaryDB")
	logger.Info("initializing primary database")

	// Initialize replica set through mongo CLI because mgo hangs otherwise.
	cmd := exec.Command(filepath.Join(p.BinDir, "mongo"), "--eval", "rs.initiate()", "127.0.0.1:"+p.Port)
	if output, err := cmd.CombinedOutput(); err != nil {
		logger.Error("error initializing replica set", "err", err, "out", string(output))
		return err
	}

	mgo.SetDebug(true) // TEMP(benbjohnson)

	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:   []string{"127.0.0.1:" + p.Port},
		Direct:  true,
		Timeout: p.OpTimeout,
	})
	if err != nil {
		logger.Error("error acquiring connection", "err", err)
		return err
	}
	defer session.Close()

	// if err := session.Run(bson.D{{"eval", "rs.initiate()"}}, nil); err != nil {
	// 	return err
	// }

	// If we are running in Singleton mode we don't need to setup replication
	if p.Singleton {
		return nil
	}

	/*
		// Enable semi-sync replication on the master.
		master_variables := map[string]string{
			"rpl_semi_sync_master_wait_point":    "AFTER_SYNC",
			"rpl_semi_sync_master_timeout":       "18446744073709551615",
			"rpl_semi_sync_master_enabled":       "1",
			"rpl_semi_sync_master_wait_no_slave": "1",
		}

		for v, val := range master_variables {
			if _, err := db.Exec(fmt.Sprintf(`SET GLOBAL %s = %s`, v, val)); err != nil {
				logger.Error("error setting system variable", "var", v, "val", val, "err", err)
				return err
			}
		}
	*/

	return nil
}

// upstreamTimeout is of the order of the discoverd heartbeat to prevent
// waiting for an upstream which has gone down.
var upstreamTimeout = 10 * time.Second

func httpAddr(addr string) string {
	host, p, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(p)
	return fmt.Sprintf("%s:%d", host, port+1)
}

func (p *Process) waitForUpstream(upstream *discoverd.Instance) error {
	logger := p.Logger.New("fn", "waitForUpstream", "upstream", upstream.Addr, "upstream_http_addr", httpAddr(upstream.Addr))
	logger.Info("waiting for upstream to come online")
	upstreamClient := client.NewClient(upstream.Addr)

	timer := time.NewTimer(upstreamTimeout)
	defer timer.Stop()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		status, err := upstreamClient.Status()
		if err == nil {
			logger.Info("status", "running", status.Database.Running, "xlog", status.Database.XLog, "user_exists", status.Database.UserExists)
		}
		if err != nil {
			logger.Error("error getting upstream status", "err", err)
		} else if status.Database.Running && status.Database.XLog != "" && status.Database.UserExists {
			logger.Info("upstream is online")
			return nil
		}

		select {
		case <-timer.C:
			logger.Error("upstream did not come online in time")
			return errors.New("upstream is offline")
		case <-ticker.C:
		}
	}
}

func (p *Process) connectLocal() (*mgo.Session, error) {
	session, err := mgo.DialWithInfo(p.DialInfo())
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (p *Process) start() error {
	logger := p.Logger.New("fn", "start", "id", p.ID, "port", p.Port)
	logger.Info("starting process")

	cmd := NewCmd(exec.Command(filepath.Join(p.BinDir, "mongod"), "--config", p.ConfigPath()))
	if err := cmd.Start(); err != nil {
		logger.Error("failed to start process", "err", err)
		return err
	}
	p.cmd = cmd
	p.runningValue.Store(true)

	go func() {
		if <-cmd.Stopped(); cmd.Err() != nil {
			logger.Error("process unexpectedly exit", "err", cmd.Err())
			shutdown.ExitWithCode(1)
		}
	}()

	logger.Debug("waiting for process to start")

	timer := time.NewTimer(p.OpTimeout)
	defer timer.Stop()

	for {
		// Connect to server.
		// Retry after sleep if an error occurs.
		if err := func() error {
			session, err := mgo.DialWithInfo(&mgo.DialInfo{
				Addrs:   []string{"127.0.0.1:" + p.Port},
				Direct:  true,
				Timeout: p.OpTimeout,
			})
			if err != nil {
				return err
			}
			defer session.Close()

			// if err := session.Ping(); err != nil {
			// 	return err
			// }

			return nil
		}(); err != nil {
			select {
			case <-timer.C:
				logger.Error("timed out waiting for process to start", "err", err)
				if err := p.stop(); err != nil {
					logger.Error("error stopping process", "err", err)
				}
				return err
			default:
				logger.Debug("ignoring error connecting to mongodb", "err", err)
				time.Sleep(checkInterval)
				continue
			}
		}

		logger.Debug("process started")
		return nil
	}
}

func (p *Process) stop() error {
	logger := p.Logger.New("fn", "stop")
	logger.Info("stopping mongodb")

	p.cancelSyncWait()

	// Attempt to kill.
	logger.Debug("stopping daemon")
	if err := p.cmd.Stop(); err != nil {
		logger.Error("error stopping command", "err", err)
	}

	// Wait for cmd to stop or timeout.
	select {
	case <-time.After(p.OpTimeout):
		return errors.New("unable to kill process")
	case <-p.cmd.Stopped():
		p.runningValue.Store(false)
		return nil
	}
}

func (p *Process) Info() (*client.DatabaseInfo, error) {
	info := &client.DatabaseInfo{
		Config:           p.config(),
		Running:          p.running(),
		SyncedDownstream: p.syncedDownstream(),
	}

	xlog, err := p.XLogPosition()
	info.XLog = string(xlog)
	if err != nil {
		return info, err
	}

	info.UserExists, err = p.userExists()
	if err != nil {
		return info, err
	}
	info.ReadWrite, err = p.isReadWrite()
	if err != nil {
		return info, err
	}
	return info, err
}

func (p *Process) isReadWrite() (bool, error) {
	if !p.running() {
		return false, nil
	}
	panic("FIXME(benbjohnson): isReadWrite()")

	/*
		db, err := p.connectLocal()
		if err != nil {
			return false, err
		}
		defer db.Close()
		var readOnly string
		if err := db.QueryRow("SELECT @@read_only").Scan(&readOnly); err != nil {
			return false, err
		}
		if readOnly == "0" {
			return true, nil
		}
		return false, nil
	*/
}

func (p *Process) userExists() (bool, error) {
	if !p.running() {
		return false, errors.New("mongod is not running")
	}

	panic("FIXME(benbjohnson): userExists()")
	/*
		session, err := p.connectLocal()
		if err != nil {
			return false, err
		}
		defer session.Close()

		var res sql.NullInt64
		if err := db.QueryRow("SELECT 1 FROM mysql.user WHERE User='flynn'").Scan(&res); err != nil {
			return false, err
		}
		return res.Valid, nil
	*/
}

func (p *Process) waitForSync(downstream *discoverd.Instance, enableWrites bool) {
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	var once sync.Once
	p.cancelSyncWait = func() {
		once.Do(func() { close(stopCh); <-doneCh })
	}

	go func() {
		defer close(doneCh)

		startTime := time.Now().UTC()
		logger := p.Logger.New(
			"fn", "waitForSync",
			"sync_name", downstream.Meta["MONGODB_ID"],
			"start_time", log15.Lazy{func() time.Time { return startTime }},
		)

		logger.Info("waiting for downstream replication to catch up")
		defer logger.Info("finished waiting for downstream replication")

		prevSlaveXLog := p.XLog().Zero()
		for {
			// Check if "wait sync" has been canceled.
			select {
			case <-stopCh:
				logger.Debug("canceled, stopping")
				return
			default:
			}

			// Read local master status.
			masterXLog, err := p.XLogPosition()
			if err != nil {
				logger.Error("error reading master xlog", "err", err)
				startTime = time.Now().UTC()
				select {
				case <-stopCh:
					logger.Debug("canceled, stopping")
					return
				case <-time.After(checkInterval):
				}
				continue
			}
			logger.Info("master xlog", "gtid", masterXLog)

			// Read downstream slave status.
			slaveXLog, err := p.nodeXLogPosition(&mgo.DialInfo{
				Addrs:   []string{downstream.Addr},
				Timeout: p.OpTimeout,
			})
			if err != nil {
				logger.Error("error reading slave xlog", "err", err)
				startTime = time.Now().UTC()
				select {
				case <-stopCh:
					logger.Debug("canceled, stopping")
					return
				case <-time.After(checkInterval):
				}
				continue
			}

			logger.Info("mongodb slave xlog", "gtid", slaveXLog)

			elapsedTime := time.Since(startTime)
			logger := logger.New(
				"master_log_pos", masterXLog,
				"slave_log_pos", slaveXLog,
				"elapsed", elapsedTime,
			)

			// Mark downstream server as synced if the xlog matches the master.
			if cmp, err := p.XLog().Compare(masterXLog, slaveXLog); err == nil && cmp == 0 {
				logger.Info("downstream caught up")
				p.syncedDownstreamValue.Store(downstream)
				break
			}

			// If the slave's xlog is making progress then reset the start time.
			if cmp, err := p.XLog().Compare(prevSlaveXLog, slaveXLog); err == nil && cmp == -1 {
				logger.Debug("slave status progressing, resetting start time")
				startTime = time.Now().UTC()
			}
			prevSlaveXLog = slaveXLog

			if elapsedTime > p.ReplTimeout {
				logger.Error("error checking replication status", "err", "downstream unable to make forward progress")
				return
			}

			logger.Debug("continuing replication check")
			select {
			case <-stopCh:
				logger.Debug("canceled, stopping")
				return
			case <-time.After(checkInterval):
			}
		}

		/*
			if enableWrites {
				db, err := p.connectLocal()
				if err != nil {
					logger.Error("error acquiring connection", "err", err)
					return
				}
				defer db.Close()
				if _, err := db.Exec(`SET GLOBAL read_only = 0`); err != nil {
					logger.Error("error setting database read/write", "err", err)
					return
				}
			}
		*/
	}()
}

// DialInfo returns dial info for connecting to the local process as the "flynn" user.
func (p *Process) DialInfo() *mgo.DialInfo {
	return &mgo.DialInfo{
		Addrs:   []string{"127.0.0.1:" + p.Port},
		Timeout: p.OpTimeout,
	}
}

func (p *Process) XLogPosition() (xlog.Position, error) {
	return p.nodeXLogPosition(p.DialInfo())
}

// XLogPosition returns the current XLogPosition of node specified by DSN.
func (p *Process) nodeXLogPosition(info *mgo.DialInfo) (xlog.Position, error) {
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return p.XLog().Zero(), err
	}
	defer session.Close()

	time.Sleep(2 * time.Second)
	var entry bson.M
	if err := session.DB("local").C("oplog.rs").Find(nil).Sort("-ts").One(&entry); err != nil {
		return p.XLog().Zero(), fmt.Errorf("find oplog.rs.ts error: %s", err)
	}
	fmt.Printf("XLOG: %T\n", entry["ts"])
	<-(chan struct{})(nil)

	return xlog.Position(entry["ts"].(bson.MongoTimestamp)), nil
}

func (p *Process) runCmd(cmd *exec.Cmd) error {
	p.Logger.Debug("running command", "fn", "runCmd", "cmd", cmd.Args)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (p *Process) writeConfig(d configData) error {
	d.ID = p.ID
	d.Port = p.Port
	d.DataDir = p.DataDir

	f, err := os.Create(p.ConfigPath())
	if err != nil {
		return err
	}
	defer f.Close()

	return configTemplate.Execute(f, d)
}

type configData struct {
	ID      string
	Port    string
	DataDir string
}

var configTemplate = template.Must(template.New("my.cnf").Parse(`
storage:
  dbPath: {{.DataDir}}
  journal:
    enabled: true
  engine: wiredTiger

#systemLog:
#  destination: file
#  path: {{.DataDir}}/mongod.log
#  logAppend: true

net:
  port: {{.Port}}

#security:
#  authorization: enabled

replication:
  replSetName: rs0
  enableMajorityReadConcern: true
`[1:]))
