package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/pkg/attempt"
	c "github.com/flynn/go-check"
)

type GitDeploySuite struct {
	Helper
}

var _ = c.ConcurrentSuite(&GitDeploySuite{})

var Attempts = attempt.Strategy{
	Total: 60 * time.Second,
	Delay: 500 * time.Millisecond,
}

func (s *GitDeploySuite) TestEnvDir(t *c.C) {
	r := s.newGitRepo(t, "env-dir")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.flynn("env", "set", "FOO=bar", "BUILDPACK_URL=https://github.com/kr/heroku-buildpack-inline"), Succeeds)

	push := r.git("push", "flynn", "master")
	t.Assert(push, SuccessfulOutputContains, "bar")
}

func (s *GitDeploySuite) TestEmptyRelease(t *c.C) {
	r := s.newGitRepo(t, "empty-release")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.flynn("env", "set", "BUILDPACK_URL=https://github.com/kr/heroku-buildpack-inline"), Succeeds)

	push := r.git("push", "flynn", "master")
	t.Assert(push, Succeeds)

	run := r.flynn("run", "echo", "foo")
	t.Assert(run, Succeeds)
	t.Assert(run, Outputs, "foo\n")
}

func (s *GitDeploySuite) TestBuildCaching(t *c.C) {
	r := s.newGitRepo(t, "build-cache")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.flynn("env", "set", "BUILDPACK_URL=https://github.com/kr/heroku-buildpack-inline"), Succeeds)

	r.git("commit", "-m", "bump", "--allow-empty")
	push := r.git("push", "flynn", "master")
	t.Assert(push, Succeeds)
	t.Assert(push, c.Not(OutputContains), "cached")

	r.git("commit", "-m", "bump", "--allow-empty")
	push = r.git("push", "flynn", "master")
	t.Assert(push, SuccessfulOutputContains, "cached: 0")

	r.git("commit", "-m", "bump", "--allow-empty")
	push = r.git("push", "flynn", "master")
	t.Assert(push, SuccessfulOutputContains, "cached: 1")
}

func (s *GitDeploySuite) TestAppRecreation(t *c.C) {
	r := s.newGitRepo(t, "empty")
	t.Assert(r.flynn("create", "-y", "app-recreation"), Succeeds)
	r.git("commit", "-m", "bump", "--allow-empty")
	t.Assert(r.git("push", "flynn", "master"), Succeeds)
	t.Assert(r.flynn("delete", "-y"), Succeeds)

	// recreate app and push again, it should work
	t.Assert(r.flynn("create", "-y", "app-recreation"), Succeeds)
	t.Assert(r.git("push", "flynn", "master"), Succeeds)
	t.Assert(r.flynn("delete", "-y"), Succeeds)
}

func (s *GitDeploySuite) TestGoBuildpack(t *c.C) {
	s.runBuildpackTest(t, "go-flynn-example", []string{"postgres"})
}

func (s *GitDeploySuite) TestNodejsBuildpack(t *c.C) {
	s.runBuildpackTest(t, "nodejs-flynn-example", nil)
}

func (s *GitDeploySuite) TestPhpBuildpack(t *c.C) {
	s.runBuildpackTest(t, "php-flynn-example", nil)
}

func (s *GitDeploySuite) TestRubyBuildpack(t *c.C) {
	s.runBuildpackTest(t, "ruby-flynn-example", nil)
}

func (s *GitDeploySuite) TestJavaBuildpack(t *c.C) {
	s.runBuildpackTest(t, "java-flynn-example", nil)
}

func (s *GitDeploySuite) TestClojureBuildpack(t *c.C) {
	s.runBuildpackTest(t, "clojure-flynn-example", nil)
}

func (s *GitDeploySuite) TestPlayBuildpack(t *c.C) {
	s.runBuildpackTest(t, "play-flynn-example", nil)
}

func (s *GitDeploySuite) TestPythonBuildpack(t *c.C) {
	s.runBuildpackTest(t, "python-flynn-example", nil)
}

func (s *GitDeploySuite) TestStaticBuildpack(t *c.C) {
	s.runBuildpackTestWithResponsePattern(t, "static-flynn-example", nil, `Hello, Flynn!`)
}

func (s *GitDeploySuite) TestPushTwice(t *c.C) {
	r := s.newGitRepo(t, "https://github.com/flynn-examples/nodejs-flynn-example")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.git("push", "flynn", "master"), Succeeds)
	t.Assert(r.git("commit", "-m", "second", "--allow-empty"), Succeeds)
	t.Assert(r.git("push", "flynn", "master"), Succeeds)
}

func (s *GitDeploySuite) runBuildpackTest(t *c.C, name string, resources []string) {
	s.runBuildpackTestWithResponsePattern(t, name, resources, `Hello from Flynn on port \d+`)
}

func (s *GitDeploySuite) runBuildpackTestWithResponsePattern(t *c.C, name string, resources []string, pat string) {
	r := s.newGitRepo(t, "https://github.com/flynn-examples/"+name)

	t.Assert(r.flynn("create", name), Outputs, fmt.Sprintf("Created %s\n", name))

	for _, resource := range resources {
		t.Assert(r.flynn("resource", "add", resource), Succeeds)
	}

	watcher, err := s.controllerClient(t).WatchJobEvents(name, "")
	t.Assert(err, c.IsNil)
	defer watcher.Close()

	push := r.git("push", "flynn", "master")
	t.Assert(push, SuccessfulOutputContains, "Creating release")
	t.Assert(push, SuccessfulOutputContains, "Application deployed")
	t.Assert(push, SuccessfulOutputContains, "Waiting for web job to start...")
	t.Assert(push, SuccessfulOutputContains, "* [new branch]      master -> master")
	t.Assert(push, c.Not(OutputContains), "timed out waiting for scale")
	t.Assert(push, SuccessfulOutputContains, "=====> Default web formation scaled to 1")

	watcher.WaitFor(ct.JobEvents{"web": {ct.JobStateUp: 1}}, scaleTimeout, nil)

	route := name + ".dev"
	newRoute := r.flynn("route", "add", "http", route)
	t.Assert(newRoute, Succeeds)

	err = Attempts.Run(func() error {
		// Make HTTP requests
		client := &http.Client{}
		req, err := http.NewRequest("GET", "http://"+routerIP, nil)
		if err != nil {
			return err
		}
		req.Host = route
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		contents, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		if res.StatusCode != 200 {
			return fmt.Errorf("Expected status 200, got %v", res.StatusCode)
		}
		m, err := regexp.MatchString(pat, string(contents))
		if err != nil {
			return err
		}
		if !m {
			return fmt.Errorf("Expected `%s`, got `%v`", pat, string(contents))
		}
		return nil
	})
	t.Assert(err, c.IsNil)

	t.Assert(r.flynn("scale", "web=0"), Succeeds)
}

func (s *GitDeploySuite) TestRunQuoting(t *c.C) {
	r := s.newGitRepo(t, "empty")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.git("push", "flynn", "master"), Succeeds)

	run := r.flynn("run", "bash", "-c", "echo 'foo bar'")
	t.Assert(run, Succeeds)
	t.Assert(run, Outputs, "foo bar\n")
}

// TestConfigDir ensures we don't regress on a bug where uploaded repos were
// being checked out into the bare git repo, which would fail if the repo
// contained a config directory because the bare repo had a config file in it.
func (s *GitDeploySuite) TestConfigDir(t *c.C) {
	r := s.newGitRepo(t, "config-dir")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.git("push", "flynn", "master"), Succeeds)
}

// TestLargeRepo ensures that there is no regression for https://github.com/flynn/flynn/issues/1799
func (s *GitDeploySuite) TestLargeRepo(t *c.C) {
	r := s.newGitRepo(t, "")

	// write 5MiB of random data to a file in 10KiB chunks
	dest, err := os.Create(filepath.Join(r.dir, "random"))
	t.Assert(err, c.IsNil)
	buf := make([]byte, 102400)
	for i := 0; i < 512; i++ {
		io.ReadFull(rand.Reader, buf)
		dest.Write(buf)
	}
	dest.Close()

	// push the repo
	t.Assert(r.git("add", "random"), Succeeds)
	t.Assert(r.git("commit", "-m", "data"), Succeeds)
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.git("push", "flynn", "master"), OutputContains, "Unable to select a buildpack")
}

func (s *GitDeploySuite) TestPrivateSSHKeyClone(t *c.C) {
	r := s.newGitRepo(t, "private-clone")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.flynn("env", "set", "BUILDPACK_URL=git@github.com:kr/heroku-buildpack-inline.git"), Succeeds)

	push := r.git("push", "flynn", "master")
	t.Assert(push, Succeeds)
}

func (s *GitDeploySuite) TestGitSubmodules(t *c.C) {
	r := s.newGitRepo(t, "empty")
	t.Assert(r.git("submodule", "add", "https://github.com/flynn-examples/go-flynn-example.git"), Succeeds)

	// use a private SSH URL to test ssh client key
	gmPath := filepath.Join(r.dir, ".gitmodules")
	gm, err := ioutil.ReadFile(gmPath)
	t.Assert(err, c.IsNil)
	gm = bytes.Replace(gm, []byte("https://github.com/"), []byte("git@github.com:"), 1)
	err = ioutil.WriteFile(gmPath, gm, os.ModePerm)
	t.Assert(err, c.IsNil)

	t.Assert(r.git("commit", "-am", "Add Submodule"), Succeeds)
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.git("push", "flynn", "master"), Succeeds)
	t.Assert(r.flynn("run", "ls", "go-flynn-example"), SuccessfulOutputContains, "main.go")
}

func (s *GitDeploySuite) TestSlugbuilderLimit(t *c.C) {
	r := s.newGitRepo(t, "slugbuilder-limit")
	t.Assert(r.flynn("create"), Succeeds)
	t.Assert(r.flynn("env", "set", "BUILDPACK_URL=git@github.com:kr/heroku-buildpack-inline.git"), Succeeds)
	t.Assert(r.flynn("limit", "set", "slugbuilder", "memory=500MB"), Succeeds)

	push := r.git("push", "flynn", "master")
	t.Assert(push, Succeeds)
	t.Assert(push, OutputContains, "524288000")
}

func (s *GitDeploySuite) TestCancel(t *c.C) {
	r := s.newGitRepo(t, "cancel-hang")
	t.Assert(r.flynn("create", "cancel-hang"), Succeeds)
	t.Assert(r.flynn("env", "set", "FOO=bar", "BUILDPACK_URL=https://github.com/kr/heroku-buildpack-inline"), Succeeds)

	// start watching for slugbuilder events
	watcher, err := s.controllerClient(t).WatchJobEvents("cancel-hang", "")
	t.Assert(err, c.IsNil)

	// start push
	cmd := exec.Command("git", "push", "flynn", "master")
	// put the command in its own process group (to emulate the way shells handle Ctrl-C)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = r.dir
	var stdout io.Reader
	stdout, _ = cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	out := &bytes.Buffer{}
	stdout = io.TeeReader(stdout, out)
	err = cmd.Start()
	t.Assert(err, c.IsNil)

	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
			t.Fatal("git push timed out")
		}
	}()

	// wait for sentinel
	sc := bufio.NewScanner(stdout)
	found := false
	for sc.Scan() {
		if strings.Contains(sc.Text(), "hanging...") {
			found = true
			break
		}
	}
	t.Log(out.String())
	t.Assert(found, c.Equals, true)

	// send Ctrl-C to git process group
	syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
	t.Assert(err, c.IsNil)
	go io.Copy(ioutil.Discard, stdout)
	cmd.Wait()
	close(done)

	// check that slugbuilder exits immediately
	err = watcher.WaitFor(ct.JobEvents{"slugbuilder": {ct.JobStateUp: 1, ct.JobStateDown: 1}}, 10*time.Second, nil)
	t.Assert(err, c.IsNil)
}
