package main

import (
	"fmt"

	c "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/mgo.v2"
	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/mgo.v2/bson"
	ct "github.com/flynn/flynn/controller/types"
)

type MongoDBSuite struct {
	Helper
}

var _ = c.ConcurrentSuite(&MongoDBSuite{})

type mgoLogger struct {
	t *c.C
}

func (l mgoLogger) Output(calldepth int, s string) error {
	debugf(l.t, s)
	return nil
}

// Sirenia integration tests
var sireniaMongoDB = sireniaDatabase{
	appName:    "mongodb",
	serviceKey: "FLYNN_MONGO",
	hostKey:    "MONGO_HOST",
	assertWriteable: func(t *c.C, r *ct.Release, d *sireniaDeploy) {
		mgo.SetLogger(mgoLogger{t})
		mgo.SetDebug(true)
		session, err := mgo.DialWithInfo(&mgo.DialInfo{
			Addrs:    []string{fmt.Sprintf("leader.%s.discoverd", d.name)},
			Username: "flynn",
			Password: r.Env["MONGO_PWD"],
			Database: "admin",
			Direct:   true,
		})
		session.SetMode(mgo.Monotonic, true)
		defer session.Close()
		t.Assert(err, c.IsNil)
		t.Assert(session.DB("test").C("test").Insert(&bson.M{"test": "test"}), c.IsNil)
	},
}

func (s *MongoDBSuite) TestDeploySingleAsync(t *c.C) {
	testSireniaDeploy(s.controllerClient(t), s.discoverdClient(t), t, &sireniaDeploy{
		name:        "mongodb-single-async",
		db:          sireniaMongoDB,
		sireniaJobs: 3,
		webJobs:     2,
		expected:    testDeploySingleAsync,
	})
}

func (s *MongoDBSuite) TestDeployMultipleAsync(t *c.C) {
	testSireniaDeploy(s.controllerClient(t), s.discoverdClient(t), t, &sireniaDeploy{
		name:        "mongodb-multiple-async",
		db:          sireniaMongoDB,
		sireniaJobs: 5,
		webJobs:     2,
		expected:    testDeployMultipleAsync,
	})
}
