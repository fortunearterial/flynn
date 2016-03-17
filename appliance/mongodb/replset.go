package mongodb

// Config structures

type replSetMember struct {
	ID       int    `bson:"_id"`
	Host     string `bson:"host"`
	Priority int    `bson:"priority"`
	Hidden   bool   `bson:"hidden"`
}

type replSetConfig struct {
	ID      string          `bson:"_id"`
	Members []replSetMember `bson:"members"`
	Version int
}

// Status structures

type replSetOptime struct {
	Timestamp int64 `bson:"ts"`
	Term      int64 `bson::"t`
}

type replSetStatusMember struct {
	Name   string        `bson:"name"`
	Optime replSetOptime `bson:"optime"`
}

type replSetStatus struct {
	Members []replSetStatusMember `bson:"members"`
}
