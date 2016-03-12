package mongodb

type replSetMember struct {
	ID       int    `bson:"_id"`
	Host     string `bson:"host"`
	Priority int    `bson:"priority"`
	Hidden   bool   `bson:"hidden"`
}

type replSetConfig struct {
	ID      string          `bson:"_id"`
	Members []replSetMember `bson:"members"`
}
