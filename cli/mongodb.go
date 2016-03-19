package main

import (
	"fmt"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-docopt"
	"github.com/flynn/flynn/controller/client"
	ct "github.com/flynn/flynn/controller/types"
)

func init() {
	register("mongodb", runMongodb, `
usage: flynn mongodb mongo [--] [<argument>...]

Commands:
	mongodb  Open a console to a Flynn mongodb database. Any valid arguments to mongo may be provided.

Examples:

    $ flynn mongodb mongo

    $ flynn mongodb mongo -- --eval "db.users.find()"
`)
}

func runMongodb(args *docopt.Args, client *controller.Client) error {
	config, err := getAppMongodbRunConfig(client)
	if err != nil {
		return err
	}
	switch {
	case args.Bool["mongo"]:
		return runMongo(args, client, config)
	}
	return nil
}

func getAppMongodbRunConfig(client *controller.Client) (*runConfig, error) {
	appRelease, err := client.GetAppRelease(mustApp())
	if err != nil {
		return nil, fmt.Errorf("error getting app release: %s", err)
	}
	return getMongodbRunConfig(client, mustApp(), appRelease)
}

func getMongodbRunConfig(client *controller.Client, app string, appRelease *ct.Release) (*runConfig, error) {
	mongodbApp := appRelease.Env["FLYNN_MONGO"]
	if mongodbApp == "" {
		return nil, fmt.Errorf("No mongodb database found. Provision one with `flynn resource add mongodb`")
	}

	mongodbRelease, err := client.GetAppRelease(mongodbApp)
	if err != nil {
		return nil, fmt.Errorf("error getting mongodb release: %s", err)
	}

	config := &runConfig{
		App:        app,
		Release:    mongodbRelease.ID,
		Env:        make(map[string]string),
		DisableLog: true,
		Exit:       true,
	}
	for _, k := range []string{"MONGO_HOST", "MONGO_USER", "MONGO_PWD", "MONGO_DATABASE"} {
		v := appRelease.Env[k]
		if v == "" {
			return nil, fmt.Errorf("missing %s in app environment", k)
		}
		config.Env[k] = v
	}
	return config, nil
}

func runMongo(args *docopt.Args, client *controller.Client, config *runConfig) error {
	config.Entrypoint = []string{"mongo"}
	config.Env["PAGER"] = "less"
	config.Env["LESS"] = "--ignore-case --LONG-PROMPT --SILENT --tabs=4 --quit-if-one-screen --no-init --quit-at-eof"
	config.Args = append([]string{
		"--host", config.Env["MONGO_HOST"],
		"-u", config.Env["MONGO_USER"],
		"-p", config.Env["MONGO_PWD"],
		"--authenticationDatabase", "admin",
		config.Env["MONGO_DATABASE"],
	}, args.All["<argument>"].([]string)...)

	return runJob(client, *config)
}
