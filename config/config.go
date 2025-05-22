package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Raft struct {
		IPaddr                []string `yaml:"nodes"`
		ElectionTimeout       int      `yaml:"election_timeout"`
		ElectionTimeoutRandom int      `yaml:"election_timeout_random"`
		HeartbeatInterval     int      `yaml:"heartbeat_interval"`
	}
	Master struct {
		NMaster int      `yaml:"nMaster"`
		Files   []string `yaml:"files"`
		NReduce int      `yaml:"nReduce"`
	}
	Worker struct {
		NWorker int    `yaml:"nWorker"`
		Plugin  string `yaml:"plugin"`
	}
	Log struct {
		Level      string `yaml:"level"`
		MasterFile string `yaml:"masterfile"`
		WorkerFile string `yaml:"workerfile"`
	}
}

var GlobalConfig *Config

func LoadConfig(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &GlobalConfig)
	if err != nil {
		return err
	}
	return nil
}
