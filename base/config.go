package base

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/nuveo/log"
)

type config struct {
	URL   string
	Env   string
	Debug bool
}

// Config contains configs infos
var Config *config

// Load sets the initial settings
func Load() (err error) {
	if Config != nil {
		return
	}
	Config = &config{
		URL: os.Getenv("CLOUDAMQP_URL"),
		Env: os.Getenv("ENV"),
	}
	if Config.URL == "" {
		return fmt.Errorf("CLOUDAMQP_URL is missing in the environments variables")
	}
	if Config.Env == "" {
		return fmt.Errorf("ENV is missing in the environments variables")
	}

	return
}

// LoadEnv loads a file with the environment variables
func LoadEnv(filename string) (err error) {
	err = godotenv.Load(filename)
	if err != nil {
		log.Errorln("Error loading .env.testing file ", err)
		return
	}

	err = Load()
	if err != nil {
		log.Errorln("Error to load of the lib ", err)
	}
	return
}
