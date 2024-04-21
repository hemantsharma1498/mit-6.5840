# Specify the name of the plugin
PLUGIN_NAME = wc

# Specify the name of the Go command
GO_COMMAND = my-command

# Specify the directory where the plugin is located
PLUGIN_DIR = ./mrapps

# Build the plugin
plugin:
	# go build -buildmode=plugin -o $(PLUGIN_DIR)/$(PLUGIN_NAME).so $(PLUGIN_DIR)/main.go
	go build -buildmode=plugin $(PLUGIN_DIR)/$(PLUGIN_NAME).go

FILES = pg-grimm.txt

# Start the coord
runc:
	# cat $(FILES)
	go run main/mrcoordinator.go $(FILES)

# Start the worker
runw:
	go run main/mrworker.go $(PLUGIN_NAME).so

# Build and run the plugin and command
all: build-plugin run-command

.PHONY: build-plugin run-command all
