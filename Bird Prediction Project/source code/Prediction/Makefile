
DIR := $(shell basename `pwd`)

$(DIR).jar: Prediction/*.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR)-all.jar $(DIR).jar

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar training_data testing_data output 10

clean-output: 
	rm -rf data? data?? models output models-*

clean: clean-output
	rm -rf build bin *.jar .gradle test.log

.PHONY: run clean-output clean
