
DIR := $(shell basename `pwd`)

$(DIR).jar: PageRank/*.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR)-all.jar $(DIR).jar

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar wikipedia-simple-html.bz2 output

clean-output: 
	rm -rf data? data?? output

data:
	perl gendata.pl > wikipedia-simple-html
	shuf wikipedia-simple-html > wikipedia-simple1-html
	mv wikipedia-simple1-html wikipedia-simple-html
	bzip2 wikipedia-simple-html

clean: clean-output
	rm -rf build bin *.jar .gradle test.log

.PHONY: run clean-output data clean
