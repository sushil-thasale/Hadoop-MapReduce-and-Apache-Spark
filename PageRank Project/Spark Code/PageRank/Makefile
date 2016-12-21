#!/bin/bash
# Author: Nat Tuck

jar:
	sbt package
	cp target/scala-*/pagerank_*.jar PageRank.jar

run: jar
	rm -rf out
	sbt "run wikipedia-simple-html.bz2 output"

clean:
	rm -rf project output target *.jar out
