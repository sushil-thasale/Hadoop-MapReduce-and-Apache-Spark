
SecondarySort.jar: SecondarySort.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/SecondarySort-all.jar SecondarySort.jar

run: SecondarySort.jar
	rm -rf output
	hadoop jar SecondarySort.jar input output

clean:
	rm -rf build SecondarySort.jar output .gradle
