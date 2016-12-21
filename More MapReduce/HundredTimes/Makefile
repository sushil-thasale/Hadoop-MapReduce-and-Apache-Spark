
HundredTimes.jar: HundredTimes.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/HundredTimes-all.jar HundredTimes.jar

run: HundredTimes.jar
	rm -rf output
	hadoop jar HundredTimes.jar alice.txt output

clean:
	rm -rf build HundredTimes.jar output .gradle
