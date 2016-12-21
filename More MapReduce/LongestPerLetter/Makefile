
LongestPerLetter.jar: LongestPerLetter.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/LongestPerLetter-all.jar LongestPerLetter.jar

run: LongestPerLetter.jar
	rm -rf output
	hadoop jar LongestPerLetter.jar alice.txt output

clean:
	rm -rf build LongestPerLetter.jar output .gradle
