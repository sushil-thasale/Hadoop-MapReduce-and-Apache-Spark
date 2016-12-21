
LongestPalindromes.jar: LongestPalindromes.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/LongestPalindromes-all.jar LongestPalindromes.jar

run: LongestPalindromes.jar
	rm -rf output
	hadoop jar LongestPalindromes.jar alice.txt output

clean:
	rm -rf build LongestPalindromes.jar output .gradle
