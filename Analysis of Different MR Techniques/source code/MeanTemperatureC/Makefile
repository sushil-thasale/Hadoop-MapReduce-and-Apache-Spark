
MeanTemperatureC.jar: MeanTemperatureC.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/MeanTemperatureC-all.jar MeanTemperatureC.jar

run: MeanTemperatureC.jar
	rm -rf output
	hadoop jar MeanTemperatureC.jar input output

clean:
	rm -rf build MeanTemperatureC.jar output .gradle
