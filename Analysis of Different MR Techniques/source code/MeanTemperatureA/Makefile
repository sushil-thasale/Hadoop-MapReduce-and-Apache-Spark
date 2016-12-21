
MeanTemperatureA.jar: MeanTemperatureA.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/MeanTemperatureA-all.jar MeanTemperatureA.jar

run: MeanTemperatureA.jar
	rm -rf output
	hadoop jar MeanTemperatureA.jar input output

clean:
	rm -rf build MeanTemperatureA.jar output .gradle
