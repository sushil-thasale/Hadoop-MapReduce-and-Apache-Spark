
MeanTemperatureB.jar: MeanTemperatureB.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/MeanTemperatureB-all.jar MeanTemperatureB.jar

run: MeanTemperatureB.jar
	rm -rf output
	hadoop jar MeanTemperatureB.jar input output

clean:
	rm -rf build MeanTemperatureB.jar output .gradle
