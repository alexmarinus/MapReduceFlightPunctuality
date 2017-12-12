# Both tasks
build: build_delay build_late

# Task 1
build_delay: org/leicester/Delay.java
	javac org/leicester/Delay.java
	jar cvf Delay.jar org
	hadoop jar Delay.jar org.leicester.Delay PunctualityData/ OutputDelay/

# Task 2
build_late: org/leicester/Late.java
	javac org/leicester/Late.java
	jar cvf Late.jar org
	hadoop jar Late.jar org.leicester.Late PunctualityData/ OutputLate/

# Remove generated files
clean:
	rm org/leicester/*.class
	rm *.jar
	rm -r OutputDelay OutputLate
