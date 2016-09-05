To compile this project you will need:
Scala 2.10.5
Sbt almost any version (but use defined in build.properties) file

Other dependencies will be downloaded automatically, if you refresh the project

To build this project just use command
sbt package
this will build "sparktest_2.10-1.0.jar" in PROJECT/target/scala-2.10

To run this project, i.e. on HDP, just use this command:
spark-submit --class "ru.hokan.HW3" /opt/sparktest_2.10-1.0.jar

specifying main class and path to jar (and jar itself) which should be run