#	Building HA-JDBC

To build from source, first obtain the source code from either:

*	[Source code archive][tags]
*	[Version control](source-repository.html)

##	Build requirements

*	[JDK 1.7+][jdk]
*	[Maven 3.0+][maven]

##	Building the JAR

	mvn package

The generated jar file resides in the `target` directory.
As of version 2.1, HA-JDBC has no require dependencies.
See the [Dependencies](dependencies.html) documentation to download any optional dependencies.

##	Building the project site/documentation

	mvn site

The generated documentation resides in the `target/site` directory.

[tags]: http://github.com/ha-jdbc/ha-jdbc/tags "HA-JDBC source code archive"
[jdk]: http://www.oracle.com/technetwork/java/javase/downloads/index.html "Java SE"
[maven]: http://maven.apache.org/download.html "Apache Maven Project"
