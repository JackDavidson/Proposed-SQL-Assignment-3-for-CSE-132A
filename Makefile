default:
	javac -cp ".:jar/spark-sql_2.11-2.0.1.jar:jar/scala-library-2.11.8.jar:jar/spark-catalyst_2.11-2.0.1.jar:jar/spark-core_2.11-2.0.1.jar" PA3.java

run: default
	java -classpath ".:jar/*" PA3
