echo "Compiling all Java Classes! Please wait."
# Compile and create Crawler.jar
javac -cp lib/jsoup-1.18.3.jar:lib/tika-app-3.0.0.jar -d bin --source-path src $(find src/cis5550/jobs -name '*.java')
sleep 1
jar cf crawler.jar -C bin cis5550/jobs
sleep 1

# Compile and create Indexer.jar
javac -cp lib/jsoup-1.18.3.jar:lib/tika-app-3.0.0.jar -d bin --source-path src src/cis5550/jobs/Indexer.java
sleep 1
jar cf indexer.jar bin/cis5550/jobs/Indexer.class
sleep 1

# Compile and create PageRank.jar
javac -cp lib/jsoup-1.18.3.jar:lib/tika-app-3.0.0.jar -d bin --source-path src src/cis5550/jobs/PageRank.java
sleep 1
jar cf pagerank.jar bin/cis5550/jobs/PageRank.class
sleep 1


# Adding all jars in lib; Should work...
CLASSPATH=$(find lib -name '*.jar' | tr '\n' ':')

# Compile the Java files
javac -cp "$CLASSPATH" --source-path src -d bin $(find src -name '*.java')

echo "Compilation Complete!"