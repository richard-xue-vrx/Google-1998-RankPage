Groupname: RankPage

Members:
Richard Xue (committed anonymously under Richard Xue and richard-xue-vrx)
Edison Liang
Paul Kotys (commited under both paulk924 and ahwassa10)
Nimish Jayakar

Third Party Libraries:

We used two approved third party libraries in our project.

1. JSoup, in order to parse and filter our crawled documents easily.
    Version we used was jsoup-1.18.3, downloaded as a jar file and placed into the /lib folder from:
    https://jsoup.org/download

2. Tika, in order to filter out non-English text from our documents for more effective search results.
    Version we used was tika-app-3.0.0, downloaded as a jar file and placed into the /lib folder from:
    https://www.apache.org/dyn/closer.lua/tika/3.0.0/tika-app-3.0.0.jar

Instructions for Compilation:

On Windows, the compile.bat script will compile crawler.jar, indexer.jar, pagerank.jar, and all the java file in the bin directory. Assumes that jsoup and tika are in the lib directory. The project can also be imported into Eclipse and built with Eclipse's build tools.

Otherwise, please run the compile.sh script on Linux/MacOS. Please ensure the third party .jar files are downloaded and placed into the /lib folder!

Regarding running the crawler:

On Windows, the crawlerCompileAndRun.bat script will compile the relevant jar and java files and spawn the terminal instances with running kvs coordinator/workers and flame coordinator/workers. Similarly, on Linux/MacOS, scriptSpawn.sh will achieve the same thing
akin to HW9 scripts.

The first argument to the crawler is a list of seed urls in the format "url1;url2;url3".

Example seed URLs:
1. https://www.wikipedia.org/
2. https://philly.eater.com/maps/38-best-philadelphia-restaurants

If running on Linux or MacOS, run the command below to flame-submit the Crawler job 

```bash
java -cp bin cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler https://www.wikipedia.org/;https://philly.eater.com/maps/38-best-philadelphia-restaurants
```

If running on Windows on Powershell, use this command instead:

```bash
java -cp bin cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler "https://www.wikipedia.org/;https://philly.eater.com/maps/38-best-philadelphia-restaurants"
```
