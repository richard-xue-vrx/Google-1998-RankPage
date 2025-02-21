package cis5550.test;

import cis5550.flame.*;
import java.util.*;
import cis5550.kvs.*;
import java.nio.file.*;
import java.util.stream.Collectors;
import java.text.DecimalFormat;

public class KvsBenchmark {
    /*
     * This Flame Job simply makes a number of calls to putRow() and 
     * subsequently to getRow(). It will give you a sense of how long it 
     * actually takes to read/write (a critical primitive in most of your 
     * components) to/from your storage layer. You can further experiment by 
     * changing the number of KVS nodes you have running, adding profiling 
     * support, and/or deploying it to AWS.
     * 
     * In order to run this program, you will first need to jar it, after which
     * you can initialize your desired number of Flame and KVS workers and then 
     * just run the program. Note that you are encouraged to modify this program
     * to better suit your testing needs, e.g., making it look more like a 
     * crawler or indexer. This file is just a starting point for performance 
     * tuning your project.
     * 
     * Possible List of commands to run this program:
     * <Initialize all workers/coordinators>
     * 
     * javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar \
     *       -d out src/cis5550/test/kvsBenchmark.java
     * jar cvf kvsBenchmark.jar -C out .
     * 
     * java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar \
     *      cis5550.flame.FlameSubmit localhost:9000 kvsBenchmark.jar \
     *      cis5550.test.KvsBenchmark ./pt-crawl-example
     */
    // Class to hold operation data
    static class OperationData {
        String rowKey;
        long fileSize;
        long putTime; // in nanoseconds
        long getTime; // in nanoseconds

        OperationData(long fileSize, long putTime, String rowKey) {
            this.fileSize = fileSize;
            this.putTime = putTime;
            this.rowKey = rowKey;
        }

        void setGetTime(long getTime) {
            this.getTime = getTime;
        }
    }

    static void benchmarkPutRow(KVSClient kvs, String table,
                                List<OperationData> contentData,
                                List<Path> contentFiles) throws Exception{
        System.out.println("Starting putRow() operations for '" + table +
                           "' table...");

        for (Path filePath : contentFiles) {
            // Read file into byte[] (not timed)
            byte[] content = Files.readAllBytes(filePath);
            long fileSize = content.length;

            String rowKey = filePath.getFileName().toString();
            Row row = new Row(rowKey);
            row.put("content", content);

            // Time the putRow() operation
            long startTime = System.nanoTime();
            kvs.putRow(table, row);
            long totalTime = System.nanoTime() - startTime;

            // Store the data
            contentData.add(new OperationData(fileSize, totalTime, rowKey));
        }

        System.out.println("Completed putRow() operations for '" + table +
                           "' table.");
    }

    static void benchmarkGetRow(KVSClient kvs, String table,
                                List<OperationData> contentData) {
        System.out.println("Starting getRow() operations for '" + table +
                           "' table...");
        for (OperationData data : contentData) {
            String rowKey = data.rowKey;

            // Ensure the rowKey is not null or empty before using it
            if (rowKey == null || rowKey.isEmpty()) {
                System.err.println("Skipping getRow() operation: row key " + 
                                   rowKey + " is null or empty.");
                continue;
            }

            try {
                // Time the getRow() operation
                long startTime = System.nanoTime();
                Row retrievedRow = kvs.getRow(table, rowKey);
                long totalTime = System.nanoTime() - startTime;

                data.setGetTime(totalTime);

                if (retrievedRow == null) {
                    System.err.println("Row not found for key: " + rowKey);
                }
            } catch (Exception e) {
                System.err.println("Exception while performing getRow() " +
                                   "for key: " + rowKey);
                e.printStackTrace();
            }
        }

        System.out.println("Completed getRow() operations for '" + table +
                           "' table.");
    }

    public static void run(FlameContext ctx, String args[]) throws Exception {
        if (args.length != 1) {
            System.err.println("Remember to include <directory_path> to " + 
                               "the pt-crawl-example directory!");
            return;
        }

        String directoryPath = args[0];
        KVSClient kvs = ctx.getKVS();

        // Read all files in the directory
        List<Path> filePaths = Files.list(Paths.get(directoryPath))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());

        int totalFiles = filePaths.size();
        int half = totalFiles / 2;

        List<Path> contentFiles = filePaths.subList(0, half);
        List<Path> ptContentFiles = filePaths.subList(half, totalFiles);

        // Data structures to hold operation data
        List<OperationData> contentData = new ArrayList<>();
        List<OperationData> ptContentData = new ArrayList<>();

        // Benchmark putRow() for 'content' table (in-memory)
        benchmarkPutRow(kvs, "content", contentData, contentFiles);

        // Benchmark putRow() for 'pt-content' table (persistent)
        benchmarkPutRow(kvs, "pt-content", ptContentData, ptContentFiles);

        // Benchmark getRow() for 'content' table (in-memory)
        benchmarkGetRow(kvs, "content", contentData);

        // Benchmark getRow() for 'pt-content' table (persistent)
        benchmarkGetRow(kvs, "pt-content", ptContentData);

        // Output results
        DecimalFormat df = new DecimalFormat("#.##");
        processResults("content", contentData, df);
        processResults("pt-content", ptContentData, df);
        System.out.println("Benchmark completed.");
    }

    private static void processResults(String tableName,
                                       List<OperationData> dataList,
                                       DecimalFormat df) {
        long totalPutTime = 0;
        long totalGetTime = 0;
        long totalFileSize = 0;

        for (OperationData data : dataList) {
            totalPutTime += data.putTime;
            totalGetTime += data.getTime;
            totalFileSize += data.fileSize;
        }

        double averagePutTime = totalPutTime / (double) dataList.size();
        double averageGetTime = totalGetTime / (double) dataList.size();
        double averageFileSize = totalFileSize / (double) dataList.size();

        System.out.println("Results for '" + tableName + "' table:");
        System.out.printf("Number of files: %d\n", dataList.size());
        System.out.printf("Average file size: %.2f bytes\n",
                          averageFileSize);
        System.out.printf("Average putRow() time: %.2f ms\n",
                          averagePutTime / 1e6);
        System.out.printf("Average getRow() time: %.2f ms\n",
                          averageGetTime / 1e6);

        System.out.println("Estimated time for operations:");

        double estimatedPutTime10k = averagePutTime * 10000;
        double estimatedGetTime10k = averageGetTime * 10000;
        System.out.printf("Estimated time for 10,000 putRow(): %.2f s\n", estimatedPutTime10k / 1e9);
        System.out.printf("Estimated time for 10,000 getRow(): %.2f s\n",
                          estimatedGetTime10k / 1e9);

        double estimatedPutTime100k = averagePutTime * 100000;
        double estimatedGetTime100k = averageGetTime * 100000;
        System.out.printf("Estimated time for 100,000 putRow(): %.2f s\n", estimatedPutTime100k / 1e9);
        System.out.printf("Estimated time for 100,000 getRow(): %.2f s\n",
                estimatedGetTime100k / 1e9);

        double estimatedPutTime1M = averagePutTime * 1000000;
        double estimatedGetTime1M = averageGetTime * 1000000;
        System.out.printf("Estimated time for 1,000,000 putRow(): %.2f s\n", estimatedPutTime1M / 1e9);
        System.out.printf("Estimated time for 1,000,000 getRow(): %.2f s\n",
                estimatedGetTime1M / 1e9);

        System.out.println();
    }

    public static void main(String args[]) throws Exception {
        run(null, args);
    }
}
