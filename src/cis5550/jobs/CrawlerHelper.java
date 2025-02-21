package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.jobs.Robots.RetrieveRobotsTXTResult;
import cis5550.jobs.Robots.RobotRule;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

public class CrawlerHelper {
    private static final Logger logger = Logger.getLogger(CrawlerHelper.class);

    static final Pattern anchorTagPattern = Pattern.compile("<\\s*a\\b[^>]*>", Pattern.CASE_INSENSITIVE);
    static final Pattern hrefAttributePattern = Pattern.compile("\\bhref\\s*=\\s*['\"]([^'\"]*)['\"]",
            Pattern.CASE_INSENSITIVE);
    // Note that we need to match both single and double quotes.

    private static LanguageDetector detector;
    static {
        CrawlerHelper.detector = LanguageDetector.getDefaultLanguageDetector();
        try {
            CrawlerHelper.detector.loadModels();
        } catch (IOException e) {
            logger.error("Failed to load models for tika");
            e.printStackTrace();
        }
    }

    private CrawlerHelper() {
    }

    public sealed interface SetupURLQueueResult {
        public record Ok(FlameRDD flameRDD) implements SetupURLQueueResult {
        }

        public record InvalidURLSyntax(URISyntaxException e) implements SetupURLQueueResult {
        }

        public record InvalidURL(String problem) implements SetupURLQueueResult {
        }

        public record OtherError(Exception e) implements SetupURLQueueResult {
        }
    }

    public static SetupURLQueueResult setupURLQueue(FlameContext flameContext, String[] seedURLs) {
        List<String> normalizedSeedURIs = new ArrayList<>();

        for (String seedURL : seedURLs) {
            try {
                URI normalizedSeedURI = CrawlerHelper.normalizeURL(seedURL, seedURL);
                if (!validScheme(normalizedSeedURI)) {
                    return new SetupURLQueueResult.InvalidURL(
                            "Scheme was " + normalizedSeedURI.getScheme()
                                    + ", but supposed to be either http or https");
                } else if (!validPort(normalizedSeedURI)) {
                    return new SetupURLQueueResult.InvalidURL(normalizedSeedURI.getPort() + " is not a valid port");
                } else if (!validHost(normalizedSeedURI)) {
                    return new SetupURLQueueResult.InvalidURL(normalizedSeedURI.getHost() + " is not a valid host");
                } else if (!validPath(normalizedSeedURI)) {
                    return new SetupURLQueueResult.InvalidURL(normalizedSeedURI.getPath() + " is not a valid path");
                }
                normalizedSeedURIs.add(normalizedSeedURI.toString());
            } catch (URISyntaxException e) {
                return new SetupURLQueueResult.InvalidURLSyntax(e);
            }
        }

        try {
            FlameRDD urlQueue = flameContext.parallelize(normalizedSeedURIs);
            return new SetupURLQueueResult.Ok(urlQueue);
        } catch (Exception e) {
            return new SetupURLQueueResult.OtherError(e);
        }
    }

    public static List<Pattern> loadBlacklist(FlameContext flameContext, String blacklistTable) {
        List<Pattern> blacklistURLPatterns = new ArrayList<>();

        try {
            Iterator<Row> rows = flameContext.getKVS().scan(blacklistTable);
            while (rows.hasNext()) {
                Row row = rows.next();
                String givenPattern = row.get("pattern");
                if (givenPattern != null) {
                    String stringPattern = Pattern.quote(givenPattern).replace("\\*", ".*");
                    blacklistURLPatterns.add(Pattern.compile(stringPattern));
                }
            }
        } catch (IOException e) {
            logger.info("Encountered an IOException when loading the blacklist");
            e.printStackTrace();
        }

        return blacklistURLPatterns;
    }

    public static StringToIterable generateURLProcessor(String coordinatorArg, List<Pattern> blacklistedURLPatterns) {
        return batchString -> {
            List<String> urls = Arrays.stream(batchString.split("\\|"))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            if (urls.isEmpty()) {
                logger.info("Empty url batch was pased in to the lambda" + batchString);
                return List.of();
            }

            ExecutorService fetchExecutor = Executors.newFixedThreadPool(20);
            try {
                KVSClient kvs = new KVSClient(coordinatorArg);

                List<CompletableFuture<List<String>>> futures = new ArrayList<>();
                for (String urlString : urls) {
                    CompletableFuture<List<String>> future = CompletableFuture.supplyAsync(() -> {
                        return processSingleURL(urlString, kvs, blacklistedURLPatterns);
                    }, fetchExecutor);

                    futures.add(future);
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                List<String> allDiscoveredURLs = futures.stream()
                        .flatMap(f -> f.join().stream())
                        .toList();

                logger.info("Finished processing a url batch");
                return batchURLs(allDiscoveredURLs, 100);

            } finally {
                fetchExecutor.shutdown();
            }
        };
    }

    public static List<String> processSingleURL(String urlString, KVSClient kvs, List<Pattern> blacklistedURLPatterns) {
        try {
            logger.debug("\n\nProcessing: " + urlString);

            if (kvs.existsRow("pt-crawl", Hasher.hash(urlString))) {
                logger.debug("Stopped processing " + urlString + " since this url was already processed");
                return List.of();
            }

            for (Pattern blacklisted : blacklistedURLPatterns) {
                if (blacklisted.matcher(urlString).matches()) {
                    logger.debug("Stopped processing " + urlString + " since this url matched the blacklist pattern: "
                            + blacklisted.toString());
                    return List.of();
                }
            }

            String urlHost = new URI(urlString).getHost();
            String urlPath = new URI(urlString).getPath();
            URL url = new URL(urlString);

            List<RobotRule> robotRules;
            Row hostRobotsTXTRow = kvs.getRow("pt-hostRobotsTXT", Hasher.hash(urlHost));
            if (hostRobotsTXTRow == null) {
                hostRobotsTXTRow = new Row(Hasher.hash(urlHost));

                String robotsTXTText = "";
                RetrieveRobotsTXTResult result = Robots.retrieveRobotsTXT(url.getProtocol(), url.getHost(),
                        url.getPort());
                if (result instanceof RetrieveRobotsTXTResult.Ok ok) {
                    robotsTXTText = ok.robotsTXT();
                } else if (result instanceof RetrieveRobotsTXTResult.NotPresent) {
                    robotsTXTText = "";
                } else if (result instanceof RetrieveRobotsTXTResult.OtherError e) {
                    logger.info("Encountered an error when retrieving robots.txt for " + url);
                    e.e().printStackTrace();
                    robotsTXTText = "";
                } else {
                    throw new IllegalStateException("Broken RetrieveRobotsTXTResult matching");
                }

                robotRules = Robots.parseRobotsTXT(robotsTXTText, "cis5550-crawler");
                byte[] robotsTXTBinary = Robots.serializeRobotRules(robotRules);

                hostRobotsTXTRow.put("robotsTXTText", robotsTXTText);
                hostRobotsTXTRow.put("robotsTXTBinary", robotsTXTBinary);
                kvs.putRow("pt-hostRobotsTXT", hostRobotsTXTRow);

            } else {
                robotRules = Robots.deserializeRobotRules(hostRobotsTXTRow.getBytes("robotsTXTBinary"));
            }

            boolean urlAllowed = Robots.urlAllowed(robotRules, urlPath);
            if (!urlAllowed) {
                logger.debug("Stopped processing " + urlString + " due to the robots policy");
                return List.of();
            }

            double crawlDelay = Robots.getHostCrawlDelay(robotRules);
            long crawlDelayMilliseconds = (long) (crawlDelay * 1000);
            if (kvs.existsRow("hostAccessTime", Hasher.hash(urlHost))) {
                Row hostAccessTimeRow = kvs.getRow("hostAccessTime", Hasher.hash(urlHost));
                if (hostAccessTimeRow.columns().contains("time")) {
                    Instant lastRequest = Instant.ofEpochMilli(Long.parseLong(hostAccessTimeRow.get("time")));

                    if (Duration.between(lastRequest, Instant.now())
                            .compareTo(Duration.ofMillis(crawlDelayMilliseconds)) < 0) {
                        logger.debug("Stopped processing " + urlString + " since this host was accessed too recently");
                        return List.of(urlString);
                    }
                }
            }
            // Prevent multiple threads from clearing the time check at the same time.
            kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" + Instant.now().toEpochMilli());

            CrawlURLResult result = CrawlerHelper.crawlURL(urlString);
            if (result instanceof CrawlURLResult.Ok200 ok200) {
                Row crawlRow = new Row(Hasher.hash(urlString));
                crawlRow.put("url", urlString);
                crawlRow.put("contentType", ok200.contentType());
                crawlRow.put("length", ok200.length());
                crawlRow.put("responseCode", ok200.responseCode());

                Document jsoupDocument = ok200.htmlDocument();
                jsoupDocument.select("script, style, img, noscript, iframe, form, button, svg").remove();
                String visibleText = jsoupDocument.text();

                String lang = jsoupDocument.select("html").attr("lang");
                if (!lang.isEmpty() && !lang.startsWith("en")) {
                    logger.info(String.format("Page says its not English. Not crawling |%s|", urlString));
                    return List.of();
                }

                LanguageResult languageResult = detector.detect(visibleText);
                if (!languageResult.isLanguage("en")) {
                    logger.info(String.format("Tika detected non-English webpage. Not crawling |%s|", urlString));
                    return List.of();
                }

                String title = jsoupDocument.title();
                crawlRow.put("title", title.getBytes());

                // Adding simple word count
                String[] words = visibleText.split("\\s+");
                int wordCount = words.length;
                crawlRow.put("wordCount", String.valueOf(wordCount));

                String contentHash = Hasher.hash(visibleText);
                List<String> validNextURLs = new ArrayList<>();
                if (kvs.existsRow("pt-contentHashes", contentHash)) {
                    Row contentHashRow = kvs.getRow("pt-contentHashes", contentHash);
                    String canonicalURL = contentHashRow.get("canonicalURL");
                    if (canonicalURL.equals(urlString)) {
                        logger.info("URL Duplicated Detected");
                        kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" + Instant.now().toEpochMilli());
                        return List.of();
                    }
                    crawlRow.put("canonicalURL", canonicalURL);
                } else {
                    Row contentHashRow = new Row(contentHash);
                    contentHashRow.put("canonicalURL", urlString);
                    kvs.putRow("pt-contentHashes", contentHashRow);
                    crawlRow.put("page", visibleText);

                    List<URI> extractedURIs = extractURIs(jsoupDocument);
                    URI referenceURI = URI.create(urlString);

                    for (URI extractedURI : extractedURIs) {
                        URI resolved = referenceURI.resolve(extractedURI);
                        if (CrawlerHelper.validScheme(resolved)) {
                            String scheme = resolved.getScheme();
                            String hostAndPort = resolved.getAuthority();
                            String path = resolved.getPath();
                            String query = resolved.getQuery();

                            if (path.equals("")) {
                                path = "/";
                            }

                            URI normalizedURI = (new URI(scheme, hostAndPort, path, query, null)).normalize();
                            if (CrawlerHelper.validPath(normalizedURI)) {
                                validNextURLs.add(normalizedURI.toString());
                            } else {
                                logger.debug(String.format("Normalized uri |%s| is not a useful file",
                                        normalizedURI.toString()));
                            }
                        } else {
                            logger.debug(String.format("Extracted uri |%s| does not have a useful scheme",
                                    extractedURI.toString()));
                        }
                    }
                }
                String outboundURLs = String.join(";", validNextURLs);
                crawlRow.put("outbound", outboundURLs);

                kvs.putRow("pt-crawl", crawlRow);
                kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" + Instant.now().toEpochMilli());
                logger.info("Successfully processed " + urlString);
                return validNextURLs;

            } else if (result instanceof CrawlURLResult.OkRedirect okRedirect) {
                Row crawlRow = new Row(Hasher.hash(urlString));
                crawlRow.put("url", urlString);
                crawlRow.put("contentType", okRedirect.contentType());
                crawlRow.put("length", okRedirect.length());
                crawlRow.put("responseCode", okRedirect.responseCode());

                kvs.putRow("pt-crawl", crawlRow);
                kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" + Instant.now().toEpochMilli());
                logger.info("Successfully processed " + urlString);

                String redirectURL = okRedirect.redirect();
                try {
                    URI normalizedRedirectURL = normalizeURL(urlString, redirectURL);
                    if (CrawlerHelper.validScheme(normalizedRedirectURL)
                            && CrawlerHelper.validPath(normalizedRedirectURL)) {
                        return List.of(normalizedRedirectURL.toString());
                    } else {
                        logger.info("Encountered a bad redirect url " + normalizedRedirectURL);
                        return List.of();
                    }
                } catch (URISyntaxException e) {
                    logger.info("Encountered a bad syntax redirect url " + redirectURL);
                    return List.of();
                }

            } else if (result instanceof CrawlURLResult.OkOther okOther) {
                Row crawlRow = new Row(Hasher.hash(urlString));
                crawlRow.put("url", urlString);
                crawlRow.put("contentType", okOther.contentType());
                crawlRow.put("length", okOther.length());
                crawlRow.put("responseCode", okOther.responseCode());

                kvs.putRow("pt-crawl", crawlRow);
                kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" + Instant.now().toEpochMilli());
                logger.info("Successfully processed " + urlString);
                return List.of();

            } else if (result instanceof CrawlURLResult.Timeout) {
                logger.info(String.format("Timeout occurred while processing: |%s|", urlString));
                return List.of();

            } else if (result instanceof CrawlURLResult.MalformedURL malformed) {
                logger.error(String.format("Malformed URL made it to processing: |%s|", urlString));
                malformed.e().printStackTrace();
                return List.of();

            } else if (result instanceof CrawlURLResult.OtherError otherError) {
                logger.error(String.format("Unexpected error while processing: |%s|", urlString));
                otherError.e().printStackTrace();
                return List.of();

            } else {
                throw new IllegalStateException("Broken CrawlURLResult matching");
            }

        } catch (Exception e) {
            logger.info("Encountered an exception when processing " + urlString);
            logger.info("Main loop continuing");
            e.printStackTrace();
            return List.of();
        }
    }

    private static List<String> batchURLs(List<String> urls, int batchSize) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < urls.size(); i += batchSize) {
            int end = Math.min(urls.size(), i + batchSize);
            List<String> batch = urls.subList(i, end);
            result.add(String.join("|", batch));
        }
        return result;
    }

    /*
     * public static StringToIterable generateURLProcessor(String coordinatorArg,
     * List<Pattern> blacklistedURLPatterns) {
     * return urlString -> {
     * try {
     * logger.debug("\n\nProcessing: " + urlString);
     * KVSClient kvs = new KVSClient(coordinatorArg);
     * 
     * if (kvs.existsRow("pt-crawl", Hasher.hash(urlString))) {
     * logger.debug("Stopped processing " + urlString +
     * " since this url was already processed");
     * return List.of();
     * }
     * 
     * for (Pattern blacklisted : blacklistedURLPatterns) {
     * if (blacklisted.matcher(urlString).matches()) {
     * logger.debug("Stopped processing " + urlString +
     * " since this url matched the blacklist pattern: " + blacklisted.toString());
     * return List.of();
     * }
     * }
     * 
     * String urlHost = new URI(urlString).getHost();
     * String urlPath = new URI(urlString).getPath();
     * URL url = new URL(urlString);
     * 
     * List<RobotRule> robotRules;
     * Row hostRobotsTXTRow = kvs.getRow("pt-hostRobotsTXT", Hasher.hash(urlHost));
     * if (hostRobotsTXTRow == null) {
     * hostRobotsTXTRow = new Row(Hasher.hash(urlHost));
     * 
     * String robotsTXTText = "";
     * RetrieveRobotsTXTResult result = Robots.retrieveRobotsTXT(url.getProtocol(),
     * url.getHost(),
     * url.getPort());
     * if (result instanceof RetrieveRobotsTXTResult.Ok ok) {
     * robotsTXTText = ok.robotsTXT();
     * } else if (result instanceof RetrieveRobotsTXTResult.NotPresent) {
     * robotsTXTText = "";
     * } else if (result instanceof RetrieveRobotsTXTResult.OtherError e) {
     * logger.info("Encountered an error when retrieving robots.txt for " + url);
     * e.e().printStackTrace();
     * robotsTXTText = "";
     * } else {
     * throw new IllegalStateException("Broken RetrieveRobotsTXTResult matching");
     * }
     * 
     * robotRules = Robots.parseRobotsTXT(robotsTXTText, "cis5550-crawler");
     * byte[] robotsTXTBinary = Robots.serializeRobotRules(robotRules);
     * 
     * hostRobotsTXTRow.put("robotsTXTText", robotsTXTText);
     * hostRobotsTXTRow.put("robotsTXTBinary", robotsTXTBinary);
     * kvs.putRow("pt-hostRobotsTXT", hostRobotsTXTRow);
     * 
     * } else {
     * robotRules =
     * Robots.deserializeRobotRules(hostRobotsTXTRow.getBytes("robotsTXTBinary"));
     * }
     * 
     * boolean urlAllowed = Robots.urlAllowed(robotRules, urlPath);
     * if (!urlAllowed) {
     * logger.debug("Stopped processing " + urlString +
     * " due to the robots policy");
     * return List.of();
     * }
     * 
     * double crawlDelay = Robots.getHostCrawlDelay(robotRules);
     * long crawlDelayMilliseconds = (long) (crawlDelay * 1000);
     * if (kvs.existsRow("hostAccessTime", Hasher.hash(urlHost))) {
     * Row hostAccessTimeRow = kvs.getRow("hostAccessTime", Hasher.hash(urlHost));
     * if (hostAccessTimeRow.columns().contains("time")) {
     * Instant lastRequest =
     * Instant.ofEpochMilli(Long.parseLong(hostAccessTimeRow.get("time")));
     * 
     * if (Duration.between(lastRequest, Instant.now())
     * .compareTo(Duration.ofMillis(crawlDelayMilliseconds)) < 0) {
     * logger.debug("Stopped processing " + urlString +
     * " since this host was accessed too recently");
     * return List.of(urlString);
     * }
     * }
     * }
     * // Prevent multiple threads from clearing the time check at the same time.
     * kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" +
     * Instant.now().toEpochMilli());
     * 
     * CrawlURLResult result = CrawlerHelper.crawlURL(urlString);
     * if (result instanceof CrawlURLResult.Ok200 ok200) {
     * Row crawlRow = new Row(Hasher.hash(urlString));
     * crawlRow.put("url", urlString);
     * crawlRow.put("contentType", ok200.contentType());
     * crawlRow.put("length", ok200.length());
     * crawlRow.put("responseCode", ok200.responseCode());
     * 
     * byte[] page = ok200.body();
     * 
     * // Adding simple word count
     * String pageContent = new String(page);
     * String[] words = pageContent.split("\\s+");
     * int wordCount = words.length;
     * crawlRow.put("wordCount", String.valueOf(wordCount));
     * 
     * String contentHash = Hasher.hash(new String(page));
     * List<String> validNextURLs = new ArrayList<>();
     * if (kvs.existsRow("pt-contentHashes", contentHash)) {
     * Row contentHashRow = kvs.getRow("pt-contentHashes", contentHash);
     * String canonicalURL = contentHashRow.get("canonicalURL");
     * if (canonicalURL.equals(urlString)) {
     * logger.info("URL Duplicated Detected");
     * kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" +
     * Instant.now().toEpochMilli());
     * return List.of();
     * }
     * crawlRow.put("canonicalURL", canonicalURL);
     * } else {
     * Row contentHashRow = new Row(contentHash);
     * contentHashRow.put("canonicalURL", urlString);
     * kvs.putRow("pt-contentHashes", contentHashRow);
     * crawlRow.put("page", page);
     * 
     * List<URI> extractedURIs = extractURIs(page);
     * URI referenceURI = URI.create(urlString);
     * 
     * for (URI extractedURI : extractedURIs) {
     * URI resolved = referenceURI.resolve(extractedURI);
     * if (CrawlerHelper.validScheme(resolved)) {
     * String scheme = resolved.getScheme();
     * String hostAndPort = resolved.getAuthority();
     * String path = resolved.getPath();
     * String query = resolved.getQuery();
     * 
     * if (path.equals("")) {
     * path = "/";
     * }
     * 
     * URI normalizedURI = (new URI(scheme, hostAndPort, path, query,
     * null)).normalize();
     * if (CrawlerHelper.validPath(normalizedURI)) {
     * validNextURLs.add(normalizedURI.toString());
     * } else {
     * logger.info(String.format("Normalized uri |%s| is not a useful file",
     * normalizedURI.toString()));
     * }
     * } else {
     * logger.info(String.format("Extracted uri |%s| does not have a useful scheme",
     * extractedURI.toString()));
     * }
     * }
     * }
     * kvs.putRow("pt-crawl", crawlRow);
     * kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" +
     * Instant.now().toEpochMilli());
     * logger.info("Successfully processed " + urlString);
     * return validNextURLs;
     * 
     * } else if (result instanceof CrawlURLResult.OkRedirect okRedirect) {
     * Row crawlRow = new Row(Hasher.hash(urlString));
     * crawlRow.put("url", urlString);
     * crawlRow.put("contentType", okRedirect.contentType());
     * crawlRow.put("length", okRedirect.length());
     * crawlRow.put("responseCode", okRedirect.responseCode());
     * 
     * kvs.putRow("pt-crawl", crawlRow);
     * kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" +
     * Instant.now().toEpochMilli());
     * logger.info("Successfully processed " + urlString);
     * 
     * String redirectURL = okRedirect.redirect();
     * try {
     * URI normalizedRedirectURL = normalizeURL(urlString, redirectURL);
     * if (CrawlerHelper.validScheme(normalizedRedirectURL)
     * && CrawlerHelper.validPath(normalizedRedirectURL)) {
     * return List.of(normalizedRedirectURL.toString());
     * } else {
     * logger.info("Encountered a bad redirect url " + normalizedRedirectURL);
     * return List.of();
     * }
     * } catch (URISyntaxException e) {
     * logger.info("Encountered a bad syntax redirect url " + redirectURL);
     * return List.of();
     * }
     * 
     * } else if (result instanceof CrawlURLResult.OkOther okOther) {
     * Row crawlRow = new Row(Hasher.hash(urlString));
     * crawlRow.put("url", urlString);
     * crawlRow.put("contentType", okOther.contentType());
     * crawlRow.put("length", okOther.length());
     * crawlRow.put("responseCode", okOther.responseCode());
     * 
     * kvs.putRow("pt-crawl", crawlRow);
     * kvs.put("hostAccessTime", Hasher.hash(urlHost), "time", "" +
     * Instant.now().toEpochMilli());
     * logger.info("Successfully processed " + urlString);
     * return List.of();
     * 
     * } else if (result instanceof CrawlURLResult.Timeout) {
     * logger.info(String.format("Timeout occurred while processing: |%s|",
     * urlString));
     * return List.of();
     * 
     * } else if (result instanceof CrawlURLResult.MalformedURL malformed) {
     * logger.error(String.format("Malformed URL made it to processing: |%s|",
     * urlString));
     * malformed.e().printStackTrace();
     * return List.of();
     * 
     * } else if (result instanceof CrawlURLResult.OtherError otherError) {
     * logger.error(String.format("Unexpected error while processing: |%s|",
     * urlString));
     * otherError.e().printStackTrace();
     * return List.of();
     * 
     * } else {
     * throw new IllegalStateException("Broken CrawlURLResult matching");
     * }
     * 
     * } catch (Exception e) {
     * logger.info("Encountered an exception when processing " + urlString);
     * logger.info("Main loop continuing");
     * e.printStackTrace();
     * return List.of();
     * }
     * };
     * 
     * }
     */

    public sealed interface CrawlURLResult {
        public record Ok200(String contentType, String length, String responseCode, Document htmlDocument)
                implements CrawlURLResult {
        }

        public record OkRedirect(String contentType, String length, String responseCode, String redirect)
                implements CrawlURLResult {
        }

        public record OkOther(String contentType, String length, String responseCode) implements CrawlURLResult {
        }

        public record MalformedURL(MalformedURLException e) implements CrawlURLResult {
        }

        public record Timeout() implements CrawlURLResult {
        }

        public record OtherError(Exception e) implements CrawlURLResult {
        }
    }

    public static CrawlURLResult crawlURL(String urlString) {
        try {
            URL url = new URL(urlString);

            HttpURLConnection headConnection = (HttpURLConnection) url.openConnection();
            headConnection.setInstanceFollowRedirects(false);
            headConnection.setRequestMethod("HEAD");
            headConnection.setRequestProperty("User-Agent", "cis5550-crawler");
            headConnection.setConnectTimeout(1000);
            headConnection.setReadTimeout(3000);
            headConnection.connect();

            String contentType = headConnection.getContentType() == null ? "" : headConnection.getContentType();
            String lengthString = "" + headConnection.getContentLength();
            int responseCode = headConnection.getResponseCode();
            String responseCodeString = "" + responseCode;

            if (responseCode == 200 && headConnection.getContentType().toLowerCase().startsWith("text/html")) {
                HttpURLConnection getConnection = (HttpURLConnection) url.openConnection();
                getConnection.setInstanceFollowRedirects(false);
                getConnection.setRequestMethod("GET");
                getConnection.setRequestProperty("User-Agent", "cis5550-crawler");
                getConnection.setConnectTimeout(1000);
                getConnection.setReadTimeout(3000);
                getConnection.connect();

                int getResponseCode = getConnection.getResponseCode();
                responseCodeString = "" + getResponseCode;
                if (getResponseCode == 200) {
                    Document jsoupDocument = Jsoup.parse(getConnection.getInputStream(), null, urlString);
                    /*
                     * byte[] body = getConnection.getInputStream().readAllBytes();
                     * String htmlBody = new String(body, StandardCharsets.UTF_8);
                     * Document htmlDocument = Jsoup.parse(htmlBody);
                     */
                    return new CrawlURLResult.Ok200(contentType, lengthString, responseCodeString, jsoupDocument);

                }

            } else if (responseCode == 301 || responseCode == 302 ||
                    responseCode == 303 || responseCode == 307 || responseCode == 308) {
                String redirectURL = headConnection.getHeaderField("Location");
                if (redirectURL != null) {
                    return new CrawlURLResult.OkRedirect(contentType, lengthString, responseCodeString, redirectURL);
                }

            }
            return new CrawlURLResult.OkOther(contentType, lengthString, responseCodeString);
        } catch (SocketTimeoutException e) {
            return new CrawlURLResult.Timeout();
        } catch (MalformedURLException e) {
            return new CrawlURLResult.MalformedURL(e);
        } catch (ProtocolException e) {
            return new CrawlURLResult.OtherError(e);
        } catch (IOException e) {
            return new CrawlURLResult.OtherError(e);
        }
    }

    private static byte[] ParseDocumentBody(byte[] body) throws IOException {
        String htmlContent = new String(body, StandardCharsets.UTF_8);
        Document doc = Jsoup.parse(htmlContent);

        // Retain only English Text

        try {
            LanguageDetector detector = LanguageDetector.getDefaultLanguageDetector();
            detector.loadModels();
            // Match non-whitespace text
            Elements textElements = doc.select("*:matchesOwn(\\S)");

            for (Element element : textElements) {
                String text = element.ownText();
                if (!text.isEmpty()) {
                    LanguageResult result = detector.detect(text);
                    if (!"en".equals(result.getLanguage())) {
                        element.remove();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Language detection model failed to load");
        }

        // Remove all JavaScript/CSS
        doc.select("script, style").remove();
        // Extra tags to be removed
        doc.select("nav, header, footer").remove();
        return doc.text().getBytes(StandardCharsets.UTF_8);
    }

    public static boolean validScheme(URI testURI) {
        String scheme = testURI.getScheme();
        return scheme != null && (scheme.equals("http") || scheme.equals("https"));
    }

    public static boolean validPort(URI testURI) {
        int port = testURI.getPort();
        return port > 0;
    }

    public static boolean validHost(URI testURI) {
        String host = testURI.getHost();
        return host != null;
    }

    public static boolean validPath(URI testURI) {
        String path = testURI.getPath();
        return path != null
                && !path.endsWith(".jpg") && !path.endsWith(".jpeg") && !path.endsWith(".gif")
                && !path.endsWith(".png") && !path.endsWith(".txt") && !path.endsWith(".pdf")
                && !path.endsWith(".epub") && !path.endsWith(".chm") && !path.endsWith(".css")
                && !path.endsWith(".js") && !path.endsWith(".svg");
    }

    public static List<URI> extractURIs(Document jsoupDocument) {
        List<URI> extractedURIs = new ArrayList<>();

        // Check for no follow in meta tag
        Element metaTag = jsoupDocument.selectFirst("meta[name=robots]");
        if (metaTag != null && metaTag.attr("content").contains("nofollow")) {
            logger.info("Found a meta[name=robots] nofollow tag");
            return extractedURIs;
        }

        for (Element link : jsoupDocument.select("a[href]")) {
            String extractedURIString = link.attr("href");
            try {
                URI extractedURI = new URI(extractedURIString);
                extractedURIs.add(extractedURI);
            } catch (URISyntaxException e) {
                logger.info(
                        String.format("Bad URI syntax |%s| extracted from |%s|", extractedURIString, link.toString()));
            }
        }

        return extractedURIs;
    }

    public static URI normalizeURL(String referenceURL, String url) throws URISyntaxException {
        URI referenceURI = new URI(referenceURL);
        URI urlURI = new URI(url);

        URI resolved = referenceURI.resolve(urlURI);

        String authority = resolved.getHost();
        int port = resolved.getPort();
        if (port == -1) {
            String scheme = resolved.getScheme();
            if (scheme.equals("http")) {
                authority += ":80";
            } else if (scheme.equals("https")) {
                authority += ":443";
            }
        } else {
            authority = authority + ":" + port;
        }

        String path = resolved.getPath();
        if (path == null || path.equals("")) {
            path = "/";
        }

        // Strips the # fragment from the URI
        resolved = new URI(resolved.getScheme(), authority, path, resolved.getQuery(), null);

        return resolved.normalize();
    }

    public static List<String> extractURLs(byte[] body) {
        List<String> extractedURLs = new ArrayList<>();
        String bodyString = new String(body);

        Matcher anchorTagMatcher = CrawlerHelper.anchorTagPattern.matcher(bodyString);
        while (anchorTagMatcher.find()) {
            String anchorTag = anchorTagMatcher.group();

            Matcher hrefAttributeMatcher = CrawlerHelper.hrefAttributePattern.matcher(anchorTag);
            if (hrefAttributeMatcher.find()) {
                String extractURL = hrefAttributeMatcher.group(1);
                extractedURLs.add(extractURL);
            }
        }

        return extractedURLs;
    }
}