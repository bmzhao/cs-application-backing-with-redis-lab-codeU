package com.flatironschool.javacs;

import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Represents a Redis-backed web search index.
 */
public class JedisIndex {
    /**
     * urlset maps from a word to the set of urls that it appears in
     * termcounter maps from a word within a url to the number of times it appears within that url's page
     */

    /**
     * to index a url, parse through all of its text, and create a termcounter associated with it
     * as well as update the urlset of any text inside it
     */

    private Jedis jedis;

    /**
     * Constructor.
     *
     * @param jedis
     */
    public JedisIndex(Jedis jedis) {
        this.jedis = jedis;
    }

    /**
     * Returns the Redis key for a given search term.
     *
     * @return Redis key.
     */
    private String urlSetKey(String term) {
        return "URLSet:" + term;
    }

    /**
     * Returns the Redis key for a URL's TermCounter.
     *
     * @return Redis key.
     */
    private String termCounterKey(String url) {
        return "TermCounter:" + url;
    }

    /**
     * Checks whether we have a TermCounter for a given URL.
     *
     * @param url
     * @return
     */
    public boolean isIndexed(String url) {
        String redisKey = termCounterKey(url);
        return jedis.exists(redisKey);
    }

    /**
     * Looks up a search term and returns a set of URLs.
     *
     * @param term
     * @return Set of URLs.
     */
    public Set<String> getURLs(String term) {
        return jedis.smembers(urlSetKey(term));
    }

    /**
     * Looks up a term and returns a map from URL to count.
     *
     * @param term
     * @return Map from URL to count.
     */
    public Map<String, Integer> getCounts(String term) {
        /**
         * get the urls associated with the term,
         * then get the actual values from the termcounter
         * currently using increment by 0 as hack to get the number stored
         */

        Set<String> urls = jedis.smembers(urlSetKey(term));
        Map<String, Integer> result = new HashMap<>();
        for (String url : urls) {
            result.put(url, jedis.hincrBy(termCounterKey(url), term, 0).intValue());
        }
        // FILL THIS IN!
        return result;
    }

    /**
     * Returns the number of times the given term appears at the given URL.
     *
     * @param url
     * @param term
     * @return
     */
    public Integer getCount(String url, String term) {
        // FILL THIS IN!
        return jedis.hincrBy(termCounterKey(url), term, 0).intValue();
    }


    /**
     * Add a page to the index.
     *
     * @param url        URL of the page.
     * @param paragraphs Collection of elements that should be indexed.
     */
    public void indexPage(String url, Elements paragraphs) {
        Map<String, Integer> wordsToCounts = processElements(paragraphs);
        Transaction transaction = jedis.multi();
        for (String word : wordsToCounts.keySet()) {
            transaction.sadd(urlSetKey(word),url);
            transaction.hincrBy(termCounterKey(url), word, wordsToCounts.get(word));
        }
        transaction.exec();
    }

    /**
     * Takes a collection of Elements and counts their words.
     *
     * @param paragraphs
     */
    public Map<String,Integer> processElements(Elements paragraphs) {
        Map<String, Integer> wordsToCounts = new HashMap<>();
        for (Node node: paragraphs) {
            processTree(node, wordsToCounts);
        }
        return wordsToCounts;
    }

    /**
     * Finds TextNodes in a DOM tree and counts their words.
     *
     * @param root
     */
    public void processTree(Node root, Map<String,Integer> wordsToCounts) {
        // NOTE: we could use select to find the TextNodes, but since
        // we already have a tree iterator, let's use it.
        for (Node node: new WikiNodeIterable(root)) {
            if (node instanceof TextNode) {
                processText(((TextNode) node).text(), wordsToCounts);
            }
        }
    }

    /**
     * Splits `text` into words and counts them.
     *
     * @param text  The text to process.
     */
    public void processText(String text, Map<String,Integer> wordsToCounts) {
        // replace punctuation with spaces, convert to lower case, and split on whitespace
        String[] array = text.replaceAll("\\pP", " ").toLowerCase().split("\\s+");

        for (int i=0; i<array.length; i++) {
            String term = array[i];
            if (!wordsToCounts.containsKey(term)) {
                wordsToCounts.put(term, 1);
            } else {
                wordsToCounts.put(term, wordsToCounts.get(term)+1);
            }
        }
    }

    /**
     * Prints the contents of the index.
     * <p>
     * Should be used for development and testing, not production.
     */
    public void printIndex() {
        // loop through the search terms
        for (String term : termSet()) {
            System.out.println(term);

            // for each term, print the pages where it appears
            Set<String> urls = getURLs(term);
            for (String url : urls) {
                Integer count = getCount(url, term);
                System.out.println("    " + url + " " + count);
            }
        }
    }

    /**
     * Returns the set of terms that have been indexed.
     * <p>
     * Should be used for development and testing, not production.
     *
     * @return
     */
    public Set<String> termSet() {
        Set<String> keys = urlSetKeys();
        Set<String> terms = new HashSet<String>();
        for (String key : keys) {
            String[] array = key.split(":");
            if (array.length < 2) {
                terms.add("");
            } else {
                terms.add(array[1]);
            }
        }
        return terms;
    }

    /**
     * Returns URLSet keys for the terms that have been indexed.
     * <p>
     * Should be used for development and testing, not production.
     *
     * @return
     */
    public Set<String> urlSetKeys() {
        return jedis.keys("URLSet:*");
    }

    /**
     * Returns TermCounter keys for the URLS that have been indexed.
     * <p>
     * Should be used for development and testing, not production.
     *
     * @return
     */
    public Set<String> termCounterKeys() {
        return jedis.keys("TermCounter:*");
    }

    /**
     * Deletes all URLSet objects from the database.
     * <p>
     * Should be used for development and testing, not production.
     *
     * @return
     */
    public void deleteURLSets() {
        Set<String> keys = urlSetKeys();
        Transaction t = jedis.multi();
        for (String key : keys) {
            t.del(key);
        }
        t.exec();
    }

    /**
     * Deletes all URLSet objects from the database.
     * <p>
     * Should be used for development and testing, not production.
     *
     * @return
     */
    public void deleteTermCounters() {
        Set<String> keys = termCounterKeys();
        Transaction t = jedis.multi();
        for (String key : keys) {
            t.del(key);
        }
        t.exec();
    }

    /**
     * Deletes all keys from the database.
     * <p>
     * Should be used for development and testing, not production.
     *
     * @return
     */
    public void deleteAllKeys() {
        Set<String> keys = jedis.keys("*");
        Transaction t = jedis.multi();
        for (String key : keys) {
            t.del(key);
        }
        t.exec();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Jedis jedis = JedisMaker.make();
        JedisIndex index = new JedisIndex(jedis);

        index.deleteTermCounters();
        index.deleteURLSets();
        index.deleteAllKeys();
//        loadIndex(index);

//        Map<String, Integer> map = index.getCounts("the");
//        for (Entry<String, Integer> entry : map.entrySet()) {
//            System.out.println(entry);
//        }
    }

    /**
     * Stores two pages in the index for testing purposes.
     *
     * @return
     * @throws IOException
     */
    private static void loadIndex(JedisIndex index) throws IOException {
        WikiFetcher wf = new WikiFetcher();

        String url = "https://en.wikipedia.org/wiki/Java_(programming_language)";
        Elements paragraphs = wf.readWikipedia(url);
        index.indexPage(url, paragraphs);

        url = "https://en.wikipedia.org/wiki/Programming_language";
        paragraphs = wf.readWikipedia(url);
        index.indexPage(url, paragraphs);
    }
}
