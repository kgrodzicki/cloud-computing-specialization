import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;

import static java.lang.System.out;
import static java.nio.charset.Charset.forName;
import static java.nio.file.Files.newBufferedReader;
import static java.nio.file.Paths.get;
import static java.util.Arrays.asList;

public class MP1 {

    Random generator;

    String userName;

    String inputFileName;

    String delimiters = " \t,;.?!-:@[](){}_*/";

    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        String[] ret = new String[20];

        Map<String, Integer> tokens = reduceAndSort(ignoreCommonWords(tokenize(filterOutIndexes(titles()))));

        Iterator<Map.Entry<String, Integer>> iterator = tokens.entrySet().iterator();
        int idx = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> elm = iterator.next();
//            out.printf("%s %d%n", elm.getKey(), elm.getValue());
            if (idx < ret.length) {
                ret[idx++] = elm.getKey();
            }
        }

        return ret;
    }

    private List<String> filterOutIndexes(List<String> titles) throws NoSuchAlgorithmException {
        ArrayList<String> strings = new ArrayList<String>(titles);
        LinkedList<String> result = new LinkedList<String>();
        for (Integer idx : getIndexes()) {
            result.add(strings.get(idx));
        }
        return result;
    }

    private Map<String, Integer> reduceAndSort(List<String> words) {
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        for (String each : words) {
            if (!result.containsKey(each)) {
                result.put(each, 1);
            } else {
                result.put(each, result.get(each) + 1);
            }
        }

        return sort(result);
    }

    private Map<String, Integer> sort(HashMap<String, Integer> result) {
        TreeMap sorted = new TreeMap(new ValueComparator(result));
        sorted.putAll(result);
        return sorted;
    }

    private List<String> ignoreCommonWords(List<String> words) {
        List<String> result = new LinkedList<String>();
        List<String> stopWords = asList(stopWordsArray);
        for (String word : words) {
            if (!stopWords.contains(word)) {
                result.add(word);
            }
        }
        return result;
    }

    private List<String> tokenize(List<String> titles) {
        List<String> result = new LinkedList<String>();
        for (String each : titles) {
            StringTokenizer st = new StringTokenizer(each, delimiters);
            while (st.hasMoreTokens()) {
                result.add(st.nextToken().toLowerCase().trim());
            }
        }
        return result;
    }

    private List<String> titles() {
        List<String> titles = new ArrayList<String>();
        Charset charset = forName("UTF-8");
        try {
            BufferedReader reader = newBufferedReader(get(inputFileName), charset);
            String line;
            while ((line = reader.readLine()) != null) {
                titles.add(line);
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
        return titles;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            out.println("MP1 <User ID>");
        } else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item : topItems) {
                out.println(item);
            }
        }
    }

    private static class ValueComparator implements Comparator<String> {

        private Map<String, Integer> base;

        public ValueComparator(Map base) {
            this.base = base;
        }

        @Override
        public int compare(String o1, String o2) {
            if (base.get(o1) == base.get(o2)) {
                return o1.compareTo(o2);
            } else if (base.get(o1) > base.get(o2)) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}
