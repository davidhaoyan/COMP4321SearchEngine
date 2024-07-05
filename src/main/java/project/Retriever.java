package project;

import project.StopStem.*;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;
import org.htmlparser.util.ParserException;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

public class Retriever {
    public class RankingEntry {
        public double score;
        public int pageId;
        public String url;
        public String title;
        public String lastDateModified;
        int size;
        Map<String, Integer> keywords;
        Vector<String> children;
        Vector<String> parents;

        RankingEntry(double score, int pageId, String url, String title, String lastDateModified, int size,
                     Map<String, Integer> keywords, Vector<String> children, Vector<String> parents) {
            this.score = score;
            this.pageId = pageId;
            this.url = url;
            this.title = title;
            this.lastDateModified = lastDateModified;
            this.size = size;
            this.keywords = keywords;
            this.children = children;
            this.parents = parents;
        }
    }

    public class RankingComparator implements Comparator<RankingEntry> {
        @Override
        public int compare(RankingEntry e1, RankingEntry e2) {
            if (e1.score < e2.score) {
                return 1;
            } else if (e1.score > e2.score) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    private String _url_ = "QueryNoURL";
    private Map<Integer, Integer> queryVector = new HashMap<>();
    private PriorityQueue<RankingEntry> searchRanking = new PriorityQueue<>(new RankingComparator());
    private HTree reversePageMapping;
    private HTree reverseWordMapping;
    private HTree pageProperties;
    private HTree childrenLinks;
    private HTree parentLinks;

    Retriever() {}

    // TODO: No Porter algorithm for stems yet, just stopword removal
    private static Vector<String> extractWords(String query) {
        String[] tokens = query.split("[\\p{Punct}\\s]+");
        Vector<String> vec_tokens = new Vector<>();
        StopStem stopStem = new StopStem("stopwords.txt");
        for (int i = 0; i < tokens.length; i++) {
            if (!stopStem.isStopWord(tokens[i]))
                vec_tokens.add(tokens[i]);
        }
        return vec_tokens;
    }

    // Query vector: weight = freq of terms
    private void createQueryVector(VectorSpaceModel vsm, Vector<String> tokens) throws IOException {
        for (String token : tokens) {
            Integer wordId = (Integer) vsm.wordMapping.get(token);
            queryVector.put(wordId, queryVector.getOrDefault(wordId, 0) + 1);
        }
    }

    private double calculateCosineSimilarity(Map<Integer, Double> documentWeights, int pageSize) {
        double dot = 0;
        for (int wordId : queryVector.keySet()) {
            if (documentWeights.get(wordId) != null) {
                dot = dot + queryVector.get(wordId) * documentWeights.get(wordId);
            }
        }
        return dot / pageSize / queryVector.size();
    }

    private double titleMatchBonus(VectorSpaceModel vsm, double score, Vector<String> queryTokens, int pageId) throws IOException {
        String title = (String) ((Map)vsm.pageProperties.get(pageId)).get("title");
        Vector<String> titleTokens = Retriever.extractWords(title);
        for (String queryToken : queryTokens) {
            for (String titleToken : titleTokens) {
                if (queryToken.equals(titleToken)) {
                    score = (score + 0.5) * 1.5;
                }
            }
        }
        return score;
    }

    private Vector<String> processLinks(Vector<Integer> linkIds) throws IOException {
        Vector<String> mapped = new Vector<>();
        for (int pageId: linkIds) {
            mapped.add((String) reversePageMapping.get(pageId));
        }
        return mapped;
    }

    private void outputRanking(VectorSpaceModel vsm, Vector<String> queryTokens) throws IOException {
        FastIterator iter = vsm.documentVectors.keys();
        Object pageId;
        while ((pageId = iter.next()) != null) {
            Map p = (Map) vsm.pageProperties.get(pageId);
            int pageSize = (int) p.get("size");
            double score = calculateCosineSimilarity((Map<Integer, Double>) vsm.documentVectors.get(pageId), pageSize);
            score = titleMatchBonus(vsm, score * 10000, queryTokens, (int)pageId);
            searchRanking.add(new RankingEntry(score,
                    (Integer) pageId,
                    (String) reversePageMapping.get(pageId),
                    (String) ((Map)pageProperties.get(pageId)).get("title"),
                    (String) ((Map)pageProperties.get(pageId)).get("lastDateModified"),
                    (int) ((Map)pageProperties.get(pageId)).get("size"),
                    (Map<String, Integer>) ((Map)pageProperties.get(pageId)).get("topKeywords"),
                    (Vector<String>) processLinks((Vector<Integer>)childrenLinks.get(pageId)),
                    (Vector<String>) processLinks((Vector<Integer>)parentLinks.get(pageId))));
        }
    }

    private Vector<RankingEntry> pollRanking() throws IOException {
        int n = 1;
        Vector<RankingEntry> polled = new Vector<>();
        while (searchRanking.size() > 0 && n <= 50) {
            RankingEntry maxEntry = searchRanking.poll();
            polled.add(maxEntry);
            System.out.println("Result" + n + " url: " + (String)reversePageMapping.get(maxEntry.pageId) + "title : " + ((Map)pageProperties.get(maxEntry.pageId)).get("title")  + " score: " + maxEntry.score);
            n++;
        }
        return polled;
    }

    public static Vector<RankingEntry> runRetriever(String query) throws IOException, ParserException {
        Retriever retriever = new Retriever();
        VectorSpaceModel vsm = new VectorSpaceModel();

        vsm.openDB("crawler", "documentVectors");
        retriever.reversePageMapping = JDBMHelper.createOrLoadHTree(vsm.indexRecordManager, "ReversePageMapping");
        retriever.reverseWordMapping = JDBMHelper.createOrLoadHTree(vsm.indexRecordManager, "ReverseWordMapping");
        retriever.pageProperties = JDBMHelper.createOrLoadHTree(vsm.indexRecordManager, "PageProperties");
        retriever.childrenLinks = JDBMHelper.createOrLoadHTree(vsm.indexRecordManager, "ChildrenLinks");
        retriever.parentLinks = JDBMHelper.createOrLoadHTree(vsm.indexRecordManager, "ParentLinks");

        Vector<String> tokens = Retriever.extractWords(query);
        retriever.createQueryVector(vsm, tokens);

        retriever.outputRanking(vsm, tokens);
        Vector<RankingEntry> polledSearchRanking = retriever.pollRanking();
        return polledSearchRanking;
    }
}