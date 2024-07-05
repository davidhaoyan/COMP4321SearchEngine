package project;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;

public class VectorSpaceModel {
    RecordManager indexRecordManager;
    HTree pageMapping;
    HTree wordMapping;
    HTree invertedIndex;
    HTree pageProperties;
    RecordManager vectorSpaceRecordManager;
    HTree documentVectors;
    HTree maxTf;
    int progress = 1;
    int N;
    VectorSpaceModel() {}

    public void openDB(String input, String output) throws IOException {
        indexRecordManager = RecordManagerFactory.createRecordManager(input);
        pageMapping = JDBMHelper.createOrLoadHTree(indexRecordManager, "PageMapping");
        wordMapping = JDBMHelper.createOrLoadHTree(indexRecordManager, "WordMapping");
        invertedIndex = JDBMHelper.createOrLoadHTree(indexRecordManager, "InvertedIndex");
        pageProperties = JDBMHelper.createOrLoadHTree(indexRecordManager, "PageProperties");

        vectorSpaceRecordManager = RecordManagerFactory.createRecordManager(output);
        documentVectors = JDBMHelper.createOrLoadHTree(vectorSpaceRecordManager, "DocumentVectors");
        maxTf = JDBMHelper.createOrLoadHTree(vectorSpaceRecordManager, "maxTf");
    }


    private Integer calculateTf(String word, String url) throws IOException {
        Integer wordId = (Integer)wordMapping.get(word);
        Integer pageId = (Integer)pageMapping.get(url);
        Map<Integer, List<Integer>> wordOccurrence = (Map<Integer, List<Integer>>)invertedIndex.get(wordId);
        if (wordOccurrence == null) {
            return 0;
        }
        if (wordOccurrence.get(pageId) == null) {
            return 0;
        } else {
            Integer tf = wordOccurrence.get(pageId).size();
            if (maxTf.get(pageId) == null) {
                maxTf.put(pageId, tf);
            } else {
                maxTf.put(pageId, Math.max((Integer) maxTf.get(pageId), tf));
            }
            return tf;
        }
    }
    private Integer calculateDf(String word) throws IOException {
        Integer wordId = (Integer)wordMapping.get(word);
        Map<Integer, List<Integer>> wordOccurrence = (Map<Integer, List<Integer>>)invertedIndex.get(wordId);
        if (wordOccurrence == null) {
            return 0;
        }
        return wordOccurrence.size();
    }
    private double calculateIdf(String word) throws IOException {
        Integer df = calculateDf(word);
        double N_df = Float.valueOf(N) / Float.valueOf(df);
        double log2N_df = Math.log(N_df) / Math.log(2);
        return log2N_df;
    }

    private double calculateWeight(String word, String url) throws IOException {
        Integer tf = calculateTf(word, url);
        double idf = calculateIdf(word);
        return tf * idf;
    }

    private void updateDocumentVectors(String url, String word, double weight) throws IOException {
        Integer pageId = (Integer)pageMapping.get(url);
        Integer wordId = (Integer)wordMapping.get(word);
        if (documentVectors.get(pageId) == null) {
            documentVectors.put(pageId, new HashMap<>(Map.ofEntries(
                    entry(wordId, weight)
            )));
        } else {
            Map<Integer, Double> existingVector = (Map<Integer, Double>) documentVectors.get(pageId);
            if (existingVector.get(wordId) == null) {
                existingVector.put(wordId, weight);
                documentVectors.put(pageId, existingVector);
            } else {
                // Entry already exists, no implementation yet
                existingVector.put(wordId, weight);
                documentVectors.put(pageId, existingVector);
            }
        }
    }

    private void normalizeDocumentVector() throws IOException {
        FastIterator documentVectorsIterator = documentVectors.keys();
        Object pageId;

        while ((pageId = documentVectorsIterator.next()) != null) {
            Map<Integer, Double> wordWeights = (Map<Integer, Double>) documentVectors.get((Integer)pageId);
            for (Integer wordId : wordWeights.keySet()) {
                Integer maxTf = (Integer) this.maxTf.get(pageId);
                wordWeights.put(wordId, wordWeights.get(wordId) / maxTf);
                documentVectors.put(pageId, wordWeights);
            }
        }
    }

    public void createVectorSpace() throws IOException {
        N = JDBMHelper.calculateSizeHTree(pageMapping);
        FastIterator pageIterator = pageMapping.keys();
        Object pageKey;
        while ((pageKey = (String)pageIterator.next()) != null) {
            System.out.println("Creating vector space model for:" + pageKey + "\nProgress: " + progress +"/" + N);
            FastIterator wordIterator = wordMapping.keys();
            Object wordKey;
            while ((wordKey = wordIterator.next()) != null) {
                Double weight = calculateWeight((String)wordKey, (String)pageKey);
                if (weight == 0) { continue; }
                updateDocumentVectors((String)pageKey, (String)wordKey, weight);
            }
            progress++;
        }
        normalizeDocumentVector();
    }

    public static void runVectorSpaceModel(String input, String output) throws IOException {
        VectorSpaceModel vsm = new VectorSpaceModel();
        vsm.openDB(input, output);
        vsm.createVectorSpace();

        //JDBMHelper.printAll(vsm.documentVectors);

        vsm.vectorSpaceRecordManager.commit();
        vsm.vectorSpaceRecordManager.close();
    }
}
