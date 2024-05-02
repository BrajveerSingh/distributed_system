package distributed.system.tfidf;

import distributed.system.tfidf.model.DocumentData;
import distributed.system.tfidf.search.TFIDF;

import javax.swing.text.Document;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SequentialSearch {
    private static final String BOOKS_DIRECTORY = "./resources/books";
    private static final String SEARCH_QUERY_1 = "The best detective that catches many criminals using his detective methods";
    private static final String SEARCH_QUERY_2 = "The girl that falls through a rabbit hole into a fantasy wonderland";
    private static final String SEARCH_QUERY_3 = "A war between Russia and France in the cold winter";

    public static void main(String[] args) throws FileNotFoundException {
        File documentsDirectory = new File(BOOKS_DIRECTORY);
        final var documents = Arrays.asList(documentsDirectory.list())
                .stream()
                .map(docName -> BOOKS_DIRECTORY + "/" + docName)
                .collect(Collectors.toList());
        final var terms = TFIDF.getWordsFromLine(SEARCH_QUERY_1);
        findMostRelevantDocuments(documents, terms);
    }

    private static void findMostRelevantDocuments(final List<String> documents,
                                                  final List<String> terms) throws FileNotFoundException {
        Map<String, DocumentData> documentDataMap = new HashMap<>();
        for(String document : documents){
            BufferedReader bufferedReader = new BufferedReader(new FileReader(document));
            var lines = bufferedReader.lines().collect(Collectors.toList());
            var words = TFIDF.getWordsFromLines(lines);
            var documentData = TFIDF.createDocumentData(words, terms);
            documentDataMap.put(document, documentData);
        }
        var documentsByScore = TFIDF.getDocumentsSortedByScore(terms, documentDataMap);
        printResults(documentsByScore);
    }

    private static void printResults(final Map<Double, List<String>> documentsByScore) {
        for(Map.Entry<Double, List<String>> documentScorePair : documentsByScore.entrySet()) {
            double score = documentScorePair.getKey();
            for(String document : documentScorePair.getValue()){
                System.out.println(String.format("Book : %s - score : %f", document.split("/")[3],score));
            }
        }
    }
}
