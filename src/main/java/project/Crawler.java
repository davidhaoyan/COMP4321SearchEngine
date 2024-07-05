package project;

import project.StopStem.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.StringComparator;
import jdbm.htree.HTree;
import org.htmlparser.beans.StringBean;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import org.htmlparser.beans.LinkBean;

import java.io.*;
import java.net.URL;
import java.net.HttpURLConnection;

import static java.lang.System.out;
import static java.util.Map.entry;

public class Crawler {
	private RecordManager recordManager;
	private HTree wordMapping; 			// Word => WordID
	private HTree reverseWordMapping; 	// WordID => Word
	private HTree pageMapping; 			// Url => PageID
	private HTree reversePageMapping; 	// PageID => Url
	private HTree invertedIndex;		// WordID => {PageID: [pos]}
	private HTree pageProperties;		// PageID => {Title, URL, LastDateModified, Size}
	private HTree childrenLinks; 		// PageID => [PageID]
	private HTree parentLinks;			// PageID => [PageID]
	private Integer pageId = 0;
	private Integer wordId = 0;
	private Set seen = new HashSet<>(); // for link structure

	Crawler() throws IOException {
		recordManager = RecordManagerFactory.createRecordManager("crawler");
		wordMapping = JDBMHelper.createOrLoadHTree(recordManager, "WordMapping");
		reverseWordMapping = JDBMHelper.createOrLoadHTree(recordManager, "ReverseWordMapping");
		pageMapping = JDBMHelper.createOrLoadHTree(recordManager, "PageMapping");
		reversePageMapping = JDBMHelper.createOrLoadHTree(recordManager, "ReversePageMapping");
		invertedIndex = JDBMHelper.createOrLoadHTree(recordManager, "InvertedIndex");
		pageProperties = JDBMHelper.createOrLoadHTree(recordManager, "PageProperties");
		childrenLinks = JDBMHelper.createOrLoadHTree(recordManager, "ChildrenLinks");
		parentLinks = JDBMHelper.createOrLoadHTree(recordManager, "ParentLinks");
	}

	private String extractLastModificationDate(String url) throws IOException {
        URL urlObj = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) urlObj.openConnection();
        long lastModified = connection.getLastModified();
        Date date = new Date(lastModified);
        return date.toString();
    }
    private int extractPageSize(String url) throws IOException {
        URL urlObj = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) urlObj.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        int contentLength = connection.getContentLength();
        return contentLength;
	}
	public Vector<String> extractWords(String url) throws ParserException {
		StringBean sb;
		sb = new StringBean();
		sb.setLinks(false);
		sb.setURL(url);
		Vector<String> vec_tokens = new Vector<>();
		StopStem stopStem = new StopStem("stopwords.txt");
		Parser parser = new Parser(url);
		NodeList titleNodes = parser.extractAllNodesThatMatch(new NodeClassFilter(org.htmlparser.tags.TitleTag.class));
		int titlenum;
        if (titleNodes.size() > 0) {
            org.htmlparser.tags.TitleTag titleTag = (org.htmlparser.tags.TitleTag) titleNodes.elementAt(0);
			vec_tokens.add(titleTag.getStringText());
			titlenum = titleTag.getStringText().split("\\s+").length;
		}else{
			vec_tokens.add("");
			titlenum = 0;
		}
		
		String text = sb.getStrings();
		String [] tokens = text.split("[\\p{Punct}\\s]+");
		
		for (int i = titlenum; i < tokens.length; i++){
			if (!stopStem.isStopWord(tokens[i]))
				vec_tokens.add(tokens[i]);
		}

		return vec_tokens;
	}
	private Vector<String> extractLinks(String url) throws ParserException {
		LinkBean lb;
		
		lb = new LinkBean();
		lb.setURL(url);
		URL [] links = lb.getLinks();
		Vector <String> vec_links = new Vector<String>();
		for (int i = 0; i < links.length; i++){
			vec_links.add(links[i].toString());
		}
		return vec_links;
	}
	public void updatePageMapping(String url) throws IOException 	{
		if (pageMapping.get(url) == null) {
			pageMapping.put(url, pageId);
			reversePageMapping.put(pageId, url);
			pageId++;
		}
	}
	private void updateWordMapping(Vector<String> words) {
		for (int pos = 1; pos < words.size(); pos++) {
			String word = words.get(pos);
			try {
				if (wordMapping.get(word) == null) {
					wordMapping.put(word, wordId);
					reverseWordMapping.put(wordId, word);
					wordId++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	private void updateInvertedIndex(Vector<String> words, String url) {
		// Title
		// System.out.println("Title:" + words.get(0));
		// System.out.println("Body:" + words.get(1));
		// Body
		for (int pos = 1; pos < words.size(); pos++) {
			try {
				Integer wordId = (Integer)wordMapping.get(words.get(pos));

				// Update inverted index
				if (wordId == null) { continue; } // Word not in vocabulary
				Integer pageId = (Integer)pageMapping.get(url);
				if (invertedIndex.get(wordId) == null) {
					Map<Integer, List<Integer>> entry = new HashMap<>();
					entry.put(pageId, new ArrayList<>(List.of(pos)));
					invertedIndex.put(wordId, entry);
				} else {
					Map<Integer, List<Integer>> existingEntry = (Map<Integer, List<Integer>>) invertedIndex.get(wordId);
					if (existingEntry.containsKey(pageId)) {
						existingEntry.get(pageId).add(pos);
					} else {
						existingEntry.put(pageId, new ArrayList<>(List.of(pos)));
					}
					invertedIndex.put(wordId, existingEntry);
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}
	private Map<String, Integer> extractTopFiveKeywords(Vector<String> keywords) {
		Map<String, Integer> keywordCounts = new HashMap<>();
		for (String keyword : keywords) {
			keywordCounts.put(keyword, keywordCounts.getOrDefault(keyword, 0) + 1);
		}
		List<Map.Entry<String, Integer>> entryList = new ArrayList<>(keywordCounts.entrySet());
		Collections.sort(entryList, (entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));
		Map<String, Integer> topKeywords = new HashMap<>();
		for (int i = 0; i < Math.min(entryList.size(), 5); i++) {
			topKeywords.put(entryList.get(i).getKey(), entryList.get(i).getValue());
		}
		return topKeywords;
	}
	private void updatePageProperties(String title, String url, String lastDateModified, Integer size, Vector<String> keywords) throws IOException {
		Integer pageId = (Integer)pageMapping.get(url);

		if (pageProperties.get(pageId) == null) {
			pageProperties.put(pageId, Map.ofEntries(
					entry("title", title),
					entry("url", url),
					entry("lastDateModified", lastDateModified),
					entry("size", size),
					entry("topKeywords", extractTopFiveKeywords(keywords))
			));
		}
	}
	private boolean pageUpdated(String url, String lastDateModified) throws IOException, ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

		Date lastModifiedDate = dateFormat.parse(lastDateModified);
		Date existingDate = dateFormat.parse((String)((Map)pageProperties.get(pageId)).get("lastDateModified"));

		return lastModifiedDate.after(existingDate);
	}
	private boolean pageCrawled(String url) throws IOException {
		return (pageMapping.get(url) != null);
	}
	private void updateLinks(String url, Vector<String> links) throws IOException {
		Integer parentId = (Integer) pageMapping.get(url);
		if (childrenLinks.get(parentId) == null) {
			childrenLinks.put(parentId, new Vector<>());
		}
		for (String child: links) {
			Integer childId = (Integer) pageMapping.get(child);
			if (parentLinks.get(childId) == null) {
				parentLinks.put(childId, new Vector<>());
			}
			Vector<Integer> parents = (Vector<Integer>) parentLinks.get(childId);
			parents.add(parentId);
			parentLinks.put(childId, parents);

			Vector<Integer> children = (Vector<Integer>) childrenLinks.get(parentId);
			children.add(childId);
			childrenLinks.put(parentId, children);
		}
	}

	public void createLinkStructure(String url) throws ParserException, IOException {
		System.out.println("Creating link structure: " + url);
		Vector<String> links = extractLinks(url);
		seen.add(url);

		updateLinks(url, links);

		for (int i = 0; i < links.size(); i++) {
			if (!seen.contains(links.get(i))) {
				createLinkStructure(links.get(i));
			}
		}
	}

	public void crawlPage(String url) throws IOException, ParserException, ParseException {
		System.out.println("Crawling: " + url);
		Vector<String> words = extractWords(url);
		Vector<String> links = extractLinks(url);

		String title = words.get(0);
		String lastDateModified = extractLastModificationDate(url);
		boolean updatedRecently = false;
		if (pageCrawled(url)) {
			updatedRecently = pageUpdated(url, lastDateModified);
		}
		Integer size = extractPageSize(url);

		updateWordMapping(words);
		updatePageMapping(url);
		updateInvertedIndex(words, url);
		updatePageProperties(title, url, lastDateModified, size, words);

		for (int i = 0; i < links.size(); i++) {
			if (!pageCrawled(links.get(i)) || updatedRecently) {
				crawlPage(links.get(i));
			}
		}
	}

	public static void runCrawler(String initialUrl) throws IOException {
		try {
			Crawler crawler = new Crawler();
			crawler.crawlPage(initialUrl);
			crawler.createLinkStructure(initialUrl);
			crawler.recordManager.commit();
			crawler.recordManager.close();
		} catch (ParserException | ParseException e) {
			e.printStackTrace ();
		}
	}
}

