package project;

import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.Callback;
import org.htmlparser.http.HttpHeader;
import org.htmlparser.util.ParserException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Main {
    public static void server() throws Exception {
        Server server = new Server(8080);
        System.out.println("Hosting on http://localhost:8080");
        server.setHandler(new Handler.Abstract()
        {
            @Override
            public boolean handle(Request request, Response response, Callback callback)
            {
                // Succeed the callback to signal that the
                // request/response processing is complete.
                String path = request.getHttpURI().getPath();
                if (path.equals("/search")) {
                    CompletableFuture<String> completable = Content.Source.asStringAsync(request, UTF_8);

                    completable.whenComplete((requestContent, failure) -> {
                        String jsonString = requestContent ; //assign your JSON String here
                        JSONObject obj = new JSONObject(jsonString);
                        String query = obj.getString("query");
                        System.out.println("Query:" + query);
                        try {
                            Vector<Retriever.RankingEntry> searchRanking = Retriever.runRetriever(query);
                            JSONArray rankingArray = new JSONArray();
                            for (Retriever.RankingEntry entry : searchRanking) {
                                JSONObject rankingEntry = new JSONObject();
                                rankingEntry.put("id", entry.pageId);
                                rankingEntry.put("url", entry.url);
                                rankingEntry.put("score", entry.score);
                                rankingEntry.put("title", entry.title);
                                rankingEntry.put("lastDateModified", entry.lastDateModified);
                                rankingEntry.put("size", entry.size);
                                rankingEntry.put("keywords", entry.keywords);
                                rankingEntry.put("children", entry.children);
                                rankingEntry.put("parents", entry.parents);
                                rankingArray.put(rankingEntry);
                            }
                            response.setStatus(200);
                            response.write(true, BufferUtil.toBuffer(rankingArray.toString()), callback);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ParserException e) {
                            e.printStackTrace();
                        }
                    });
                }

                callback.succeeded();
                return true;
            }
        });
        server.start();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Please provide an argument (crawler/retriever).");
            return;
        }
        String argument = args[0];
        if (argument.equals("crawler")) {
            Crawler.runCrawler("https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"); // BFS indexing keywords and page properties
            VectorSpaceModel.runVectorSpaceModel("crawler", "documentVectors"); // Transforms db into vector space model
        } else if (argument.equals("server")){
            server();
        } else {
            System.out.println("Invalid argument. Please provide 'crawler' or 'retriever'.");
        }
    }
}