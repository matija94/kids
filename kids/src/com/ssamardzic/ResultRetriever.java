package com.ssamardzic;

import java.util.*;
import java.util.concurrent.*;

public class ResultRetriever {

    private ExecutorService executorService;

    private ConcurrentMap<String, List<Future<Map<String, Integer>>>> results;

    private Map<String, Map<String, Integer>> computedResults;

    public ResultRetriever() {
        this.results = new ConcurrentHashMap<>();
        this.computedResults = new HashMap<>();
        executorService = Executors.newCachedThreadPool();
    }

    public void clearRestultDataFor(DirectoryCrawler.FileJob fj) {
        results.remove(fj.getCorpusDir().getName());
        results.remove("summary");
        computedResults.remove(fj.getCorpusDir().getName());
        computedResults.remove("summary");
    }

    public void addCorpusResult(String rootDir, String corpusName, Future<Map<String, Integer>> futureRes) {
        List<Future<Map<String, Integer>>> result = results.getOrDefault(corpusName, new ArrayList<>());
        result.add(futureRes);
        results.put(corpusName, result);
    }

    public Object processCommandSync(String token) throws ExecutionException, InterruptedException {
        String[] split = token.split("\\|");
        switch (split[0]) {
            case "file":
                String query = split[1];
                if (query.equals("summary")) {
                    return processSummaryAndCompute();
                }
                if (computedResults.containsKey(query)) {
                    return computedResults.get(query);
                }
                else {
                    if (!results.containsKey(query)) {
                        return String.format("%s corpus does not exist", query);
                    }
                    return processQuery(query);
                }

            default:
                return null;
        }
    }

    public Object processCommandAsync(String token) {
        String [] split = token.split("\\|");
        switch (split[0]) {
            case "file":
                String query = split[1];
                if (query.equals("summary")) {
                    boolean ready = processSummary();
                    if (ready) {
                        if (!computedResults.containsKey("summary")) {
                            return computeSummary();
                        }
                        return computedResults.get("summary");
                    }
                    else {
                        return "Summary is still being computed";
                    }
                }
                if (computedResults.containsKey(query)) {
                    return computedResults.get(query);
                }
                else {
                    if (!results.containsKey(query)) {
                        return String.format("%s corpus does not exist", query);
                    }
                    executorService.submit(() -> processQuery(query));
                    return String.format("Result for %s is not ready yet", query);
                }
            default:
                return null;
        }
    }

    public void shutdownNow() {
        executorService.shutdownNow();
    }

    public Object clearSummary() {
        computedResults.clear();
        return "File summary has been removed";
    }

    private Map<String, Integer> processQuery(String query) throws InterruptedException {
        List<Future<Map<String, Integer>>> futureRes = results.get(query);
        Map<String, Integer> aggregatedResult = new HashMap<>();
        for (Future<Map<String, Integer>> textCorpusFutureRes : futureRes) {
            Map<String, Integer> textCorpusRes = null;
            try {
                textCorpusRes = textCorpusFutureRes.get();
            } catch (ExecutionException e) {
                System.out.println(String.format("Couldn't read file under corpus %", query));
            }
            for (Map.Entry<String, Integer> entry : textCorpusRes.entrySet()) {
                aggregatedResult.put(entry.getKey(), aggregatedResult.getOrDefault(entry.getKey(),
                        0) + entry.getValue());
            }
        }
        computedResults.put(query, aggregatedResult);
        return aggregatedResult;
    }

    private Map<String, Integer> processSummaryAndCompute() throws InterruptedException {
        for (String key : results.keySet()) {
            if (!computedResults.containsKey(key)) {
                processQuery(key);
            }
        }
        if (!computedResults.containsKey("summary")) {
            return computeSummary();
        }
        return computedResults.get("summary");
    }

    private boolean processSummary() {
        boolean done = true;
        for (String key : results.keySet()) {
            if (!computedResults.containsKey(key)) {
                done = false;
                executorService.submit(() -> processQuery(key));
            }
        }
        return done;
    }

    private Map<String, Integer> computeSummary() {
        Map<String, Integer> summary = new HashMap<>();
        for (Map<String, Integer> vals : computedResults.values()) {
            for (Map.Entry<String, Integer> entry : vals.entrySet()) {
                summary.put(entry.getKey(), summary.getOrDefault(entry.getKey(),
                        0) + entry.getValue());
            }
        }
        computedResults.put("summary", summary);
        return summary;
    }
}
