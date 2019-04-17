package com.ssamardzic;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

public class FileScanner {

    private ExecutorService executorService;

    private ResultRetriever resultRetriever;

    private long sizeThreshold;

    private Set<String> keyWords;

    FileScanner(long sizeThreshold, Set<String> keywords, ResultRetriever resultRetriever) {
        this.executorService = Executors.newCachedThreadPool();
        this.sizeThreshold = sizeThreshold;
        this.keyWords = keywords;
        this.resultRetriever = resultRetriever;
    }

    public void enqueue(ScanningJob scanningJob) {
        DirectoryCrawler.FileJob fj = (DirectoryCrawler.FileJob) scanningJob;
        resultRetriever.clearRestultDataFor(fj);
        System.out.println(String.format("[FileScanner] Started new scan file|%s", fj.getCorpusDir()));
        File corpusDir = fj.getCorpusDir();
        long currentSize = 0;
        int start = 0;
        int end = 1;
        File[] files = corpusDir.listFiles();
        for (File corpus : files) {
            currentSize += corpus.length();
            if (currentSize > sizeThreshold) {
                PartJob pj = new PartJob(start, end, files);
                resultRetriever.addCorpusResult(fj.getQuery(), fj.getCorpusDir().getName(), executorService.submit(pj));
                currentSize = 0;
                start = end;
            }
            end += 1;
        }
    }

    public void shutdownNow() {
        executorService.shutdownNow();
    }

    private class PartJob implements Callable<Map<String, Integer>> {

        private int start;
        private int end;
        private File[] files;

        public PartJob(int start, int end, File [] files) {
            this.start = start;
            this.end = end;
            this.files = files;
        }

        @Override
        public Map<String, Integer> call() throws Exception {
            Map<String, Integer> res = new HashMap<>();
            for (; start < end; start++) {
                File f = files[start];
                List<String> lines = Files.readAllLines(f.toPath());
                for (String line : lines) {
                    String[] words = line.split("\\s+");
                    for (String word : words) {
                        if (keyWords.contains(word)) {
                            res.put(word, 1 + res.getOrDefault(word, 0));
                        }
                    }
                }
            }
            return res;
        }
    }
}

