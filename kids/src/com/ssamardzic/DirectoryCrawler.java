package com.ssamardzic;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class DirectoryCrawler implements Runnable {

    private LinkedBlockingQueue<ScanningJob> jobQueue;

    private String corpusPrefix;

    private Map<String, Long> lastModified;

    private boolean shutdown = false;

    private long timeToSleep;

    private File root;

    public DirectoryCrawler(LinkedBlockingQueue<ScanningJob> jobQueue, File root, String corpusPrefix,
                            long timeToSleep) {
        this.root = root;
        this.jobQueue = jobQueue;
        this.corpusPrefix = corpusPrefix;
        lastModified = new HashMap<>();
        this.timeToSleep = timeToSleep;
    }

    class FileJob implements ScanningJob {
        private File corpusDir;
        private String rootDir;

        FileJob(String rootDir, File corpusDir) {
            this.rootDir = rootDir;
            this.corpusDir = corpusDir;
        }

        @Override
        public ScanningType getType() {
            return ScanningType.FILE;
        }

        @Override
        public String getQuery() {
            return rootDir;
        }

        public File getCorpusDir() {
            return corpusDir;
        }

    }

    @Override
    public void run() {
        while(!shutdown) {
            File[] files = root.listFiles();
            List<File> jobFiles = scan(files);
            for (File jobFile : jobFiles) {
                FileJob fj = new FileJob(root.getName(), jobFile);
                jobQueue.add(fj);
            }
            try {
                Thread.sleep(timeToSleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        shutdown = true;
    }

    private List<File> scan(File[] files) {
        List<File> toScan = new ArrayList<>();
        for (File file : files) {
            if (file.getName().startsWith(corpusPrefix)) {
                long modifiedAt = lastModified.getOrDefault(file.getName(), -1l);
                if (modifiedAt != file.lastModified()) {
                    lastModified.put(file.getName(), file.lastModified());
                    toScan.add(file);
                }
            }
            else if (file.isDirectory()){
                List<File> toScanChildren = scan(file.listFiles());
                toScan.addAll(toScanChildren);
            }
        }
        return toScan;
    }
}
