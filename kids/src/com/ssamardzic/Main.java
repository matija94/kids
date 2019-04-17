package com.ssamardzic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    public static void main(String[] args) throws IOException {
        LinkedBlockingQueue<ScanningJob> jobQueue = new LinkedBlockingQueue<>();
        HashMap<String, String> props = props();
        ResultRetriever resultRetriever = new ResultRetriever();
        FileScanner fs = new FileScanner(Long.parseLong(props.get("sizeThreshold")),
                parseKeywords(props.get("keywords")), resultRetriever);
        JobDispatcher dispatcher = new JobDispatcher(jobQueue, fs, null);
        Thread dispatcherThread = new Thread(dispatcher, "JobDisptacher");
        dispatcherThread.start();
        List<DirectoryCrawler> directoryCrawlers = new ArrayList<>();


        Scanner scanner = new Scanner(System.in);
        boolean shutdown = false;
        while (!shutdown) {
            System.out.print("===> ");
            String commandWithParam = scanner.nextLine();
            String tokens[] = commandWithParam.split("\\s+");

            switch (tokens[0]) {
                case "aw":
                    // TODO ADD WEB
                    break;
                case "ad":
                    if(tokens.length != 2) {
                        System.out.println("Invalid number of arguments");
                    }
                    else {
                        File root = new File(tokens[1]);
                        if (!root.exists()) {
                            System.out.println(String.format("%s is not valid directory", tokens[1]));
                        }
                        else {
                            DirectoryCrawler dc = new DirectoryCrawler(jobQueue, root, props.get("corpusPrefix"),
                                    Long.parseLong(props.get("timeToSleep")));
                            Thread dcThread = new Thread(dc, "DirectoryCrawler");
                            dcThread.start();
                            directoryCrawlers.add(dc);
                        }
                    }

                    break;
                case "get":
                    if(tokens.length != 2) {
                        System.out.println("Invalid number of arguments");
                    }
                    else {
                        try {
                            System.out.println(resultRetriever.processCommandSync(tokens[1]));
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case "query":
                    if(tokens.length != 2) {
                        System.out.println("Invalid number of arguments");
                    }
                    else {
                        System.out.println(resultRetriever.processCommandAsync(tokens[1]));
                    }
                    break;
                case "cfs":
                    System.out.println(resultRetriever.clearSummary());
                    break;
                case "stop":
                    resultRetriever.shutdownNow();
                    fs.shutdownNow();
                    dispatcherThread.interrupt();
                    directoryCrawlers.forEach(DirectoryCrawler::shutdown);
                    shutdown = true;
                    break;
                default:
                    System.out.println("Not valid command");
            }
        }
    }

    private static Set<String> parseKeywords(String keywords) {
        Set<String> keywordsSet = new HashSet<>();
        for (String word : keywords.split(",")) {
            keywordsSet.add(word);
        }
        return keywordsSet;
    }

    private static HashMap<String, String> props() throws IOException {
        List<String> strings = Files.readAllLines(new File("app.properties").toPath());
        HashMap<String, String> props = new HashMap<>();
        for (String prop : strings) {
            String[] split = prop.split("=");
            props.put(split[0], split[1]);
        }
        return props;
    }
}
