package com.ssamardzic;

import java.util.concurrent.LinkedBlockingQueue;

public class JobDispatcher implements Runnable {

    private LinkedBlockingQueue<ScanningJob> jobQueue;

    private FileScanner fileScanner;

    private WebScanner webScanner;

    public JobDispatcher(LinkedBlockingQueue<ScanningJob> jobQueue, FileScanner fileScanner, WebScanner webScanner) {
        this.jobQueue = jobQueue;
        this.fileScanner = fileScanner;
        this.webScanner = webScanner;
    }

    @Override
    public void run() {
        while(true) {
            ScanningJob scanningJob = null;
            try {
                scanningJob = jobQueue.take();

            } catch (InterruptedException e) {
                return;
            }
            ScanningType type = scanningJob.getType();
            System.out.println(String.format("[%s] Got new %s job for %s", Thread.currentThread().getName(),
                    type.toString(), scanningJob.getQuery()));
            switch (type){
                case FILE :
                    fileScanner.enqueue(scanningJob);
                    break;
                case WEB :
                    webScanner.enqueue(scanningJob);
                    break;
                default:
                    // NIJE NI WEB NI FILE
            }
        }
    }
}
