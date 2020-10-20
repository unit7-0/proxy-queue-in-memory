package com.breezzo.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class RequestSendingTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(RequestSendingTask.class);

    private static final int BACKOFF_PERIOD_SECONDS = 1;

    private final BlockingQueue<ClientRequest> requestQueue;
    private final RemoteServiceHttpClient remoteServiceHttpClient;

    public RequestSendingTask(BlockingQueue<ClientRequest> requestQueue,
                              RemoteServiceHttpClient remoteServiceHttpClient) {
        this.requestQueue = requestQueue;
        this.remoteServiceHttpClient = remoteServiceHttpClient;
    }

    @Override
    public void run() {
        try {
            pollQueue();
        } catch (InterruptedException e) {
            logger.warn("Thread was interrupted while waiting new element from the queue");
        }
    }

    private void pollQueue() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            final ClientRequest request = requestQueue.take();
            final boolean requestHasBeenSent = remoteServiceHttpClient.sendRequest(request);
            if (!requestHasBeenSent) {
                requestQueue.put(request);
                break; // backoff for a while in case of failure
            }
        }
    }

    public static ScheduledExecutorService scheduleTasks(BlockingQueue<ClientRequest> requestQueue,
                                                         int concurrencyLevel,
                                                         URI remoteServiceUri) {
        final var executor = Executors.newScheduledThreadPool(concurrencyLevel);
        final var remoteServiceHttpClient = new RemoteServiceHttpClient(remoteServiceUri);
        for (int i = 0; i < concurrencyLevel; ++i) {
            final var requestSendingTask = new RequestSendingTask(requestQueue, remoteServiceHttpClient);
            executor.scheduleAtFixedRate(requestSendingTask, 0, BACKOFF_PERIOD_SECONDS, TimeUnit.SECONDS);
        }
        return executor;
    }
}