/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/*
 * Performance BenchMark is specific to customers load pattern!!
 *
 * This sample is intended to highlight various variables available to tune latencies
 * - One variable that cannot be exercised in Code - is Network proximity
 *  (make sure to run in same region/AzureDataCenter as your target scenario - to get identical results)
 *
 * If you are running this against an EventHubs Namespace in a shared public instance
 * - results might slightly vary across runs; if you want more predictable results - use Dedicated EventHubs .
 */
public class BenchMarkIngressLatency {

    public static void main(String[] args)
            throws ServiceBusException, ExecutionException, InterruptedException, IOException {

        final String namespaceName = "----ServiceBusNamespaceName-----";
        final String eventHubName = "----EventHubName-----";
        final String sasKeyName = "-----SharedAccessSignatureKeyName-----";
        final String sasKey = "---SharedAccessSignatureKey----";
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);


        // ***************************************************************************************************************
        // List of variables involved
        // 1 - EVENT SIZE
        // 2 - NO OF CONCURRENT SENDS per sec
        // 3 - NO OF EVENTS - CLIENTS CAN BATCH & SEND <-- and there by optimize on ACKs returned from the Service (typically, this number is supposed to help bring 2 down)
        // 4 - NO OF SENDERS PER CONNECTION <-- This sample doesn't include this / only demonstrates what can be achieved using 1 Sender AMQP link using 1 Connection
        // ***************************************************************************************************************
        final int EVENT_SIZE = 2048; // 2k
        final int NO_OF_CONCURRENT_SENDS = 20;
        final int BATCH_SIZE = 50;


        // Consider creating a pool of EventHubClient objects - based on the predicted load per process and this Benchmark test outcome
        // if you want to send 10 MBperSEC from a single process to 1 EventHub - you might want 2-3 of these
        // EventHubClient reserves its own **PHYSICAL SOCKET**
        final EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());


        for (int dataSetCount = 0; dataSetCount < 10000; dataSetCount++) {
            final List<EventData> eventDataList = new LinkedList<>();

            for (int batchSize = 0; batchSize < BATCH_SIZE; batchSize++) {
                final byte[] payload = new byte[EVENT_SIZE];
                Arrays.fill(payload, (byte) 32);
                final EventData eventData = new EventData(payload);
                eventDataList.add(eventData);
            }

            final CompletableFuture<Void>[] sendTasks = new CompletableFuture[NO_OF_CONCURRENT_SENDS];
            for (int concurrentSends = 0; concurrentSends < NO_OF_CONCURRENT_SENDS; concurrentSends++) {
                final Instant beforeSend = Instant.now();
                sendTasks[concurrentSends] = ehClient.send(eventDataList).whenComplete(new BiConsumer<Void, Throwable>() {
                    @Override
                    public void accept(Void aVoid, Throwable throwable) {
                        System.out.println(String.format("%s,%s,%s", throwable == null ? "success" : "failure", Duration.between(Instant.now(), beforeSend).toMillis()));
                    }
                });
            }

            // wait for the first send to return - to control the send-pipe line speed & degree of parallelism
            CompletableFuture.anyOf(sendTasks).get();
        }
    }
}