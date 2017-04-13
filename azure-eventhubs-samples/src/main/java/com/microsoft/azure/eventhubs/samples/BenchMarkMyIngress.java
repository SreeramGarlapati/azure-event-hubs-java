/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/*
 * Performance BenchMark is specific to customers load pattern!!
 *
 * This sample is intended to highlight various load parameters involved
 * - One variable that cannot be exercised in Code - is Network proximity
 *  (make sure to run in same region/AzureDataCenter as your target scenario - to get identical results)
 *
 * If you are running this against an EventHubs Namespace in a shared public instance
 * - results might slightly vary across runs; if you want more predictable results - use Dedicated EventHubs .
 */
public class BenchMarkMyIngress {

    public static void main(String[] args)
            throws ServiceBusException, ExecutionException, InterruptedException, IOException {

        final String namespaceName = "----ServiceBusNamespaceName-----";
        final String eventHubName = "----EventHubName-----";
        final String sasKeyName = "-----SharedAccessSignatureKeyName-----";
        final String sasKey = "---SharedAccessSignatureKey----";
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);


        // ***************************************************************************************************************
        // List of parameters involved
        // 1 - EVENT SIZE
        // 2 - NO OF CONCURRENT SENDS per sec
        // 3 - NO OF EVENTS - CLIENTS CAN BATCH & SEND <-- and there by optimize on ACKs returned from the Service
        // 4 - NO OF SENDERS PER CONNECTION <-- This sample doesn't include this / only demonstrates what can be achieved using 1 Sender AMQP link using 1 Connection
        // ***************************************************************************************************************
        final int EVENT_SIZE = 2048; // 2k
        final int NO_OF_CONCURRENT_SENDS = 20;
        final int BATCH_SIZE = 50;


        // Consider creating a pool of EventHubClient objects - based on the predicted load per process and this Benchmark test outcome
        // if you want to send 10 MBperSEC from a single process to 1 EventHub - you might want 2-3 of these
        // EventHubClient reserves its own **PHYSICAL SOCKET**
        final EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());


        // generate eventdatas with load params


        // figure out how to calculate percentiles
    }
}