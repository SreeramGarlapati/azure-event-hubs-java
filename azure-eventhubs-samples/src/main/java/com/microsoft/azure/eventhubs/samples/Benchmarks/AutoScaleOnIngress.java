/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples.Benchmarks;

import com.microsoft.azure.eventhubs.EventData;
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

public class AutoScaleOnIngress {

	public static void main(String[] args)
			throws ServiceBusException, ExecutionException, InterruptedException, IOException {

		// *********************************************************************
		// List of variables involved - to achieve desired LOAD / THROUGHPUT UNITS
		// 1 - NO OF CONCURRENT SENDS
		// 2 - BATCH SIZE - aka NO OF EVENTS CLIENTS CAN BATCH & SEND <-- and
		// there by optimize on ACKs returned from the Service (typically, this
		// number is supposed to help bring 2 down)
		// *********************************************************************
		System.out.println();
		System.out.print("Enter no. of Target ThroughPut's to hit the EventHub: ");
		final int tus = Integer.parseInt(System.console().readLine());
		final int EVENT_SIZE = 10; // 10 kb <-- Change these knobs to determine
									// target throughput; default is set to 5 *
									// 5 * 1024 = 100 KB per SendLatency

		final int NO_OF_CONCURRENT_SENDS = tus;
		final int BATCH_SIZE = 1000;

		final int NO_OF_CONNECTIONS = 4 * tus;

		System.out.println();
		System.out.print("Connection String: ");
		final String connectionString = System.console().readLine();
		System.out.println();

		final EventHubClientPool ehClientPool = new EventHubClientPool(NO_OF_CONNECTIONS, connectionString);

		ehClientPool.initialize().get();
		System.out.println("started sending...");

		final CompletableFuture<Void>[] sendTasks = new CompletableFuture[NO_OF_CONCURRENT_SENDS];
		for (int perfSample = 0; perfSample < Integer.MAX_VALUE - NO_OF_CONCURRENT_SENDS + 1; perfSample++) {

			final Instant beforeSend = Instant.now();
			final List<EventData> eventDataList = new LinkedList<>();

			for (int batchSize = 0; batchSize < BATCH_SIZE; batchSize++) {
				final byte[] payload = new byte[EVENT_SIZE];
				Arrays.fill(payload, (byte) 32);
				final EventData eventData = new EventData(payload);
				eventDataList.add(eventData);
			}

			for (int concurrentSends = 0; concurrentSends < NO_OF_CONCURRENT_SENDS; concurrentSends++) {
				if (sendTasks[concurrentSends] == null || sendTasks[concurrentSends].isDone()) {
					sendTasks[concurrentSends] = ehClientPool.send(eventDataList)
							.whenComplete(new BiConsumer<Void, Throwable>() {
								@Override
								public void accept(Void aVoid, Throwable throwable) {
									System.out.println(String.format("result: %s, latency: %s, batchSize, %s", throwable == null ? "success" : "failure",
											Duration.between(beforeSend, Instant.now()).toMillis(), BATCH_SIZE));

									if (throwable != null && throwable.getCause() != null) {
										System.out.println(String.format("send failed with error: %s",
												throwable.getCause().getMessage()));
									}
								}
							});
				}
			}

			try {
				CompletableFuture.allOf(sendTasks).get();
			} catch (Exception ignore) {
			}

			long millis = Duration.between(beforeSend, Instant.now()).toMillis();
			if (millis < 1000) {
				Thread.sleep(1000 - millis);
				System.out.println(String.format("wait (to slow down the pipeline to hit configured TUs) before the next send burst for: %sms", 1000 - millis));
			}
		}
	}
}