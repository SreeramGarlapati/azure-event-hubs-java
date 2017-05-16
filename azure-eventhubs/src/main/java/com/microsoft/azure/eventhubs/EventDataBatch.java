/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.util.Collections;
import java.util.List;

/**
 * Helper class for creating a batch of {@link EventData} objects to be used with {@link EventHubClient#send(Iterable)} call.
 */
public final class EventDataBatch {

    private final long maxSize;
    private final String partitionKey;

    private long currentSize;
    private List<EventData> eventDataList;

    EventDataBatch(final long maxSizeInBytes, final String partitionKey) {

        this.maxSize = maxSizeInBytes;
        this.partitionKey = partitionKey;
        this.currentSize = (long) Math.ceil(((double) maxSizeInBytes) / 65536) * 1024; // reserve 1KB for every 64KB
    }

    /**
     * Size of EventDataBatch
     * @return Size of EventDataBatch
     */
    public int size() {

        return eventDataList == null ? 0 : eventDataList.size();
    }

    public Iterable<EventData> toIterable() {

        return this.eventDataList;
    }

    /**
     * Tries to add {@link EventData} to the current {@link EventDataBatch}, if permitted by the batch's size limit.
     * @param eventData the {@link EventData to be added}
     * @return
     */
    public boolean tryAdd(EventData eventData) {

        if (eventData == null) {
            throw new IllegalArgumentException("eventData");
        }

        eventDataList.add(eventData);
        return true;
    }

    private static long computeSize(EventData eventdata, boolean ifFirst) {

        long size = eventdata.getBytes().length;
        size += 16;

        if ()
    }
}
