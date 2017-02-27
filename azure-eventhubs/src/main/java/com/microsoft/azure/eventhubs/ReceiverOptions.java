/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

/**
 * Represents various options which can be set during the creation of a {@link PartitionReceiver}.
 */
public final class ReceiverOptions {
    
    private boolean enableReceiverRuntimeMetric;
    
    /**
     * Knob to enable/disable runtime metric of the receiver. If this is enabled and passed to {@link EventHubClient#createReceiver(...)}  
     * @return the {@link boolean} indicating whether the runtime metric of the receiver should be enabled
     */
    public boolean getEnableReceiverRuntimeMetric() {
        
        return this.enableReceiverRuntimeMetric;
    }
    
    public void setEnableReceiverRuntimeMetric(boolean value) {
        
        this.enableReceiverRuntimeMetric = value;
    }
}
