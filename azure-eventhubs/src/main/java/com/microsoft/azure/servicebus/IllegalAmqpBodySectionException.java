/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.servicebus.amqp.AmqpConstants;
import java.util.HashMap;
import java.util.Map;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;

public class IllegalAmqpBodySectionException extends RuntimeException {
    
    private static final Map<Class, String> KNOWN_SECTIONS = new HashMap<Class, String>() {{
        put(AmqpValue.class, AmqpConstants.AMQP_VALUE);
        put(AmqpSequence.class, AmqpConstants.AMQP_SEQUENCE);
    }};
    
    private final Class bodySection;
    
    public IllegalAmqpBodySectionException(final Class actualBodySection) {
        super(KNOWN_SECTIONS.containsKey(actualBodySection)
            ? String.format("AmqpMessage Body Section will be available in %s only if it is of type: %s. use getSystemPropertyName() method to find this value in %s.getSystemProperties()", EventData.class, Data.class, EventData.class)
            : "AmqpMessage Body Section cannot be mapped to any EventData section");
        this.bodySection = actualBodySection;
    }
    
    public String getSystemPropertyName() {
        return KNOWN_SECTIONS.get(this.bodySection);
    }
    
}
