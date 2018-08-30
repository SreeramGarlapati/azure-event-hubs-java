/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

/**
 * This exception is thrown for the following reasons:
 * <ul>
 * <li> When the entity user attempted to connect does not exist
 * <li> The entity user wants to connect is disabled
 * </ul>
 *
 * @see <a href="http://go.microsoft.com/fwlink/?LinkId=761101">http://go.microsoft.com/fwlink/?LinkId=761101</a>
 */
public class IllegalEntityException extends EventHubException {
    private static final long serialVersionUID = 1842057379278310290L;

    // TEST HOOK - to be used by unit tests to inject non-transient failures
    private static volatile boolean IS_TRANSIENT = false;

    IllegalEntityException() {
        super(IS_TRANSIENT);
    }

    public IllegalEntityException(final String message) {
        super(IS_TRANSIENT, message);
    }

    public IllegalEntityException(final Throwable cause) {
        super(IS_TRANSIENT, cause);
    }

    public IllegalEntityException(final String message, final Throwable cause) {
        super(IS_TRANSIENT, message, cause);
    }
}
