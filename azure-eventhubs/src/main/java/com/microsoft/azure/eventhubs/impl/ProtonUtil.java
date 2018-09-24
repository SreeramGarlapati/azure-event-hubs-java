/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.engine.HandlerException;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.reactor.Reactor;
import org.apache.qpid.proton.reactor.ReactorOptions;

import java.io.IOException;

public final class ProtonUtil {

    private ProtonUtil() {
    }

    public static Reactor reactor(final ReactorHandler reactorHandler, final int maxFrameSize) throws IOException {

        final ReactorOptions reactorOptions = new ReactorOptions();
        reactorOptions.setMaxFrameSize(maxFrameSize);
        reactorOptions.setEnableSaslByDefault(true);

        final Reactor reactor = Proton.reactor(reactorOptions, reactorHandler);
        reactor.setGlobalHandler(new CustomIOHandler());

        return reactor;
    }

    public static boolean processAndHandleKnownIssues(final Reactor reactor) {
        try {
            return reactor.process();
        } catch(HandlerException handlerException) {
            final Throwable cause = handlerException.getCause();
            if (cause == null) {
                // we do not know - why reactor threw Handler Exception
                // so, this cannot be handled.
                throw handlerException;
            }

            // KnownIssue: when AmqpSession.begin response is received in wrong order
            // proton-j library (in TransportImpl.java:1143) throws NPE with
            // errorMsg prefixed as "uncorrelated channel: "
            // This will be dead-code & can be removed when this Proton-j issue is fixed:
            // https://issues.apache.org/jira/browse/PROTON-1939
            if (cause instanceof NullPointerException
                    && cause.getMessage() != null
                    && cause.getMessage().startsWith("uncorrelated channel: ")
                    && cause.toString().contains(TransportImpl.class.toString() + ".handleBegin")) {
                // handler: ignore the error and continue
                return true;
            }

            throw handlerException;
        }
    }
}
