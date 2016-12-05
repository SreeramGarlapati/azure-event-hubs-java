package com.microsoft.azure.servicebus.amqp;

import java.nio.channels.UnresolvedAddressException;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.reactor.impl.IOHandler;

public class CustomIOHandler extends IOHandler {
    
	@Override
	public void onConnectionLocalOpen(Event event) {
            
            Connection connection = event.getConnection();
            if (connection.getRemoteState() != EndpointState.UNINITIALIZED) {
                    return;
            }

            Transport transport = Proton.transport();
            transport.setMaxFrameSize(AmqpConstants.MAX_FRAME_SIZE);
            transport.sasl();
            transport.setEmitFlowEventOnSend(false);
            transport.bind(connection);
	}
        
        @Override
        public void onConnectionBound(Event event) {
            
            try {
                super.onUnhandled(event);
            }
            catch (UnresolvedAddressException addressResolutionError) {
                final Transport transport = event.getConnection().getTransport();
                final ErrorCondition condition = new ErrorCondition();
                condition.setCondition(AmqpErrorCode.UnresolvedAddressError);
                condition.setDescription(addressResolutionError.getMessage());
                transport.setCondition(condition);
                transport.close_tail();
                transport.close_head();
                transport.pop(Math.max(0, transport.pending())); // Force generation of TRANSPORT_HEAD_CLOSE (not in C code)
                
                selectableTransport(event.getReactor(), null, transport);
            }
        }
}
