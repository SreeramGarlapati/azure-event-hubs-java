/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnknownDescribedType;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.amqp.DispatchHandler;
import com.microsoft.azure.servicebus.amqp.IAmqpReceiver;
import com.microsoft.azure.servicebus.amqp.IOperationResult;
import com.microsoft.azure.servicebus.amqp.ReceiveLinkHandler;

/**
 * Common Receiver that abstracts all amqp related details
 * translates event-driven reactor model into async receive Api
 */
public final class MessageReceiver extends ClientEntity implements IAmqpReceiver, IErrorContextProvider
{
	private static final Logger TRACE_LOGGER = Logger.getLogger(ClientConstants.SERVICEBUS_CLIENT_TRACE);
	private static final int MIN_TIMEOUT_DURATION_MILLIS = 20;

	private final ConcurrentLinkedQueue<ReceiveWorkItem> pendingReceives;
	private final MessagingFactory underlyingFactory;
	private final String receivePath;
	private final Runnable onOperationTimedout;
	private final Duration operationTimeout;
	private final CompletableFuture<Void> linkClose;
	private final Object prefetchCountSync;
	private final IReceiverSettingsProvider settingsProvider;
        private final String tokenAudience;
        private final ActiveClientTokenManager activeClientTokenManager;
        private final WorkItem<MessageReceiver> linkOpen;
	private final ConcurrentLinkedQueue<Message> prefetchedMessages;
        private final ReceiveWork receiveWork;
        private final CreateAndReceive createAndReceive;
	            
	private int prefetchCount;
        private Receiver receiveLink;
	private Duration receiveTimeout;
        private Message lastReceivedMessage;
	private Exception lastKnownLinkError;
	private int nextCreditToFlow;
        private boolean creatingLink;

	private MessageReceiver(final MessagingFactory factory,
			final String name, 
			final String recvPath,
			final int prefetchCount,
			final IReceiverSettingsProvider settingsProvider)
	{
		super(name, factory);

		this.underlyingFactory = factory;
		this.operationTimeout = factory.getOperationTimeout();
		this.receivePath = recvPath;
		this.prefetchCount = prefetchCount;
		this.prefetchedMessages = new ConcurrentLinkedQueue<>();
		this.linkClose = new CompletableFuture<>();
		this.lastKnownLinkError = null;
		this.receiveTimeout = factory.getOperationTimeout();
		this.prefetchCountSync = new Object();
                this.settingsProvider = settingsProvider;
                this.linkOpen = new WorkItem<>(new CompletableFuture<>(), factory.getOperationTimeout());
		
		this.pendingReceives = new ConcurrentLinkedQueue<>();

		// onOperationTimeout delegate - per receive call
		this.onOperationTimedout = new Runnable()
		{
                    public void run()
                    {
                        WorkItem<Collection<Message>> topWorkItem = null;
                        while((topWorkItem = MessageReceiver.this.pendingReceives.peek()) != null)
                        {
                            if (topWorkItem.getTimeoutTracker().remaining().toMillis() <= MessageReceiver.MIN_TIMEOUT_DURATION_MILLIS)
                            {
                                WorkItem<Collection<Message>> dequedWorkItem = MessageReceiver.this.pendingReceives.poll();
                                if (dequedWorkItem != null && dequedWorkItem.getWork() != null && !dequedWorkItem.getWork().isDone()) {
                                        dequedWorkItem.getWork().complete(null);
                                }
                                else
                                        break;
                            }
                            else
                            {
                                MessageReceiver.this.scheduleOperationTimer(topWorkItem.getTimeoutTracker());
                                break;
                            }
                        }
                    }
		};
                
                this.receiveWork = new ReceiveWork();
                this.createAndReceive = new CreateAndReceive();
                
                this.tokenAudience = String.format("amqp://%s/%s", underlyingFactory.getHostName(), receivePath);
                
                this.activeClientTokenManager = new ActiveClientTokenManager(
                        this, 
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                        underlyingFactory.getCBSChannel().sendToken(
                                            underlyingFactory.getReactorScheduler(),
                                            underlyingFactory.getTokenProvider().getToken(tokenAudience, ClientConstants.TOKEN_REFRESH_INTERVAL), 
                                            tokenAudience, 
                                            new IOperationResult<Void, Exception>() {
                                                @Override
                                                public void onComplete(Void result) {
                                                    if (TRACE_LOGGER.isLoggable(Level.FINE)) {
                                                            TRACE_LOGGER.log(Level.FINE,
                                                                            String.format(Locale.US, 
                                                                            "path[%s], linkName[%s] - token renewed", receivePath, receiveLink.getName()));
                                                    }
                                                }
                                                @Override
                                                public void onError(Exception error) {
                                                    MessageReceiver.this.onError(error);
                                                }
                                            });
                                    }
                                    catch(IOException|NoSuchAlgorithmException|InvalidKeyException|RuntimeException exception) {
                                        MessageReceiver.this.onError(exception);
                                    }
                                }
                            }, 
                            ClientConstants.TOKEN_REFRESH_INTERVAL);
	}
        
	// @param connection Connection on which the MessageReceiver's receive Amqp link need to be created on.
	// Connection has to be associated with Reactor before Creating a receiver on it.
	public static CompletableFuture<MessageReceiver> create(
			final MessagingFactory factory, 
			final String name, 
			final String recvPath,
			final int prefetchCount,
			final IReceiverSettingsProvider settingsProvider)
	{
		MessageReceiver msgReceiver = new MessageReceiver(
                        factory,
                        name,
                        recvPath,
                        prefetchCount,
                        settingsProvider);
		return msgReceiver.createLink();
	}
        
        public String getReceivePath()
	{
		return this.receivePath;
	}

	private CompletableFuture<MessageReceiver> createLink()
	{
		this.scheduleLinkOpenTimeout(this.linkOpen.getTimeoutTracker());
		try
		{
			this.underlyingFactory.scheduleOnReactorThread(new DispatchHandler()
			{
				@Override
				public void onEvent()
				{
					MessageReceiver.this.createReceiveLink();
				}
			});
		}
		catch (IOException ioException)
		{
			this.linkOpen.getWork().completeExceptionally(new ServiceBusException(false, "Failed to create Receiver, see cause for more details.", ioException));
		}

		return this.linkOpen.getWork();
	}

	private List<Message> receiveCore(final int messageCount)
	{
		List<Message> returnMessages = null;
		Message currentMessage = this.pollPrefetchQueue();
	
		while (currentMessage != null) 
		{
			if (returnMessages == null)
			{
				returnMessages = new LinkedList<>();
			}

			returnMessages.add(currentMessage);
			if (returnMessages.size() >= messageCount)
			{
				break;
			}

			currentMessage = this.pollPrefetchQueue();
		}
		
		return returnMessages;
	}

	public int getPrefetchCount()
	{
		synchronized (this.prefetchCountSync)
		{
			return this.prefetchCount;
		}
	}

	public void setPrefetchCount(final int value) throws ServiceBusException
	{
		final int deltaPrefetchCount;
		synchronized (this.prefetchCountSync)
		{
			deltaPrefetchCount = this.prefetchCount - value;
			this.prefetchCount = value;
		}
		
		try
		{
			this.underlyingFactory.scheduleOnReactorThread(new DispatchHandler()
			{
				@Override
				public void onEvent()
				{
					sendFlow(deltaPrefetchCount);
				}
			});
		}
		catch (IOException ioException)
		{
			throw new ServiceBusException(false, "Setting prefetch count failed, see cause for more details", ioException);
		}
	}

	public Duration getReceiveTimeout()
	{
		return this.receiveTimeout;
	}

	public void setReceiveTimeout(final Duration value)
	{
		this.receiveTimeout = value;
	}

	public CompletableFuture<Collection<Message>> receive(final int maxMessageCount)
	{
		this.throwIfClosed(this.lastKnownLinkError);

		if (maxMessageCount <= 0 || maxMessageCount > this.prefetchCount)
		{
			throw new IllegalArgumentException(String.format(Locale.US, "parameter 'maxMessageCount' should be a positive number and should be less than prefetchCount(%s)", this.prefetchCount));
		}

		if (this.pendingReceives.isEmpty())
		{
			this.scheduleOperationTimer(TimeoutTracker.create(this.receiveTimeout));
		}

		CompletableFuture<Collection<Message>> onReceive = new CompletableFuture<>();
                pendingReceives.offer(new ReceiveWorkItem(onReceive, receiveTimeout, maxMessageCount));
		
		try {
                    this.underlyingFactory.scheduleOnReactorThread(this.createAndReceive);
		}
		catch (IOException ioException) {
                    onReceive.completeExceptionally(new OperationCancelledException("Receive failed while dispatching to Reactor, see cause for more details.", ioException));
		}
                
		return onReceive;
	}

        @Override
	public void onOpenComplete(Exception exception)
	{		
                this.creatingLink = false;
            
		if (exception == null)
		{
                        if (this.getIsClosingOrClosed()) {
                            
                            this.receiveLink.close();
                            return;
                        }
                        
			if (this.linkOpen != null && !this.linkOpen.getWork().isDone())
			{
				this.linkOpen.getWork().complete(this);
			}

			this.lastKnownLinkError = null;

			this.underlyingFactory.getRetryPolicy().resetRetryCount(this.underlyingFactory.getClientId());

			this.nextCreditToFlow = 0;
			this.sendFlow(this.prefetchCount - this.prefetchedMessages.size());

			if(TRACE_LOGGER.isLoggable(Level.FINE))
			{
				TRACE_LOGGER.log(Level.FINE, String.format("receiverPath[%s], linkname[%s], updated-link-credit[%s], sentCredits[%s]",
						this.receivePath, this.receiveLink.getName(), this.receiveLink.getCredit(), this.prefetchCount));
			}
		}
		else
		{
			if (this.linkOpen != null && !this.linkOpen.getWork().isDone())
			{
				this.setClosed();
				ExceptionUtil.completeExceptionally(this.linkOpen.getWork(), exception, this);
			}

			this.lastKnownLinkError = exception;
		}
	}

	@Override
	public void onReceiveComplete(Delivery delivery)
	{
		int msgSize = delivery.pending();
		byte[] buffer = new byte[msgSize];
		
		int read = receiveLink.recv(buffer, 0, msgSize);
		
		Message message = Proton.message();
		message.decode(buffer, 0, read);
                
		delivery.settle();

		this.prefetchedMessages.add(message);
		this.underlyingFactory.getRetryPolicy().resetRetryCount(this.getClientId());
		
                this.receiveWork.onEvent();
	}

	public void onError(final ErrorCondition error)
	{		
		final Exception completionException = ExceptionUtil.toException(error);
		this.onError(completionException);
	}

	@Override
	public void onError(final Exception exception)
	{
                this.prefetchedMessages.clear();

		if (this.getIsClosingOrClosed())
		{
			WorkItem<Collection<Message>> workItem = null;
			final boolean isTransientException = exception == null ||
					(exception instanceof ServiceBusException && ((ServiceBusException) exception).getIsTransient());
			while ((workItem = this.pendingReceives.poll()) != null)
			{
				final CompletableFuture<Collection<Message>> future = workItem.getWork();
				if (isTransientException)
				{
					future.complete(null);
				}
				else
				{
					ExceptionUtil.completeExceptionally(future, exception, this);
				}
			}
                        
                        this.linkClose.complete(null);
		}
		else
		{
			this.lastKnownLinkError = exception == null ? this.lastKnownLinkError : exception;
			
                        final Exception completionException = exception == null
                                ? new ServiceBusException(true, "Client encountered transient error for unknown reasons, please retry the operation.") : exception;
                        
                        this.onOpenComplete(completionException);
			
			final WorkItem<Collection<Message>> workItem = this.pendingReceives.peek();
			final Duration nextRetryInterval = workItem != null && workItem.getTimeoutTracker() != null
					? this.underlyingFactory.getRetryPolicy().getNextRetryInterval(this.getClientId(), completionException, workItem.getTimeoutTracker().remaining())
					: null;
			
                        boolean recreateScheduled = true;

                        if (nextRetryInterval != null)
			{
                                try
				{
					this.underlyingFactory.scheduleOnReactorThread((int) nextRetryInterval.toMillis(), new DispatchHandler()
					{
						@Override
						public void onEvent()
						{
							if (!MessageReceiver.this.getIsClosingOrClosed()
                                                                && (receiveLink.getLocalState() == EndpointState.CLOSED || receiveLink.getRemoteState() == EndpointState.CLOSED))
							{
								createReceiveLink();
								underlyingFactory.getRetryPolicy().incrementRetryCount(getClientId());
							}
						}
					});
				}
				catch (IOException ignore)
				{
                                    recreateScheduled = false;
				}
                        }			
                                
			if (nextRetryInterval == null || !recreateScheduled)
			{
				WorkItem<Collection<Message>> pendingReceive = null;
				while ((pendingReceive = this.pendingReceives.poll()) != null)
				{
					ExceptionUtil.completeExceptionally(pendingReceive.getWork(), completionException, this);
				}
			}
		}
	}

	private void scheduleOperationTimer(final TimeoutTracker tracker)
	{
		if (tracker != null)
		{
			Timer.schedule(this.onOperationTimedout, tracker.remaining(), TimerType.OneTimeRun);
		}
	}

	private void createReceiveLink()
	{
            if (creatingLink)
                return;
            
            this.creatingLink = true;
            
            final Consumer<Session> onSessionOpen = new Consumer<Session>()
            {
                @Override
                public void accept(Session session)
                {
                    // if the MessageReceiver is closed - we no-longer need to create the link
                    if (MessageReceiver.this.getIsClosingOrClosed()) {
                        
                        MessageReceiver.this.underlyingFactory.deregisterForConnectionError(MessageReceiver.this.receiveLink);
                        session.close();
                        return;
                    }
                    
                    final Source source = new Source();
                    source.setAddress(receivePath);

                    final Map<Symbol, UnknownDescribedType> filterMap = MessageReceiver.this.settingsProvider.getFilter(MessageReceiver.this.lastReceivedMessage);
                    if (filterMap != null)
                        source.setFilter(filterMap);
                    
                    final Receiver receiver = session.receiver(TrackingUtil.getLinkName(session));
                    receiver.setSource(source);
                    
                    final Target target = new Target();
                    
                    receiver.setTarget(target);

                    // use explicit settlement via dispositions (not pre-settled)
                    receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
                    receiver.setReceiverSettleMode(ReceiverSettleMode.SECOND);
                    
                    final Map<Symbol, Object> linkProperties = MessageReceiver.this.settingsProvider.getProperties();
                    if (linkProperties != null)
                        receiver.setProperties(linkProperties);

                    final Symbol[] desiredCapabilities = MessageReceiver.this.settingsProvider.getDesiredCapabilities();
                    if (desiredCapabilities != null)
                        receiver.setDesiredCapabilities(desiredCapabilities);
                    
                    final ReceiveLinkHandler handler = new ReceiveLinkHandler(MessageReceiver.this);
                    BaseHandler.setHandler(receiver, handler);
                    MessageReceiver.this.underlyingFactory.registerForConnectionError(receiver);

                    receiver.open();

                    if (MessageReceiver.this.receiveLink != null)
                    {
                            final Receiver oldReceiver = MessageReceiver.this.receiveLink;
                            MessageReceiver.this.underlyingFactory.deregisterForConnectionError(oldReceiver);
                    }

                    MessageReceiver.this.receiveLink = receiver;
                }
            };
            
            final Consumer<ErrorCondition> onSessionOpenFailed = new Consumer<ErrorCondition>()
            {
                @Override
                public void accept(ErrorCondition t)
                {
                    onError(t);
                }
            };
            
            try {
                this.underlyingFactory.getCBSChannel().sendToken(
                    this.underlyingFactory.getReactorScheduler(),
                    this.underlyingFactory.getTokenProvider().getToken(tokenAudience, ClientConstants.TOKEN_REFRESH_INTERVAL), 
                    tokenAudience, 
                    new IOperationResult<Void, Exception>() {
                        @Override
                        public void onComplete(Void result) {
                            if (MessageReceiver.this.getIsClosingOrClosed())
                                return;
                            
                            underlyingFactory.getSession(
                                    receivePath,
                                    onSessionOpen,
                                    onSessionOpenFailed);
                        }
                        @Override
                        public void onError(Exception error) {
                            MessageReceiver.this.onError(error);
                        }
                    });
            }
            catch(IOException|NoSuchAlgorithmException|InvalidKeyException|RuntimeException exception) {
                MessageReceiver.this.onError(exception);
            }
        }

	// CONTRACT: message should be delivered to the caller of MessageReceiver.receive() only via Poll on prefetchqueue
	private Message pollPrefetchQueue()
	{
		final Message message = this.prefetchedMessages.poll();
		if (message != null)
		{
			// message lastReceivedOffset should be up-to-date upon each poll - as recreateLink will depend on this 
			this.lastReceivedMessage = message;
			this.sendFlow(1);
		}

		return message;
	}

	private void sendFlow(final int credits)
	{
		// slow down sending the flow - to make the protocol less-chat'y
		this.nextCreditToFlow += credits;
		if (this.nextCreditToFlow >= this.prefetchCount || this.nextCreditToFlow >= 100)
		{
			final int tempFlow = this.nextCreditToFlow;
			this.receiveLink.flow(tempFlow);
			this.nextCreditToFlow = 0;
			
			if(TRACE_LOGGER.isLoggable(Level.FINE))
			{
				TRACE_LOGGER.log(Level.FINE, String.format("receiverPath[%s], linkname[%s], updated-link-credit[%s], sentCredits[%s]",
						this.receivePath, this.receiveLink.getName(), this.receiveLink.getCredit(), tempFlow));
			}
		}
	}

	private void scheduleLinkOpenTimeout(final TimeoutTracker timeout)
	{
		// timer to signal a timeout if exceeds the operationTimeout on MessagingFactory
		Timer.schedule(
				new Runnable()
				{
					public void run()
					{
						if (!linkOpen.getWork().isDone())
						{
							Exception operationTimedout = new TimeoutException(
									String.format(Locale.US, "%s operation on ReceiveLink(%s) to path(%s) timed out at %s.", "Open", MessageReceiver.this.receiveLink.getName(), MessageReceiver.this.receivePath, ZonedDateTime.now()),
									MessageReceiver.this.lastKnownLinkError);
							if (TRACE_LOGGER.isLoggable(Level.WARNING))
							{
								TRACE_LOGGER.log(Level.WARNING, 
										String.format(Locale.US, "receiverPath[%s], linkName[%s], %s call timedout", MessageReceiver.this.receivePath, MessageReceiver.this.receiveLink.getName(),  "Open"), 
										operationTimedout);
							}

							ExceptionUtil.completeExceptionally(linkOpen.getWork(), operationTimedout, MessageReceiver.this);
						}
					}
				}
				, timeout.remaining()
				, TimerType.OneTimeRun);
	}

	private void scheduleLinkCloseTimeout(final TimeoutTracker timeout)
	{
		// timer to signal a timeout if exceeds the operationTimeout on MessagingFactory
		Timer.schedule(
				new Runnable()
				{
					public void run()
					{
						if (!linkClose.isDone())
						{
							Exception operationTimedout = new TimeoutException(String.format(Locale.US, "%s operation on Receive Link(%s) timed out at %s", "Close", MessageReceiver.this.receiveLink.getName(), ZonedDateTime.now()));
							if (TRACE_LOGGER.isLoggable(Level.WARNING))
							{
								TRACE_LOGGER.log(Level.WARNING, 
										String.format(Locale.US, "receiverPath[%s], linkName[%s], %s call timedout", MessageReceiver.this.receivePath, MessageReceiver.this.receiveLink.getName(), "Close"), 
										operationTimedout);
							}

							ExceptionUtil.completeExceptionally(linkClose, operationTimedout, MessageReceiver.this);
						}
					}
				}
				, timeout.remaining()
				, TimerType.OneTimeRun);
	}

	@Override
	public void onClose(ErrorCondition condition)
	{
		if (condition == null || condition.getCondition() == null)
		{
			this.onError((Exception) null);
		}
		else
		{
			this.onError(condition);
		}
	}

	@Override
	public ErrorContext getContext()
	{
		final boolean isLinkOpened = this.linkOpen != null && this.linkOpen.getWork().isDone();
		final String referenceId = this.receiveLink != null && this.receiveLink.getRemoteProperties() != null && this.receiveLink.getRemoteProperties().containsKey(ClientConstants.TRACKING_ID_PROPERTY)
				? this.receiveLink.getRemoteProperties().get(ClientConstants.TRACKING_ID_PROPERTY).toString()
						: ((this.receiveLink != null) ? this.receiveLink.getName(): null);

		ReceiverContext errorContext = new ReceiverContext(this.underlyingFactory != null ? this.underlyingFactory.getHostName() : null,
				this.receivePath,
				referenceId,
				isLinkOpened ? this.prefetchCount : null, 
                                isLinkOpened && this.receiveLink != null ? this.receiveLink.getCredit(): null, 
				isLinkOpened && this.prefetchedMessages != null ? this.prefetchedMessages.size(): null);

		return errorContext;
	}	

	private static class ReceiveWorkItem extends WorkItem<Collection<Message>>
	{
		private final int maxMessageCount;

		public ReceiveWorkItem(CompletableFuture<Collection<Message>> completableFuture, Duration timeout, final int maxMessageCount)
		{
			super(completableFuture, timeout);
			this.maxMessageCount = maxMessageCount;
		}
	}

	@Override
	protected CompletableFuture<Void> onClose()
	{
            if (!this.getIsClosed())
            {
                try
                {
                    this.activeClientTokenManager.cancel();
                    this.underlyingFactory.scheduleOnReactorThread(new DispatchHandler()
                        {
                            @Override
                            public void onEvent()
                            {
                                if (receiveLink != null && receiveLink.getLocalState() != EndpointState.CLOSED)
                                {
                                    receiveLink.close();
                                    scheduleLinkCloseTimeout(TimeoutTracker.create(operationTimeout));
                                }
                                else if (receiveLink == null || receiveLink.getRemoteState() == EndpointState.CLOSED)
                                {
                                    linkClose.complete(null);
                                }
                            }
                        });
                }
                catch(IOException ioException)
                {
                    this.linkClose.completeExceptionally(new ServiceBusException(false, "Scheduling close failed with error. See cause for more details.", ioException));
                }
            }

            return this.linkClose;
	}
        
        private final class ReceiveWork extends DispatchHandler {
            
            @Override
            public void onEvent() {

                ReceiveWorkItem pendingReceive;
                while (!prefetchedMessages.isEmpty() && (pendingReceive = pendingReceives.poll()) != null) {
                    
                    if (pendingReceive.getWork() != null && !pendingReceive.getWork().isDone()) {
                        
                        Collection<Message> receivedMessages = receiveCore(pendingReceive.maxMessageCount);
                        pendingReceive.getWork().complete(receivedMessages);
                    }
                }
            }
        }
        
        private final class CreateAndReceive extends DispatchHandler {
            
            @Override
            public void onEvent() {
                
                receiveWork.onEvent();
                
                if (!MessageReceiver.this.getIsClosingOrClosed()
                    && (receiveLink.getLocalState() == EndpointState.CLOSED || receiveLink.getRemoteState() == EndpointState.CLOSED)) {
                        createReceiveLink();
                }
            }
        }
}
