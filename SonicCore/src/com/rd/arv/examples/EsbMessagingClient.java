package com.rd.arv.examples;

import java.util.Optional;
import java.util.Arrays;
import java.util.List;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

public class EsbMessagingClient {

	private EsbMessageSender sender;
	private EsbMessageReceiver receiver;

	public EsbMessagingClient(String brokerUrl, String username, String password,int timeToLiveMilliseconds) {
		this.sender = new EsbMessageSender(brokerUrl, username, password, timeToLiveMilliseconds);
		this.receiver = new EsbMessageReceiver(brokerUrl, username, password);
	}
	public void send(final String message,final String... destinations) {
		this.sender.send(message, destinations);
	}
	public Optional<TextMessage> subscribeAndWait(final String destination, final int timeout) {
		return this.receiver.subscribeAndWait(destination, timeout);
	}
}
class EsbMessageSender {

	private final String brokerUrl;
	private final String username ;
	private final String password;

	public EsbMessageReceiver() {
	}
	public EsbMessageReceiver(String brokerUrl, String username, String password) {
		this.brokerUrl = brokerUrl;
		this.username = username;
		this.password = password;
	}

	public Optional<TextMessage> subscribeAndWait(final String destination, final int timeout) {
		try {
			ConnectionFactory connectionFactory = new progress.message.jclient.ConnectionFactory(this.brokerUrl);
			// try with resources will implement the auto-closeables and ensure session and
			// connection are closed
			try (Connection connection = connectionFactory.createConnection(this.username, this.password);
		
				Session session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
				MessageConsumer consumer = session.createConsumer(session.createTemporaryTopic(destination));) {
				//Starts (or restarts) a connection's delivery of incoming messages. 
				//A call to start on a connection that has already been started is ignored.
				connection.start();
			  return	Optional.of(consumer.receive(timeout)).map(msg -> (TextMessage) msg).orElse(Optional.empty());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return	Optional.empty();
	}
}
class EsbMessageSender {

	private final String brokerUrl;
	private final String username ;
	private final String password;
	// time in milliseconds
	private final int timeToLiveMilliseconds ;

	public EsbMessageSender() {
	}
	public EsbMessageSender(String brokerUrl, String username, String password, int timeToLiveMilliseconds) {
		this.brokerUrl = brokerUrl;
		this.username = username;
		this.password = password;
		this.timeToLiveMilliseconds = timeToLiveMilliseconds;
	}

	public void send(final String message,final String... destinations) {
		try {
			if(destinations == null || destinations.length == 0 || message == null) {
				return;
			}

			ConnectionFactory connectionFactory = new progress.message.jclient.ConnectionFactory(this.brokerUrl);
			// try with resources will implement the auto-closeables and ensure session and
			// connection are closed
			try (Connection connection = connectionFactory.createConnection(this.username, this.password);
					Session session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE)) {

				TextMessage msg = session.createTextMessage();
				msg.setBooleanProperty(progress.message.jclient.Constants.PRESERVE_UNDELIVERED, false);
				msg.setText(msg);
				for (String destination : destinations) {
					try (MessageProducer producer = session.createProducer(session.createTopic(destination))){
						producer.send(msg, javax.jms.DeliveryMode.NON_PERSISTENT, javax.jms.Message.DEFAULT_PRIORITY,this.timeToLiveMilliseconds);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				//Starts (or restarts) a connection's delivery of incoming messages. 
				//A call to start on a connection that has already been started is ignored.
				connection.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}