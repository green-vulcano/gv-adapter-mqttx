package it.greenvulcano.gvesb.channel.mqtt;


import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttChannel implements MqttCallback {
	
	private final static List<MqttChannel> channels;
	
	static {
		channels = Collections.synchronizedList(new LinkedList<>());
	}
	
	public synchronized static List<MqttChannel> getChannels() {
		return channels;
	}
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final String id, system;
	private final MqttClient mqttClient;		
	private final Set<SubscriptionListener> listeners;
	
	MqttChannel(String endpoint, String id, String system) throws MqttException {
		this.id = id;
		this.system = system;
		logger.debug(String.format("GVESB Creating MQTT channel %s/%s on endpoint %s ", system, id, endpoint));
		listeners = Collections.synchronizedSet(new LinkedHashSet<>());
		
		mqttClient = new MqttClient(endpoint, system+"/"+id);		
		mqttClient.setCallback(this);
		mqttClient.connect();		
		
	}
	
	public String getId(){
		return id;
	}
	
	public String getSystem(){
		return system;
	}
	
	synchronized void registerListener(SubscriptionListener listener) {
		try {
			 if (!mqttClient.isConnected()) {
					mqttClient.connect();
				}
		
			logger.debug(String.format("GVESB MQTT channel %s/%s subcribed to %s", system, id, listener.getTopic()));
			listeners.add(listener);
			
			Map<String, Integer> topics = listeners.stream().collect(Collectors.toMap(SubscriptionListener::getTopic, SubscriptionListener::getQoS));
						
			mqttClient.subscribe(topics.keySet().toArray(new String[]{}), topics.values().stream().mapToInt(Integer::intValue).toArray());
		} catch (MqttException exception) {
			logger.error(String.format("GVESB MQTT channel %s/%s subscribe error on", system, id, listener.getTopic()), exception);
		}
	}
	
	synchronized void unregisterListener(String topic) {
		try {			
			logger.debug(String.format("GVESB MQTT channel %s/%s unsubcribe to %s", system, id, topic));
			mqttClient.unsubscribe(topic);
			List<SubscriptionListener> unwanted = listeners.stream()
					.filter(l->l.getTopic().equals(topic))
					.collect(Collectors.toList());
			
			listeners.removeAll(unwanted);
		} catch (MqttException exception) {
			logger.error(String.format("GVESB MQTT channel %s/%s unsubscribe error", system, id), exception);
		}	
	}
	
	public void publish(String topic, byte[] payload, int qos) throws MqttException  {
		       
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        if (!mqttClient.isConnected()) {
			mqttClient.connect();
		}
        mqttClient.publish(topic, message);		        
		    
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {		
		
		synchronized (listeners) {
			listeners.stream().forEach(l->l.processMessage(topic, message));
		}		
	}	
	
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// do nothing		
	}
	
	@Override
	public void connectionLost(Throwable cause) {
		logger.error(String.format("GVESB MQTT channel %s/%s connection lost", system, id), cause);		
	}
	
	void dismiss() {
		try {
			if (mqttClient.isConnected()) {
				mqttClient.disconnect();
			}
			listeners.forEach(SubscriptionListener::stop);
			listeners.clear();
			mqttClient.close();
		} catch (MqttException e) {
			logger.error(String.format("GVESB MQTT channel %s/%s dismission error", system, id), e);
		}
	}	
		
	public interface SubscriptionListener {
		
		String getTopic();
		
		Integer getQoS();
				
		void stop();
		
		void processMessage(String topic, MqttMessage message);

	}
}