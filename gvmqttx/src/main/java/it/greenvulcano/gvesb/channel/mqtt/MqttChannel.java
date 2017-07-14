/*******************************************************************************
 * Copyright (c) 2009, 2016 GreenVulcano ESB Open Source Project.
 * All rights reserved.
 *
 * This file is part of GreenVulcano ESB.
 *
 * GreenVulcano ESB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GreenVulcano ESB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with GreenVulcano ESB. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package it.greenvulcano.gvesb.channel.mqtt;


import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
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
	
	MqttChannel(String protocol, String host, int port, String username, char[] password, String id, String system) throws MqttException {
		this.id = id;
		this.system = system;
		String endpoint = protocol+"://"+host+":"+port;
		logger.debug(String.format("GVESB Creating MQTT channel %s/%s on endpoint %s ", system, id, endpoint));
		listeners = Collections.synchronizedSet(new LinkedHashSet<>());
		
		mqttClient = new MqttClient(endpoint, system+"/"+id);		
		mqttClient.setCallback(this);
		
		MqttConnectOptions connectOptions = new MqttConnectOptions();
		
		Optional.ofNullable(username).ifPresent(connectOptions::setUserName);
		Optional.ofNullable(password).ifPresent(connectOptions::setPassword);
		
		mqttClient.connect(connectOptions);		
		
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