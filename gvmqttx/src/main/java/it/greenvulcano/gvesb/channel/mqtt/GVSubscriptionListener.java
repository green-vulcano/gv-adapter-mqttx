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

import java.util.Objects;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;

import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.channel.mqtt.MqttChannel;
import it.greenvulcano.gvesb.core.GreenVulcano;
import it.greenvulcano.gvesb.core.exc.GVCoreException;
import it.greenvulcano.gvesb.core.pool.GreenVulcanoPool;
import it.greenvulcano.gvesb.core.pool.GreenVulcanoPoolManager;
import it.greenvulcano.gvesb.log.GVBufferMDC;

public class GVSubscriptionListener implements MqttChannel.SubscriptionListener {
	private transient final Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());	
	private transient final GreenVulcano greenVulcano;
	private final String topic, system, service, operation;
	private final int qos;
		
	public GVSubscriptionListener(String topic, int qos, String system, String service, String operation) throws GVCoreException {
		this.topic = Objects.requireNonNull(topic);
		this.qos = qos;
		this.system = Objects.requireNonNull(system);
		this.service = Objects.requireNonNull(service);
		this.operation = Objects.requireNonNull(operation);
		logger.debug(String.format("Creating channel listener for %s/%s/%s on %s (%s)",system, service, operation, topic, qos));
		this.greenVulcano = new GreenVulcano();
	}
		
	public String getSystem() {
		return system;
	}

	public String getService() {
		return service;
	}

	public String getOperation() {
		return operation;
	}

	@Override
	public String getTopic() {		
		return topic;
	}
	
	@Override
	public Integer getQoS() {
		return qos;
	}
	
	@Override
	public void stop() {
		greenVulcano.destroy(false);
		
	}
	
	@Override
	public void processMessage(String topic, MqttMessage message) {
	    if (isOfInterest(topic)) {
			try {
	        	
	            GVBuffer in = new GVBuffer(getSystem(), getService());
	            in.setObject(message.getPayload());
	            //in.setProperty("MQTT_SUBSCRIBER", subscriber);
	            in.setProperty("MQTT_TOPIC", topic);
	            in.setProperty("MQTT_QOS", String.valueOf(message.getQos()));
	            in.setProperty("MQTT_IS_DUPLICATE", message.isDuplicate() ? "Y" : "N");
	            in.setProperty("MQTT_IS_RETAINED", message.isRetained() ? "Y" : "N");
	
	            GVBufferMDC.put(in);
	            logger.debug("BEGIN Operation");	            
	            greenVulcano.forward(in, getOperation());	           
	            logger.debug("END Operation");
	        }  catch (Exception exc) {
	            logger.error("Error processing message", exc);
	        }
	    }
	}
	
	class GreenVulcanoTask implements Runnable {

		private final GreenVulcanoPool greenVulcano;
		private final MqttMessage message;
		
		GreenVulcanoTask(MqttMessage message) throws GVCoreException{
			this.greenVulcano = GreenVulcanoPoolManager.instance().getGreenVulcanoPool("gvmqtt").orElseGet(GreenVulcanoPoolManager::getDefaultGreenVulcanoPool);
			this.message = message;
		}
		
		@Override
		public void run() {
			try {
	        	
	            GVBuffer in = new GVBuffer(getSystem(), getService());
	            in.setObject(message.getPayload());
	            //in.setProperty("MQTT_SUBSCRIBER", subscriber);
	            in.setProperty("MQTT_TOPIC", getTopic());
	            in.setProperty("MQTT_QOS", String.valueOf(message.getQos()));
	            in.setProperty("MQTT_IS_DUPLICATE", message.isDuplicate() ? "Y" : "N");
	            in.setProperty("MQTT_IS_RETAINED", message.isRetained() ? "Y" : "N");

	            GVBufferMDC.put(in);
	            logger.debug("BEGIN Operation");	            
	            greenVulcano.forward(in, getOperation());	           
	            logger.debug("END Operation");
	        }  catch (Exception exc) {
	            logger.error("Error processing message", exc);
	        }			
		}
		
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((operation == null) ? 0 : operation.hashCode());
		result = prime * result + ((service == null) ? 0 : service.hashCode());
		result = prime * result + ((system == null) ? 0 : system.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GVSubscriptionListener other = (GVSubscriptionListener) obj;
		if (operation == null) {
			if (other.operation != null)
				return false;
		} else if (!operation.equals(other.operation))
			return false;
		if (service == null) {
			if (other.service != null)
				return false;
		} else if (!service.equals(other.service))
			return false;
		if (system == null) {
			if (other.system != null)
				return false;
		} else if (!system.equals(other.system))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "GVMqttChannelListener "
				+ "[topic=" + topic + ", system=" 
				+ system + ", service=" 
				+ service + ", operation="
				+ operation + "]";
	}
	
	private boolean isOfInterest(String topic) {
		String pattern = this.topic.replace("+", "[^/]+").replace("/#", "(/.*|$)");		
		return this.topic.equals(topic) || topic.matches(pattern)  ;
	}

}
