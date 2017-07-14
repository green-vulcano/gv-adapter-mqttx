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

import java.net.URI;
import java.util.List;
import java.util.Optional;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.virtual.OperationFactory;
import it.greenvulcano.gvesb.virtual.mqtt.MQTTPublisherCallOperation;

public class Activator implements BundleActivator {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
    public void start(BundleContext context) {
        logger.debug("Starting bundle GVESB MQTT Channel");
        
        OperationFactory.registerSupplier("mqtt-publish-call", MQTTPublisherCallOperation::new);
        try {
        	
			NodeList mqttChannelList = XMLConfig.getNodeList("GVSystems.xml","//Channel[@type='MQTTAdapter' and @enabled='true']");
			logger.debug("Found "+ mqttChannelList.getLength() + " MQTT Channel");
			List<MqttChannel> channels = IntStream.range(0, mqttChannelList.getLength())
									         .mapToObj(mqttChannelList::item)
									         .map(this::buildChannel)
									         .filter(Optional::isPresent)
									         .map(Optional::get)
									         .collect(Collectors.toList());
				
		 
			MqttChannel.getChannels().addAll(channels);
			
		 } catch (XMLConfigException e) {
			 logger.error("GVESB MQTT channel setup error", e);
		}        
        
    }

    public void stop(BundleContext context) {
    	logger.debug("Stopping bundle GVESB MQTT Channel");
    	MqttChannel.getChannels().stream().forEach(MqttChannel::dismiss);
    	MqttChannel.getChannels().clear();
    }
    
    private Optional<MqttChannel> buildChannel(Node node) {
		 MqttChannel mqttChannel = null;
		 try {
			 
			URI endpoint = URI.create(XMLConfig.get(node, "@endpoint"));
			
			String userInfo = endpoint.getRawUserInfo();
			
			String username = null;
			char[] password = null;
			if (userInfo!=null) {
				String[] credentials = userInfo.split(":");
				username =  credentials[0];
				if(credentials.length>1) {
					password = credentials[1].toCharArray();	
				}
				
			}
			
			mqttChannel = new MqttChannel(endpoint.getScheme(), endpoint.getHost(), endpoint.getPort(), username, password,  XMLConfig.get(node, "@id-channel"), XMLConfig.get(node.getParentNode(), "@id-system"));        		 
			
			NodeList listeners = XMLConfig.getNodeList(node, "./mqtt-subscribe-listener");
			logger.debug("Found "+ listeners.getLength() + " Listenter for channel "+mqttChannel.getSystem()+"/"+mqttChannel.getId());
			
			IntStream.range(0, listeners.getLength())
			     	 .mapToObj(listeners::item)
			     	 .map(this::buildListener)
			     	 .filter(Optional::isPresent)
			     	 .map(Optional::get)
			     	 .forEach(mqttChannel::registerListener);			
			
		 } catch (MqttException | XMLConfigException e) {
			 logger.error("GVESB MQTT channel configuration error", e);
		 }
					        	 
		return Optional.ofNullable(mqttChannel);
    }
    
    private Optional<GVSubscriptionListener> buildListener(Node node) {
    	GVSubscriptionListener listener = null;
    	try {
    		listener = new GVSubscriptionListener(XMLConfig.get(node, "@topic"), XMLConfig.getInteger(node, "@qos"), 
						    				      XMLConfig.get(node.getParentNode().getParentNode(), "@id-system"), 
						    				      XMLConfig.get(node, "@gv-service"), XMLConfig.get(node, "@gv-operation"));    		
    		
    	} catch (Exception e) {
    		logger.error("GVESB MQTT listener configuration error", e);
		}
					        	 
		return Optional.ofNullable(listener);
    }

}