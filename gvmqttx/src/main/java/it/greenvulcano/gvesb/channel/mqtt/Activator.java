package it.greenvulcano.gvesb.channel.mqtt;

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
			mqttChannel = new MqttChannel(XMLConfig.get(node, "@endpoint"), XMLConfig.get(node, "@id-channel"), XMLConfig.get(node.getParentNode(), "@id-system"));        		 
			
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