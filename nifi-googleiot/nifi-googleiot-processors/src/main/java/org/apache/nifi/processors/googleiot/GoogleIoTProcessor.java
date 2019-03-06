package org.apache.nifi.processors.googleiot;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"publish", "MQTT", "IOT", "Google"})
@CapabilityDescription("Publishes a message to an MQTT topic")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class GoogleIoTProcessor extends AbstractProcessor {

    private ComponentLog logger;
    private GoogleIoTDeviceClient client;

    public static final PropertyDescriptor PROP_PROJECT = new PropertyDescriptor.Builder()
            .name("project")
            .description("The topic to publish the message to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_REGION = new PropertyDescriptor.Builder()
            .name("region")
            .description("The topic to publish the message to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_REGISTRY = new PropertyDescriptor.Builder()
            .name("registry")
            .description("The topic to publish the message to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_DEVICEID = new PropertyDescriptor.Builder()
            .name("deviceid")
            .description("The topic to publish the message to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PRIVATEKEYFILE = new PropertyDescriptor.Builder()
            .name("privatekeyfile")
            .description("The topic to publish the message to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("rsa_private.pem")
            .build();

    public static final PropertyDescriptor PROP_TOPIC = new PropertyDescriptor.Builder()
            .name("messagetype")
            .description("default: 'events' / 'state'")
            .defaultValue("events")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are transferred to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are transferred to this relationship.")
            .build();

    public static final Relationship REL_CONFIG = new Relationship.Builder()
            .name("config")
            .description("From config topic")
            .build();

    public static final Relationship REL_COMMANDS = new Relationship.Builder()
            .name("commands")
            .description("From command topic")
            .build();

    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;

    static {
        final List<PropertyDescriptor> innerDescriptorsList = new ArrayList<>();
        innerDescriptorsList.add(PROP_PROJECT);
        innerDescriptorsList.add(PROP_REGISTRY);
        innerDescriptorsList.add(PROP_REGION);
        innerDescriptorsList.add(PROP_DEVICEID);
        innerDescriptorsList.add(PROP_PRIVATEKEYFILE);
        innerDescriptorsList.add(PROP_TOPIC);
        descriptors = Collections.unmodifiableList(innerDescriptorsList);

        final Set<Relationship> innerRelationshipsSet = new HashSet<>();
        innerRelationshipsSet.add(REL_SUCCESS);
        innerRelationshipsSet.add(REL_FAILURE);
        innerRelationshipsSet.add(REL_CONFIG);
        innerRelationshipsSet.add(REL_COMMANDS);
        relationships = Collections.unmodifiableSet(innerRelationshipsSet);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {

        if (client == null) {
            client = new GoogleIoTDeviceClient(logger);
        }

        try {

            GoogleIoTDeviceConfig config = new GoogleIoTDeviceConfig(
                    context.getProperty(PROP_DEVICEID).getValue(),
                    context.getProperty(PROP_PROJECT).evaluateAttributeExpressions().getValue(),
                    context.getProperty(PROP_REGION).evaluateAttributeExpressions().getValue(),
                    context.getProperty(PROP_REGISTRY).evaluateAttributeExpressions().getValue(),
                    context.getProperty(PROP_PRIVATEKEYFILE).getValue(),
                    "RS256"
            );

            client.onScheduled(config);
        } catch (Exception e) {
            logger.error("failed to start", e);
            throw e;
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        synchronized (this) {
            client.onStopped();
        }
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowfile = session.get();
        if (flowfile == null) {
            return;
        }


        MqttMessage mqttMessage = getMessage(session, flowfile);
        String messageType = context.getProperty(PROP_TOPIC).evaluateAttributeExpressions(flowfile).getValue();

        try {
            final StopWatch stopWatch = new StopWatch(true);

            client.tryPublish(mqttMessage, messageType);

            session.getProvenanceReporter().send(flowfile, "Google IoT", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowfile, REL_SUCCESS);
        } catch(Exception me) {
            logger.error("Failed to publish message.", me);
            session.transfer(flowfile, REL_FAILURE);
        }
    }

    private static MqttMessage getMessage(ProcessSession session, FlowFile flowfile) {
        final byte[] messageContent = new byte[(int) flowfile.getSize()];
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });

        final MqttMessage mqttMessage = new MqttMessage(messageContent);
        return mqttMessage;
    }
}
