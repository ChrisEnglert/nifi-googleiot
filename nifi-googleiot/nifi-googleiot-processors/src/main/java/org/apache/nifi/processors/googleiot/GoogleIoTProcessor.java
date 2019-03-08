package org.apache.nifi.processors.googleiot;


import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.util.*;
import java.util.concurrent.TimeUnit;


@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"publish", "MQTT", "IOT", "Google"})
@CapabilityDescription("Publishes a message to an MQTT topic")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@TriggerSerially
@WritesAttributes({
@WritesAttribute(attribute = GoogleIoTProcessor.TopicAttribute, description = "Google IoT topic '/device/{deviceId}/{topic}'")})
@DynamicProperty(name = "Received FlowFile attribute name",
        value = "Received FlowFile attribute value",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Specifies an attribute on received FlowFiles defined by the Dynamic Property's key and value.")
public class GoogleIoTProcessor extends AbstractProcessor {

    private ComponentLog logger;
    private GoogleIoTDeviceClient client;

    public static final String TopicAttribute = "googleiot.topic";

    public static final PropertyDescriptor PROP_PROJECT = new PropertyDescriptor.Builder()
            .name("project")
            .displayName("Project")
            .description("The Google IoT projectid")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_REGION = new PropertyDescriptor.Builder()
            .name("region")
            .displayName("Region")
            .description("The Google IoT region")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .allowableValues("us-central1", "europe-west1", "asia-east1")
            .build();

    public static final PropertyDescriptor PROP_REGISTRY = new PropertyDescriptor.Builder()
            .name("registry")
            .displayName("Registry")
            .description("The Google IoT registry")
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
            .description("File path to the private key file in PKCS8 DER format")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("rsa_private.der")
            .build();

    public static final PropertyDescriptor PROP_TOPIC = new PropertyDescriptor.Builder()
            .name("topic")
            .description("Topic supports 'events' or 'state'")
            .defaultValue("events")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are transferred to this relationship.")
            .build();

    public static final Relationship REL_RECEIVED = new Relationship.Builder()
            .name("received")
            .description("From command or config topic")
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
        innerRelationshipsSet.add(REL_RECEIVED);
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
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
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

            logger.info("Starting Google IoT Device : " + config.getDeviceURL());

            client.onScheduled(config);
        } catch (Exception e) {
            logger.error("failed to start", e);
            throw e;
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        client.onStopped();
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final boolean received = receive(context, session);

        final boolean published = publish(context, session);

        if (!received && !published) {
            context.yield();
        }
    }

    private boolean receive(ProcessContext context, ProcessSession session) {
        Pair<String, byte[]> message = client.receive();
        if (message == null) {
            return false;
        }

        FlowFile flowFile = session.create();
        session.putAttribute(flowFile, GoogleIoTProcessor.TopicAttribute, message.getKey());

        session.write(flowFile, outputStream -> outputStream.write( message.getValue() ));
        session.getProvenanceReporter().receive(flowFile, "Google IoT :" + message.getKey());
        session.transfer(flowFile, REL_RECEIVED);

        return true;
    }

    private boolean publish(ProcessContext context, ProcessSession session) {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return false;
        }

        final byte[] mqttMessage = getMessage(session, flowFile);
        final String topic = context.getProperty(PROP_TOPIC).evaluateAttributeExpressions(flowFile).getValue();

        flowFile = session.putAllAttributes(flowFile, getDynamicPropertys(context));

        final StopWatch stopWatch = new StopWatch(true);

        final Boolean res = client.tryPublish(mqttMessage, topic);

        session.getProvenanceReporter().send(flowFile, "Google IoT", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(flowFile, REL_SUCCESS);

        return true;
    }


    private static byte[] getMessage(ProcessSession session, FlowFile flowfile) {
        final byte[] messageContent = new byte[(int) flowfile.getSize()];
        session.read(flowfile, in -> StreamUtils.fillBuffer(in, messageContent, true));
        return messageContent;
    }

    private static Map<String,String> getDynamicPropertys(ProcessContext context) {
        Map<PropertyDescriptor, String> processorProperties = context.getProperties();
        Map<String, String> generatedAttributes = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String dynamicValue = context.getProperty(property).evaluateAttributeExpressions().getValue();
                generatedAttributes.put(property.getName(), dynamicValue);
            }
        }
        return generatedAttributes;
    }
}
