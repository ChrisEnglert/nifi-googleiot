package org.apache.nifi.processors.googleiot;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;


public class GoogleIoTDeviceProcessorTests {

    final TestRunner runner = TestRunners.newTestRunner(new GoogleIoTProcessor());

    Properties getProperties() {
        Properties prop = new Properties();
        try {
            String resource = this.getClass().getSimpleName() + ".properties";
            prop.load( ClassLoader.getSystemResourceAsStream(resource));
        } catch (IOException e) {
            return null;
        }

        return prop;
    }

    private GoogleIoTDeviceConfig getConfig() {
        Properties prop = getProperties();
        Assert.assertNotNull(prop);

        return GoogleIoTDeviceConfig.apply(prop);
    }

    private String getResourceFilePath(String filename) {
        return new File("src/test/resources/" + filename).getAbsolutePath();
    }

    @Test
    public void test() {

        GoogleIoTDeviceConfig config = getConfig();


        runner.setProperty(GoogleIoTProcessor.PROP_REGION, config.getRegion());
        runner.setProperty(GoogleIoTProcessor.PROP_PROJECT, config.getProjectId());
        runner.setProperty(GoogleIoTProcessor.PROP_DEVICEID, config.getDeviceId());
        runner.setProperty(GoogleIoTProcessor.PROP_REGISTRY, config.getRegistryId());
        runner.setProperty(GoogleIoTProcessor.PROP_PRIVATEKEYFILE, getResourceFilePath("rsa_private.der"));

        runner.assertValid();


        runner.enqueue("[]", new HashMap<String,String>() {{
                put(CoreAttributes.FILENAME.key(), "test");
            }});

        runner.setRunSchedule(1000);
        runner.run(10);


        List<MockFlowFile> success = runner.getFlowFilesForRelationship(GoogleIoTProcessor.REL_SUCCESS);
        List<MockFlowFile> received = runner.getFlowFilesForRelationship(GoogleIoTProcessor.REL_RECEIVED);

        Assert.assertEquals(1, success.size());

        runner.shutdown();
    }


}
