package org.apache.nifi.processors.googleiot;

import java.util.Properties;

public class GoogleIoTDeviceConfig {

    public GoogleIoTDeviceConfig(
            String deviceId,
            String projectId,
            String region,
            String registryId,
            String privateKeyFile,
            String algorithm
    ) {
        this.deviceId = deviceId;
        this.projectId = projectId;
        this.region = region;
        this.registryId = registryId;
        this.privateKeyFile = privateKeyFile;
        this.algorithm = algorithm;
    }

    private final String deviceId;
    private final String projectId;
    private final String region;
    private final String registryId;

    private final String privateKeyFile;
    private final String algorithm;

    public String getDeviceId() {
        return deviceId;
    }

    public String getRegion() {
        return region;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getRegistryId() {
        return registryId;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getPrivateKeyFile() {
        return privateKeyFile;
    }

    public String getDeviceURL() {
        return String.format("https://console.cloud.google.com/iot/locations/%s/registries/%s/devices/%s?project=%s", region, registryId, deviceId, projectId);
    }

    public static GoogleIoTDeviceConfig apply(Properties prop) {
        GoogleIoTDeviceConfig config = new GoogleIoTDeviceConfig(
            prop.getProperty("deviceId"),
            prop.getProperty("projectId"),
            prop.getProperty("region"),
            prop.getProperty("registryId"),
            prop.getProperty("privatekeyfile"),
            prop.getProperty("algorithm")
        );
        return config;
    }
}
