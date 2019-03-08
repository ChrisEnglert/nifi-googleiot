# nifi-googleiot

Google IoT Device as Apache NiFi Processor 

- Supports MQTT publish / subscribe
- Implements RSA key handling 
- JWT as MQTT password


https://console.cloud.google.com/iot

https://cloud.google.com/iot/docs/how-tos/


Can use https://github.com/ChrisEnglert/nifi-googleiot/blob/master/nifi-googleiot-processors/src/test/resources/createkey.sh
to create a private-key in DER format and a public-key from it.

The private key file must be placed in the nifi instance and specified in the Processor's attributes
The public key must be must be added as RS256 key for the device in google IoT
