#!/bin/bash
openssl genrsa -out rsa_private.pem 2048
openssl rsa -in rsa_private.pem -pubout -out rsa_public.pem
openssl pkcs8 -nocrypt -topk8 -inform PEM -in rsa_private.pem -outform DER -out rsa_private.der