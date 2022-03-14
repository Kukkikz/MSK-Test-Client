# MSK-Test-Client

This demo show how consumer and producer application connect to Kafka (MSK) using Java. It contains 4 Classes

- Consumer
- ConsumerAvro
- Producer
- ProducerAvro

## How to run

You need to add file `kafka.config` in src/main/resource folder the have SASL credential to MSK. For example

```
saslUsername=alice
saslPassword=alice-secret
```

You also need IAM credential setup in your local machine (See how to setup in https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/setup.html#setup-credentials)
