# Connect Archiver SMT

A pair of Kafka Connect Single Message Transforms to wrap a record's 
metadata a value and unwrap it again.

Can be used, e.g., for backup/restore to an object store such as AWS S3 or GCP GCS.

### Installation

The following two packages need to be compiled locally for the integration tests in the `src/intTest` folder to work:

* https://github.com/christophschubert/cp-testcontainers
* https://github.com/christophschubert/kafka-connect-java-client

Based on 
References:


- https://www.confluent.io/blog/kafka-connect-single-message-transformation-tutorial-with-examples