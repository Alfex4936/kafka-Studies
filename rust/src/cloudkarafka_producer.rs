use std::thread::sleep;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

const CLOUDKARAFKA_USERNAME: &str = env!("CLOUDKARAFKA_USERNAME");
const CLOUDKARAFKA_PASSWORD: &str = env!("CLOUDKARAFKA_PASSWORD");
const CLOUDKARAFKA_BROKERS: &str = env!("CLOUDKARAFKA_BROKERS");
const CLOUDKARAFKA_TOPIC: &str = env!("CLOUDKARAFKA_TOPIC");

fn main() {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", CLOUDKARAFKA_BROKERS)
        .set("session.timeout.ms", "6000")
        .set("compression.type", "snappy")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "SCRAM-SHA-256")
        .set("sasl.username", CLOUDKARAFKA_USERNAME)
        .set("sasl.password", CLOUDKARAFKA_PASSWORD)
        .create()
        .expect("Producer creation error");

    'producer: loop {
        producer
            .send(
                BaseRecord::to(CLOUDKARAFKA_TOPIC)
                    .payload("this is value")
                    .key("test"),
            )
            .expect("Failed to enqueue");

        producer.poll(Duration::from_millis(100));

        // And/or flush the producer before dropping it.
        producer.flush(Duration::from_millis(100));

        sleep(Duration::from_secs(5));
    }
}
