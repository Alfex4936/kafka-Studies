use rdkafka::config::ClientConfig;

use rdkafka::consumer::{base_consumer::BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use std::str;
use std::thread::sleep;
use std::time::Duration;

const CLOUDKARAFKA_USERNAME: &str = env!("CLOUDKARAFKA_USERNAME");
const CLOUDKARAFKA_PASSWORD: &str = env!("CLOUDKARAFKA_PASSWORD");
const CLOUDKARAFKA_BROKERS: &str = env!("CLOUDKARAFKA_BROKERS");
const CLOUDKARAFKA_TOPIC: &str = env!("CLOUDKARAFKA_TOPIC");

fn main() {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", CLOUDKARAFKA_BROKERS)
        .set("group.id", &format!("{}-consumer", CLOUDKARAFKA_USERNAME))
        .set("auto.offset.reset", "largest")
        .set("session.timeout.ms", "6000")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "SCRAM-SHA-256")
        .set("sasl.username", CLOUDKARAFKA_USERNAME)
        .set("sasl.password", CLOUDKARAFKA_PASSWORD)
        .create()
        .expect("Consumer creation error");

    consumer
        .subscribe(&[CLOUDKARAFKA_TOPIC])
        .expect("Can't subscribe to specified topics");

    'consumer: loop {
        println!("Polling...");
        let message = consumer.poll(Duration::from_secs(1));
        match message {
            Some(Ok(msg)) => {
                let msg_content: &str = str::from_utf8(msg.payload().unwrap()).unwrap(); // value as &str

                // Typed msg
                // let notice_parsed: Notice = match serde_json::from_str(msg_content) {
                //     Ok(yes) => yes,
                //     _ => {
                //         sleep(Duration::from_secs(300));
                //         continue 'consumer;
                //     }
                // };
                println!("Payload: {:#?}", msg_content);
            }
            _ => {}
        }
    }
}
