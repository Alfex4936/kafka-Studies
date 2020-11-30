class Config:
    MY_SERVER = "localhost:9092"
    TOPIC_ID = "first-topic"
    GROUP_ID = "group-one"

    CLIENT_ID = "client-1"
    SESSION_TIMEOUT_MS = 6000
    OFFSET_REST = "smallest"

    # Consumer
    SETTINGS = {
        "bootstrap.servers": MY_SERVER,
        "group.id": GROUP_ID,
        "client.id": CLIENT_ID,
        "enable.auto.commit": True,
        "session.timeout.ms": SESSION_TIMEOUT_MS,
        "default.topic.config": {"auto.offset.reset": OFFSET_REST},
    }
