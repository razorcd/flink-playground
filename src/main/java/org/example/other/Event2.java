package org.example.other;

public class Event2 {
    public String event_id;
    public String user_id;

    public Event2() {}

    public Event2(String event_id, String user_id) {
        this.event_id = event_id;
        this.user_id = user_id;
    }

    @Override
    public String toString() {
        return "Event2{" +
                "event_id='" + event_id + '\'' +
                ", user_id='" + user_id + '\'' +
                '}';
    }
}