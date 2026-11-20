package io.eventscale;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EventScaleApplication {
    public static void main(String[] args) { SpringApplication.run(EventScaleApplication.class, args); }
}
