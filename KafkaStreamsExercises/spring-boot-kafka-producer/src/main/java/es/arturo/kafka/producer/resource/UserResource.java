package es.arturo.kafka.producer.resource;

import es.arturo.kafka.producer.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class UserResource {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    private static final String TOPIC = "kafka-spring-cloud";

    @GetMapping("/publish/{name}/{dept}/{salary}")
    public String post(@PathVariable("name") final String name,
                       @PathVariable("dept") final String dept,
                       @PathVariable("salary") final Long salary) {

        kafkaTemplate.send(TOPIC, new User(name, dept, salary));

        return "Published successfully";
    }
}
