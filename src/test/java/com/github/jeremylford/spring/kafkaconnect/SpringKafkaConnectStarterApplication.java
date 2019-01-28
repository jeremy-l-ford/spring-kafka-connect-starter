package com.github.jeremylford.spring.kafkaconnect;

import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectProperties;
import org.junit.Ignore;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootApplication
//@EnableConfigurationProperties
//@Import(ContextRefreshedListener.class)
@Ignore
public class SpringKafkaConnectStarterApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaConnectStarterApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(KafkaConnectProperties kafkaConnectProperties) {
		return new CommandLineRunner() {
			@Override
			public void run(String... args) throws Exception {
				System.out.println(kafkaConnectProperties);
			}
		};
	}


}

