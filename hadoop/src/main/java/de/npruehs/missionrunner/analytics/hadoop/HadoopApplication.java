package de.npruehs.missionrunner.analytics.hadoop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class HadoopApplication {

	public static void main(String[] args) {
		SpringApplication.run(HadoopApplication.class, args);
	}

	@Bean
	public AnalyticsFileProcessor analyticsFileProcessor() {
		return new AnalyticsFileProcessor();
	}
}
