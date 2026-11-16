package cl.duoc.adminservice.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({AdminProperties.class, StreamAdminProperties.class})
public class AdminConfig {
}
