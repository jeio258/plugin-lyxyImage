package cc.lyxy.image.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import reactor.netty.http.client.HttpClient;

@Configuration
public class WebClientConfig {
    @Bean
    public WebClient.Builder webClientBuilder() {
        // 配置 HttpClient：跟随重定向（图源常用 302 跳转到真实图片）+ 超时
        HttpClient httpClient = HttpClient.create()
            .followRedirect(true)
            .responseTimeout(java.time.Duration.ofSeconds(180))
            .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000);
        
        return WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .codecs(configurer -> configurer
                .defaultCodecs()
                .maxInMemorySize(16 * 1024 * 1024)); // 设置为 16MB
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
} 