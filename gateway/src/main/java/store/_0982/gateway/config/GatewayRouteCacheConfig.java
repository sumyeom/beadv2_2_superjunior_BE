package store._0982.gateway.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import store._0982.gateway.domain.GatewayRoute;

import java.time.Duration;
import java.util.Optional;

@Configuration
public class GatewayRouteCacheConfig {

    @Bean
    public Cache<String, Optional<GatewayRoute>> gatewayRouteCache() {
        return Caffeine.newBuilder()
                .maximumSize(1_000)
                .expireAfterWrite(Duration.ofMinutes(5))
                .build();
    }
}
