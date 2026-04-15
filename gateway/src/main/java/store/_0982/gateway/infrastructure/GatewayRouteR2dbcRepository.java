package store._0982.gateway.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import store._0982.gateway.domain.GatewayRoute;
import store._0982.gateway.domain.GatewayRouteRepository;

import java.util.Optional;


@Repository
@RequiredArgsConstructor
public class GatewayRouteR2dbcRepository implements GatewayRouteRepository {

    private final GatewayRouteReactiveCrudRepository gatewayRouteReactiveCrudRepository;
    private final Cache<String, Optional<GatewayRoute>> gatewayRouteCache;

    @Override
    public Mono<GatewayRoute> findByMethodAndEndpoint(String httpMethod, String endpoint) {
        String cacheKey = cacheKey(httpMethod, endpoint);
        Optional<GatewayRoute> cached = gatewayRouteCache.getIfPresent(cacheKey);

        if (cached != null) {
            return toMono(cached);
        }

        return gatewayRouteReactiveCrudRepository.findByMethodAndEndpoint(httpMethod, endpoint)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .doOnNext(route -> gatewayRouteCache.put(cacheKey, route))
                .flatMap(this::toMono);
    }

    private String cacheKey(String httpMethod, String endpoint) {
        return httpMethod + " " + endpoint;
    }

    private Mono<GatewayRoute> toMono(Optional<GatewayRoute> route) {
        return route.map(Mono::just).orElseGet(Mono::empty);
    }
}
