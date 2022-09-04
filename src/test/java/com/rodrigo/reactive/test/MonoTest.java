package com.rodrigo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
public class MonoTest {

    @Test
    public void monoSubscriber() {
        String name = "Rodrigo";
        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe();

        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumer() {
        String name = "Rodrigo";
        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe(s -> log.info("Value {}", s));

        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerError() {
        String name = "Rodrigo";
        Mono<String> mono = Mono.just(name)
                .map(s -> {
                    throw new RuntimeException("Testando mono com error");
                });

        mono.subscribe(s -> log.info("Name {}", s), s -> log.error("Deu ruim"));
        mono.subscribe(s -> log.info("Name {}", s), Throwable::printStackTrace);

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    public void monoSubscriberConsumerComplete() {
        String name = "Rodrigo";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
                () -> log.info("FINISHED!"));

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerSubscription() {
        String name = "Rodrigo";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"),
                s -> s.request(5));

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoDoOnMethods() {
        String name = "Rodrigo";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(s -> log.info("Subscribou"))
                .doOnRequest(longNum -> log.info("Resquest recebido, começou a fazer algo..."))
                .doOnNext(s -> log.info("Valor encontrado. Executando doOnNext {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Valor encontrado. Executando doOnNext {}", s))
                .doOnSuccess(s -> log.info("doOnSucces executou {}", s));

        mono.subscribe(s -> log.info("Value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));
    }

    @Test
    public void monoDoOnError() {
        String name = "Rodrigo Eduardo";
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal Argument error"))
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .onErrorResume(s -> {
                    log.info("Dentro do resume error");
                    return Mono.just(name);
                })
                .log();

        StepVerifier.create(error)
                .expectNext(name)
                .verifyComplete();
    }
}
