package com.rodrigo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class OperatorsTest {
    @Test
    public void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("1-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // v VAI AFETAR TODA A CHAIN (todas as operações irão acontecer nesta thread) v
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    public void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("1-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // VAI AFETAR APENAS OS OPERATORS ABAIXO
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    public void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                // APENAS O PRIMEIRO SUBSCRIBEON SERÁ LEVADO EM CONTA
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("1-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // v SERÁ IGNORADO v
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    public void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                // VAI AFETAR APENAS OS OPERATORS ABAIXO ATÉ O PROXIMO PUBLISHON
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("1-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // VAI AFETAR APENAS OS OPERATORS ABAIXO
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    public void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                // VAI AFETAR TODOS OS OPERATORS ABAIXO
                // (PORÉM SE TROCAR A ORDEM, O SUBSCRIBEON TERÁ PREFERENCIA E
                // O PUBLISHON ATUARÁ NOS OPERATORS ABAIXO DELE)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("1-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // VAI SER IGNORADO
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2-thread {}-número {}", Thread.currentThread().getName(), i);
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    public void subscribeOnIO() throws Exception {
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

//        list.subscribe(s -> log.info("{}", s));

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    Assertions.assertFalse(l.isEmpty());
                    log.info("Tamanho {}", l.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void switchIfEmptyOperator() {
        Flux<Object> flux = emptyFlux()
                // funciona como um fallback para se o publisher (no caso "flux") for vazio
                .switchIfEmpty(Flux.just("não sou vazio"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("não sou vazio")
                .verifyComplete();
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }
}
