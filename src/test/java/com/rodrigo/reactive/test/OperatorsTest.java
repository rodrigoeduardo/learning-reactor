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
import java.time.Duration;
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

    @Test
    public void deferOperator() throws Exception {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        // COM JUST TODOS OS VALORES DOS LOGS SERÃO IGUAIS
        // POIS O VALOR É SETADO NA HORA DA INSTANCIAÇÃO
        just.subscribe(s -> log.info("Hora (just): {}", s));
        Thread.sleep(100);
        just.subscribe(s -> log.info("Hora (just): {}", s));
        Thread.sleep(100);
        just.subscribe(s -> log.info("Hora (just): {}", s));

        // JÁ COM DEFER OS VALORES SERÃO SETADOS NA HORA DA
        // INSCRIÇÃO (SUBSCRIBE)
        defer.subscribe(s -> log.info("Hora (defer): {}", s));
        Thread.sleep(100);
        defer.subscribe(s -> log.info("Hora (defer): {}", s));
        Thread.sleep(100);
        defer.subscribe(s -> log.info("Hora (defer): {}", s));
    }

    @Test
    public void concatAndConcatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concat = Flux.concat(flux1, flux2).log();
        Flux<String> concatWith = flux1.concatWith(flux2).log();

        concat.subscribe(s -> log.info("Concat {}", s));
        concatWith.subscribe(s -> log.info("Concat with {}", s));
    }

    @Test
    public void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combineLatest = Flux.combineLatest(flux1, flux2,
                (s1, s2) -> s1 + s2)
                        .log();

        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("bc", "bd")
                .verifyComplete();
    }

    @Test
    public void mergeOperator() throws Exception {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        // É possível utilizar o mergeWith também que seria: flux1.mergeWith(flux2)
        Flux<String> mergeFlux = Flux.merge(flux1, flux2)
                .delayElements(Duration.ofMillis(200))
                .log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .verifyComplete();
    }
}
