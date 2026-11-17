# Цель и границы
Нужно 40-100 часов собрать систему из двух сервисов, которая принимает заказ, асинхронно проводит оплату и переводит заказ в PAID или CANCELLED.

Проект учебный: каждая фича существует ради одной технологии из плана развития, бизнес-логика намеренно упрощена.

# План
| Пункт плана | Критерии |
| --- | --- |
| PostgreSQL | 5 млн заказов + запросы с планами выполнения до и после индексов |
| Kafka | Два топика по три партиции, группа из двух потребителей, ретраи и DLT |
| Микросервисная архитектура | Сага-хореография, transactional outbox, idempotent consumer, database per service |
| Реактивное программирование | order-service на WebFlux + R2DBC + ReactiveKafka |
| Многопоточность Java | ??? |
| Grafana | Дашборд для CPU/RAM/HDD/Нагрузка на кафку/DB/Колличество записей в DLT и алерты |

**По желанию:**

- авторизация, UI, Kubernetes, CI/CD;
- API gateway, service discovery, config server;
- третий сервис (склад, уведомления);
- покрытие тестами сверх двух-трёх интеграционных на Testcontainers.

## Архитектура

Два сервиса общаются только событиями через Kafka, синхронных вызовов между ними нет. У каждого своя база данных в одном инстансе PostgreSQL.

```mermaid
flowchart LR
    subgraph OS["order-service (WebFlux + R2DBC, REST)"]
        O1["1. outbox → OrderCreated"]
        O4["4. статус PAID / CANCELLED"]
    end
    subgraph K["Kafka"]
        T1[["order-events<br/>3 партиции, ключ orderId"]]
        T2[["payment-events<br/>3 партиции, ключ orderId"]]
        T3[["order-events.dlt<br/>необработанные сообщения"]]
    end
    subgraph PS["payment-service × 2"]
        P2["2. оплата, идемпотентно"]
        P3["3. PaymentCompleted / Failed"]
        P5["сбой после ретраев → DLT"]
    end
    ODB[("PostgreSQL · orders_db")]
    PDB[("PostgreSQL · payments_db")]
    O1 --> T1 --> P2
    P3 --> T2 --> O4
    P5 --> T3
    OS --- ODB
    PS --- PDB
```

Поток саги:

1. Клиент вызывает `POST /api/orders`. order-service в одной транзакции пишет заказ со статусом `NEW` и строку в `outbox`.
2. Outbox-relay читает неотправленные строки и публикует `OrderCreated` в `order-events`.
3. payment-service получает событие, проверяет, что не обрабатывал его раньше, имитирует оплату и публикует `PaymentCompleted` или `PaymentFailed` в `payment-events`.
4. order-service получает результат, переводит заказ в `PAID` или `CANCELLED` и отправляет новый статус.
5. Если payment-service не смог обработать сообщение за три попытки, оно уходит в `order-events.dlt`, а заказ остаётся в `NEW`. Такие «зависшие» заказы находит отдельный запрос.

Доставка везде at-least-once: дубли возможны на каждом шаге, поэтому оба потребителя обязаны быть идемпотентными.


## Стек и структура репозитория

Один монорепозиторий, два модуля сервисов, вся инфраструктура в docker-compose.

| Что | Чем |
| --- | --- |
| Язык и сборка | Java 21+, Maven (multi-module), актуальный Spring Boot |
| order-service | Spring WebFlux, Spring Data R2DBC, Flyway, spring-kafka |
| payment-service | Spring Boot (обычный, блокирующий), JDBC, Flyway, spring-kafka |
| Брокер | Kafka, один брокер в режиме KRaft |
| База | Один контейнер PostgreSQL, две базы: `orders_db` и `payments_db` |
| Метрики | Micrometer, Spring Boot Actuator, Prometheus, Grafana |
| Нагрузка | k6 или простой скрипт |
| Тесты | JUnit 5, Testcontainers (PostgreSQL, Kafka) |

payment-service намеренно блокирующий: на защите нужно сравнить две модели исполнения на собственном коде.

```
order-processing/
  pom.xml
  docker-compose.yml
  mise.toml                  # задачи: up, down, seed, load
  order-service/
    src/main/java/...
    src/main/resources/db/migration/   # Flyway
  payment-service/
    src/main/java/...
    src/main/resources/db/migration/
  infra/
    postgres/init.sql        # создание двух баз
    prometheus/prometheus.yml
    grafana/provisioning/    # datasource + дашборд как JSON
  scripts/
    seed.sql                 # 5 млн заказов
    load.js                  # сценарий k6
  docs/
    postgres-report.md       # планы до и после
  README.md
```

В docker-compose поднимаются: `postgres`, `kafka`, `prometheus`, `grafana`, `order-service`, `payment-service` в двух экземплярах. Всё стартует одной командой, например `mise run up`. Дашборд и datasource Grafana подключаются через provisioning, руками в UI после старта ничего настраивать не нужно.
