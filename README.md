# Task Queue (Go)

Фоновые задачи: HTTP API принимает задачу, сразу отвечает `202`, а пул воркеров выполняет её в фоне.

## Запуск

    go run ./cmd/server -workers 4 -queue 100

## Примеры

    # создать задачу (выполнится за 5 секунд)
    curl -i -X POST localhost:8080/tasks \
      -d '{"type":"sleep","payload":{"seconds":5}}'

    # нестабильная задача с 5 попытками
    curl -X POST localhost:8080/tasks -d '{"type":"flaky","max_attempts":5}'

    # статус
    curl localhost:8080/tasks/<id>

Статусы: `pending -> running -> done | retrying -> running ... -> failed`.

## Как устроено

- `Queue` — буферизованный канал ID; при переполнении API отвечает 503 (backpressure).
- `Service.Start` запускает N воркеров: N = лимит параллельных задач.
- Retry: экспоненциальная задержка `BaseDelay * 2^(attempt-1)` + jitter, ожидание идёт в отдельной горутине и не занимает воркера.
- Shutdown (SIGINT/SIGTERM): HTTP перестаёт принимать запросы -> очередь закрывается -> воркеры доделывают текущие и буферизованные задачи -> если за 20 с не успели, контекст обработчиков отменяется.
- Паника в обработчике превращается в ошибку и участвует в retry.
