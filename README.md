# Freela Marketplace

Projeto de referência para um marketplace de contratação de freelancers construído com arquitetura de microsserviços em Java e Spring.

A aplicação representa um cenário em que clientes contratam freelancers para a execução de trabalhos. O núcleo do sistema é o gerenciamento dos contratos firmados entre as partes. A partir desse domínio, outros serviços mantêm informações relacionadas a notificações, reputação e auditoria.

As mudanças no ciclo de vida de um contrato são comunicadas aos demais serviços de forma **assíncrona, por eventos no Apache Kafka**, com publicação transacional (Outbox), processamento idempotente, preservação de ordem por contrato, logs centralizados e rastreamento distribuído.

## Documentação

| Documento | Conteúdo |
|---|---|
| [`docs/arquitetura.md`](docs/arquitetura.md) | visão geral, por que o fluxo é assíncrono, serviços, publicação transacional (Outbox), idempotência, tracing, correlação, particionamento e concorrência, falhas e DLT, centralização de logs, testes |
| [`docs/eventos.md`](docs/eventos.md) | contrato de comunicação: tópicos, produtor e consumidores, chave, envelope, payload e exemplo de cada evento, DLT, regras de evolução |

## Visão geral

O sistema é composto por seis aplicações Spring Boot:

- `eureka-server`: registro e descoberta dos serviços.
- `api-gateway`: ponto de entrada HTTP da aplicação.
- `contrato-service`: gerenciamento dos contratos entre clientes e freelancers; **produtor** dos eventos de contrato.
- `notificacao-service`: registro das notificações relacionadas aos contratos; **consumidor**.
- `reputacao-service`: manutenção de informações agregadas e do histórico dos freelancers; **consumidor**.
- `auditoria-service`: registro de todos os eventos do sistema; **consumidor**.

A infraestrutura local utiliza PostgreSQL, Apache Kafka (com Kafka UI), Zipkin, Loki e Grafana.

```text
                Cliente
                   │ HTTP
                   ▼
            API Gateway :8080 ◄──── Eureka :8761
                   │ HTTP
                   ▼
          contrato-service :8081 ──► contrato_db (contratos + outbox_eventos)
                   │ publica eventos (Outbox)
                   ▼
          Kafka :9092  —  contratos.eventos.v1 / contratos.eventos.v1.DLT
           │                    │                     │
           ▼                    ▼                     ▼
 notificacao-service   reputacao-service     auditoria-service
       :8082                :8083                 :8084

 Todos os serviços: traces → Zipkin :9411 · logs → Loki :3100 → Grafana :3000
```

O diagrama detalhado e o fluxo passo a passo estão em [`docs/arquitetura.md`](docs/arquitetura.md#visão-geral).

## Domínio

O domínio principal está no `contrato-service`.

Um contrato representa o vínculo entre um cliente e um freelancer para a execução de um trabalho. Cada contrato possui:

- identificador;
- cliente;
- freelancer;
- título do trabalho;
- valor;
- status;
- data de criação.

Os estados disponíveis são:

```text
ATIVO
ENTREGA_REGISTRADA
CONCLUIDO
CANCELADO
```

O fluxo de negócio previsto pelo modelo é:

```text
ATIVO
  |
  v
ENTREGA_REGISTRADA
  |
  v
CONCLUIDO
```

Um contrato ativo também pode ser cancelado (somente a partir de `ATIVO`).

O `contrato-service` utiliza uma organização inspirada em Domain-Driven Design, separando domínio, aplicação e infraestrutura.

```text
contrato-service
└── src/main/java/br/com/freela/contrato
    ├── application
    ├── domain
    │   ├── event
    │   ├── model
    │   ├── repository
    │   └── shared
    └── infrastructure
        ├── correlation
        ├── messaging
        │   └── outbox
        ├── persistence
        └── web
```

O Aggregate `Contrato` concentra as regras relacionadas às mudanças de estado e produz eventos de domínio, publicados no Kafka:

| Evento | Quando ocorre |
|---|---|
| `ContratoCriado` | criação do contrato (`ATIVO`) |
| `EntregaRegistrada` | `ATIVO → ENTREGA_REGISTRADA` |
| `ContratoConcluido` | `ENTREGA_REGISTRADA → CONCLUIDO` |
| `ContratoCancelado` | `ATIVO → CANCELADO` |

## Serviços

### contrato-service

Responsável pelo ciclo de vida dos contratos e pela publicação dos eventos (Outbox + publicador).

Porta: `8081` · Banco: `contrato_db`

```text
POST /api/contratos                      cria o contrato               → 201
POST /api/contratos/{id}/entrega         registra a entrega            → 200
POST /api/contratos/{id}/conclusao       conclui o contrato            → 200
POST /api/contratos/{id}/cancelamento    cancela o contrato            → 200
GET  /api/contratos                      lista os contratos
GET  /api/contratos/{id}                 consulta um contrato
```

Erros: `400` (requisição inválida), `404` (contrato não encontrado), `409` (transição de status inválida ou conflito de concorrência).

Exemplo de criação de contrato:

```json
{
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "22222222-2222-2222-2222-222222222222",
  "titulo": "Construção de API de pagamentos",
  "valor": 3500.00
}
```

### notificacao-service

Registra as notificações de cada evento de contrato: criação e conclusão para o freelancer, entrega para o cliente, cancelamento para os dois.

Porta: `8082` · Banco: `notificacao_db`

```text
GET /api/notificacoes?contratoId=&destinatarioId=    filtros opcionais
```

### reputacao-service

Mantém, para cada freelancer, a quantidade de contratos concluídos e cancelados, o valor total dos contratos concluídos e o histórico de eventos.

Porta: `8083` · Banco: `reputacao_db`

```text
GET /api/reputacoes
GET /api/reputacoes/{freelancerId}
GET /api/reputacoes/{freelancerId}/historico
```

### auditoria-service

Registra todos os eventos publicados: identificador do evento, tipo, contrato, data e hora do evento, `correlationId`, mensagem completa e horário de recebimento.

Porta: `8084` · Banco: `auditoria_db`

```text
GET /api/auditoria?contratoId=&eventType=&correlationId=    filtros opcionais, ordenado por data do evento
GET /api/auditoria/eventos/{eventId}
```

## API Gateway

O `api-gateway` é o ponto de entrada HTTP para os microsserviços.

Porta: `8080`

As rotas configuradas são:

| Caminho | Serviço |
|---|---|
| `/api/contratos/**` | `contrato-service` |
| `/api/notificacoes/**` | `notificacao-service` |
| `/api/reputacoes/**` | `reputacao-service` |
| `/api/auditoria/**` | `auditoria-service` |

O Gateway utiliza Eureka para localizar as instâncias dos serviços.

Também existe suporte ao header:

```text
X-Correlation-Id
```

Quando o header não é enviado pelo cliente, o Gateway gera automaticamente um UUID e o encaminha para o serviço de destino. O mesmo identificador acompanha a operação no `contrato-service`, nas mensagens do Kafka e nos serviços consumidores.

## Eureka Server

O Eureka Server mantém o registro das aplicações disponíveis no ambiente.

Porta: `8761` · Interface web: http://localhost:8761

Os microsserviços utilizam, por padrão, `http://localhost:8761/eureka/` como endereço do service registry.

## PostgreSQL

O ambiente utiliza uma única instância PostgreSQL com bancos separados para cada serviço.

```text
Host:     localhost
Porta:    5432
Usuário:  freela
Senha:    freela
```

Bancos criados durante a inicialização (`infra/postgres/init-databases.sql`):

```text
contrato_db
notificacao_db
reputacao_db
auditoria_db
```

Os serviços utilizam Hibernate com `ddl-auto: update` para criação e atualização das tabelas locais.

## Apache Kafka

O Apache Kafka é executado em modo KRaft, sem ZooKeeper.

- aplicações executadas na máquina: `localhost:9092`;
- aplicações executadas dentro da rede Docker: `kafka:19092`.

Tópicos (criados pelo `contrato-service` na inicialização, com 3 partições):

| Tópico | Uso |
|---|---|
| `contratos.eventos.v1` | todos os eventos de contrato; key = `contratoId` |
| `contratos.eventos.v1.DLT` | mensagens que não puderam ser processadas pelos consumidores após as tentativas |

O ambiente também inclui o Kafka UI: http://localhost:8090.

## Observabilidade

### Logs

Todos os serviços utilizam o mesmo formato de log no console:

```text
2026-10-13 04:24:50.252 INFO  service=notificacao-service traceId=6acddea6... spanId=... correlationId=teste-contrato-001 contratoId=2d722fd3-... eventId=057a5c60-... thread=... logger=... - evento.recebido topic=contratos.eventos.v1 partition=0 offset=7 eventType=ContratoCriado
```

Os logs também são enviados ao **Loki** (em JSON, com os campos `correlationId`, `contratoId`, `eventId`, `traceId`) e consultados no **Grafana** (http://localhost:3000 → Explore → Loki). Uma operação é acompanhada em todos os serviços com:

```logql
{service=~".+"} | json | correlationId="teste-contrato-001"
```

Outras consultas em [`docs/arquitetura.md`](docs/arquitetura.md#centralização-de-logs).

### Tracing

Os serviços enviam traces ao **Zipkin** (http://localhost:9411). Uma operação iniciada no Gateway gera um único trace com os spans do Gateway, do `contrato-service`, da publicação no Kafka e do processamento em cada consumidor. Detalhes em [`docs/arquitetura.md`](docs/arquitetura.md#rastreamento-distribuído).

## Infraestrutura local

Os serviços de infraestrutura estão definidos em:

```text
infra/docker-compose.yml
```

| Container | Imagem | Uso |
|---|---|---|
| `freela-postgres` | `postgres:16` | bancos dos serviços |
| `freela-kafka` | `apache/kafka:4.2.1` | broker (KRaft) |
| `freela-kafka-ui` | `provectuslabs/kafka-ui` | inspeção de tópicos e mensagens |
| `freela-zipkin` | `openzipkin/zipkin:3` | traces |
| `freela-loki` | `grafana/loki:3.4.2` | armazenamento de logs |
| `freela-grafana` | `grafana/grafana:11.6.0` | consulta de logs e traces (datasources Loki e Zipkin provisionados; acesso anônimo) |

Para iniciar o ambiente:

```bash
cd infra
docker compose up -d
```

Para verificar os containers:

```bash
docker compose ps
```

Para encerrar:

```bash
docker compose down
```

Os dados do PostgreSQL são mantidos em volume Docker.

Para remover também os dados persistidos:

```bash
docker compose down -v
```

## Execução das aplicações

Com a infraestrutura em execução, a partir da raiz do projeto, cada módulo pode ser iniciado separadamente com Maven. Inicie o Eureka Server primeiro.

```bash
mvn -pl eureka-server spring-boot:run
mvn -pl api-gateway spring-boot:run
mvn -pl contrato-service spring-boot:run
mvn -pl notificacao-service spring-boot:run
mvn -pl reputacao-service spring-boot:run
mvn -pl auditoria-service spring-boot:run
```

Variáveis de ambiente opcionais (padrões para execução local): `DB_URL`, `DB_USER`, `DB_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`, `EUREKA_URL`, `ZIPKIN_URL`, `LOKI_URL`.

Após a subida, o Gateway pode levar alguns segundos (atualização do cache do Eureka) para rotear um serviço recém-iniciado; nesse intervalo responde `503`.

## Portas

| Componente | Porta |
|---|---:|
| API Gateway | `8080` |
| contrato-service | `8081` |
| notificacao-service | `8082` |
| reputacao-service | `8083` |
| auditoria-service | `8084` |
| Eureka Server | `8761` |
| Kafka | `9092` |
| Kafka UI | `8090` |
| PostgreSQL | `5432` |
| Zipkin | `9411` |
| Loki | `3100` |
| Grafana | `3000` |

## Exemplos de chamadas

Com a infraestrutura e as aplicações em execução, todas as chamadas passam pelo Gateway. Cada operação gera um evento.

**1. Criar o contrato** → `ContratoCriado`

```bash
curl -i -X POST http://localhost:8080/api/contratos \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: teste-contrato-001' \
  -d '{
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "API de pagamentos",
    "valor": 3500.00
  }'
```

A resposta traz o `id` do contrato, usado nas chamadas seguintes.

**2. Registrar a entrega** → `EntregaRegistrada`

```bash
curl -i -X POST http://localhost:8080/api/contratos/{id}/entrega -H 'X-Correlation-Id: teste-contrato-002'
```

**3. Concluir** → `ContratoConcluido`

```bash
curl -i -X POST http://localhost:8080/api/contratos/{id}/conclusao -H 'X-Correlation-Id: teste-contrato-003'
```

**Cancelar** (a partir de `ATIVO`, em outro contrato) → `ContratoCancelado`

```bash
curl -i -X POST http://localhost:8080/api/contratos/{id}/cancelamento -H 'X-Correlation-Id: teste-contrato-004'
```

**Consultar o resultado nos consumidores:**

```bash
curl http://localhost:8080/api/contratos/{id}
curl "http://localhost:8080/api/auditoria?contratoId={id}"
curl "http://localhost:8080/api/notificacoes?contratoId={id}"
curl http://localhost:8080/api/reputacoes/22222222-2222-2222-2222-222222222222
curl http://localhost:8080/api/reputacoes/22222222-2222-2222-2222-222222222222/historico
```

**Acompanhar a operação:**

- mensagens no Kafka UI: http://localhost:8090 → Topics → `contratos.eventos.v1` → Messages (key = id do contrato);
- logs no Grafana: `{service=~".+"} | json | correlationId="teste-contrato-001"`;
- trace no Zipkin: http://localhost:9411 → Run Query → `api-gateway: http post`.

## Testes

```bash
mvn test
```

Requer o Docker em execução: os testes de integração sobem Kafka e PostgreSQL com Testcontainers. Cobrem as transições do contrato, a gravação transacional na outbox, a publicação no Kafka (key, headers e ordem), o consumo nos três serviços, mensagens duplicadas, a ordem dos eventos de um contrato e o envio à DLT. Detalhes em [`docs/arquitetura.md`](docs/arquitetura.md#testes-automatizados).

## Tecnologias

```text
Java 21
Spring Boot 4.1
Spring Cloud
Spring Cloud Gateway
Netflix Eureka
Spring Data JPA
Spring for Apache Kafka
PostgreSQL 16
Apache Kafka 4 (KRaft)
Micrometer Tracing (Brave) + Zipkin
Loki + Grafana (loki4j)
Testcontainers
Docker Compose
Maven
```
