# Rinha de Backend 2025

Gateway de pagamentos implementado em Rust para a Rinha de Backend 2025.

## Arquitetura

- **Load Balancer**: Distribui requisições entre instâncias do gateway usando circuit breakers
- **Gateway**: Recebe pagamentos via HTTP e os enfileira no Redis
- **Worker**: Processa pagamentos da fila usando estratégias adaptativas
- **Valkey**: Cache/fila Redis para persistir pagamentos

## Estratégias Implementadas

### Greedy
Envia todos os pagamentos para o processador padrão (mais barato).

### Quick
Estratégia adaptativa que:
- Monitora saúde dos processadores via health checks (a cada 5.5s)
- Marca processadores como falhos em caso de erro ou timeout
- Seleciona processador baseado em:
  - Se ambos falhando: não processa (aguarda)
  - Se apenas um funcionando: usa esse
  - Se ambos funcionando: distribui por peso baseado na latência inversa
  - Se ambos com latência zero: prefere o padrão

## Otimizações

- **jemalloc**: Alocador de memória otimizado
- **Atomic operations**: Estado lock-free para health checks
- **Connection pooling**: Reutilização de conexões HTTP
- **Circuit breakers**: Proteção contra processadores indisponíveis

## Execução

```bash
docker-compose up
```

Gateway disponível em `http://localhost:9999`