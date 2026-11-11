# NotificationsAPI

Host .NET 8 reservado para a simulação de notificações. Execute `dotnet run` neste diretório e acesse http://localhost:5104/health.

O perfil de desenvolvimento conecta ao RabbitMQ local. Use `RabbitMQ__Uri` para sobrescrever a conexão.

O perfil de desenvolvimento abre o Swagger em http://localhost:5104/swagger. As configurações padrão ficam em `appsettings.json`, com ajustes locais de log em `appsettings.Development.json`.

## Teste da confirmação de compra

Inicie a NotificationsAPI antes de fazer um novo pedido e aguarde o log `Consumindo a fila pagamentos.notificacoes`. Essa fila recebe os eventos da exchange `pagamentos` pela chave `pagamento.processado`, em paralelo com a fila do catálogo.

Faça um pedido no catálogo para um jogo ainda fora da biblioteca. Para `Approved`, procure `E-mail de confirmação simulado` no console e compare o `orderId` com a resposta do catálogo. Para `Rejected`, o consumidor confirma a mensagem sem registrar um e-mail.

## Kubernetes

Os manifestos deste serviço ficam em k8s/. O script de implantação, a infraestrutura e as instruções para clonar os cinco repositórios estão em https://github.com/DanteSil/fcg.Infrastructure.
