# NotificationsAPI

Serviço .NET 8 que consome eventos de cadastro e pagamento e simula e-mails nos logs. Para execução direta, instale .NET 8 SDK, disponibilize o RabbitMQ, execute `dotnet run` neste diretório e acesse http://localhost:5104/health.

O perfil de desenvolvimento conecta ao RabbitMQ local. Use `RabbitMQ__Uri` para sobrescrever a conexão.

```powershell
$env:RabbitMQ__Uri = 'amqp://guest:guest@localhost:5672/%2f'
dotnet run
```

Ajuste a URI às credenciais do ambiente. Compose e Kubernetes injetam a URI com usuário `fcg` e senha do `.env` da infraestrutura.

O perfil de desenvolvimento abre o Swagger em http://localhost:5104/swagger. As configurações padrão ficam em `appsettings.json`, com ajustes locais de log em `appsettings.Development.json`.

## Teste da confirmação de compra

Antes do cadastro, aguarde também o log `Consumindo a fila usuarios.notificacoes`. Cadastre um usuário pela UsersAPI e confira `E-mail de boas-vindas simulado`. Essa fila recebe eventos da exchange `usuarios` pela chave `usuario.criado`.

Inicie a NotificationsAPI antes de fazer um novo pedido e aguarde o log `Consumindo a fila pagamentos.notificacoes`. Essa fila recebe os eventos da exchange `pagamentos` pela chave `pagamento.processado`, em paralelo com a fila do catálogo.

Faça um pedido no catálogo para um jogo ainda fora da biblioteca. Para `Approved`, procure `E-mail de confirmação simulado` no console e compare o `orderId` com a resposta do catálogo. Para `Rejected`, o consumidor confirma a mensagem sem registrar um e-mail.

## Kubernetes

Os manifestos deste serviço ficam em k8s/. O script de implantação, a infraestrutura e as instruções para clonar os cinco repositórios estão em https://github.com/DanteSil/fcg.Infrastructure.
