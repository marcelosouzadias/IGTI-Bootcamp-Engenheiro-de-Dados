![](https://redis.io/images/redis-white.png)


# Links

- [Redis](https://redis.io/)
- [Documentação](https://redis.io/documentation)
- [Downloand](https://redis.io/download)

<br>

# Instalação

```bash
sudo apt update
sudo apt install redis-server
```

Após a instalação iniciar o serviço:

```bash
sudo service redis-server start
```

Iniciar o redis
```bash
redis-cli
```

No prompt que segue, teste a conectividade com o comando ping:

```bash
ping
```

O retorno sera **PONG**

Setar um registro

```bash
set test "inserindo um registro!"
```

Rrecuperar o Registro

```bash
get test
```
```bash
output: inserindo um registro!
```