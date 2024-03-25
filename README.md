# airflow_dev
Projeto Airflow dockerizado.

O objetivo deste projeto foi construir um ambiente Docker para o Airflow com uma imagem customizada padrão, `airflow-geo`, configurada com as bibliotecas de Ciência de Dados e Geoprocessamento, que possa ser replicado a demais projetos.

Este projeto foi construído com base nos códigos utilizados para o projeto P3M-SGB  e deve ser adaptado aos demais projetos que forem utilizar a imagem `airflow-geo` como padrão. 

## Inicializando

1. __Faça o clone do repositório airfow_dev__
  Vale lembrar que a estrutura foi construida tendo como base o projeto P3M, sendo assim irá conter uma dag principal e scripts de tasks, os quais podem ser reaproveitados para o seu projeto.

2. __Identificação da estrutura do projeto__

  + __Diretórios padrão do airflow e diretório de scripts__
    ./config ./dags ./logs ./plugins ./includes 
  
  + __docker-compose.yaml__
    Configuração de composição da imagem base padrão do airflow

  + __docker-compose_new.yaml__
    Configurado para operacionalização da imagem extendida airflow-geo 

  + __dockerfile__
    Composição da imagem airflow-geo com as devidas configurações, em caso de necessidades específicas para determinado projeto em que está em uso pode ser modificado.

  + __requirements.txt__
    Bibliotecas python padrão para operacionalização de métodos geo e cd, pode ser modificado em caso de necessidades específicas.

3. __Inicialize a estrutura padrão__

    Antes de inicializar o projeto com a imagem customizada, é necessário estabelecer a estrutura inicial do Airflow, a partir da imagem. Isso devido ao processo de criação da imagem customizada se tratar da extensão de uma imagem Docker já existente.
    
    Para criação da estrutura padrão é preciso estabelecer os diretórios/volumes para responsividade entre o host e os contêineres, então garanta a indicação dos volumes já estabelecidos no repositório no docker-compose.yaml

    O diretório includes deve ser adicionado como um volume extra no `docker-compose.yaml`, como a seguir:

    ```yaml
    volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - ${AIRFLOW_PROJ_DIR:-.}/includes:/opt/airflow/includes
    ```

    Em sistemas Linux, outra configuração necessária é estabelecer a variavel de ambiente do UID:

    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

    Em caso de sistemas operacionais como windows ou mac, você pode receber o WARNING para o UID, mas você pode seguramente ignora-lo, e criar manualmente o arquivo ".env" no diretório do projeto e adicionar o conteúdo:

    ´´´bash
    AIRFLOWUID=50000
    ´´´
    __Confirmar o endereço das portas utilizadas:__

    O Airflow por padrão possui portas de serviços já indicadas em sua configuração, se acaso estiver operando em um servidor ou mesmo uma máquina com outros serviços já alocados nessas portas é necessário fazer o bind das mesmas para evitar conflitos e garantir o funcionamento.

    Verifique as seções indicadas abaixo e altere o padrão para o endereço que te atenda.Em caso de exposição do serviço, o LocalHost deve ser substituído pelo endereço da máquina.

    ```yaml
    ...
        airflow-webserver
        ports:
          - "8080:8080"
        healthcheck:
          test: ["CMD", "curl", "--fail", "http://LocalHost/:8080/health"]
    ...    
        airflow-scheduler
          <<: *airflow-common
          command: scheduler
          healthcheck:
            test: ["CMD", "curl", "--fail", "http://LocalHost/:8974/health"]
    ...
        Flower
        ports:
            - "5555:5555"
        healthcheck:
            test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
    ``` 

    Após esses passos, é preciso inicializar o Airflow para a migração e estabelecimento do banco de metadados e a criação de usuários:

    ```bash
    docker compose up airflow-init
    ```

    No final a mensagem deve ser parecida com:

    ```
    airflow-init_1       | Upgrades done
    airflow-init_1       | Admin user airflow created
    airflow-init_1       | 2.7.2
    start_airflow-init_1 exited with code 0
    ```

4. __Verifique se a inicialização ocorreu normalmente inicializando os conteineres__

  Para verificação suba os serviços nos conteineres com o comando a seguir.

    ```bash
    docker compose up
    ```
  Durante a inicialização, no terminal podem ser registrados alguns WARNINGS, porém para os padrões das configurações pretendidas nesse projeto, estes já estão previsos e estão listados nos próximos passos  com as devidas soluções, podendo aguardar o fim do processo para lidar com estes detalhes.
  
  Acesse, o endereço do Aiflow Webserve UI configurado, e para verificar se os serviços funcionaram, a menos que tenha feito o bind no passo anterior, o endereço padrão é:

    http://localhost/:8080

    + __Em caso de dúvidos sobre a inicialização padrão acesse o seguinte link__:

        https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

5. __Prepare o projeto para imagem `airflow-geo`__:

    O arquivo denominado `docker-compose.yaml` neste momento no seu projeto corresponde a versão da imagem padrão, para que haja correspondência com a imagem `airflow-geo` é necessário utilizar o novo arquivo disponível no repositório que será clonado, para isso siga os passos a seguir.

    Derrube os serviços do Airflow no docker:

    ```bash
    docker compose down
    ```

    Renomeie o arquivo `docker-compose.yaml` para `docker-compose_old.yaml` para que não fique operacional e cause conflito.

    Agora, renomeie o arquivo `docker-compose_new.yaml` para `docker-compose.yaml` para que este se torne a versão operacional.


6. __Confirmar as configurações do novo `docker-compose.yaml`__

    Para utilização de uma imagem extendida/customizada do Airflow no Docker, o arquivo `docker-compose.yaml` precisa ser adicionado de determinadas configurações para seu correto funcionamento e garantia de funcionalidades que são interessantes porém podem estar desabilitadas por padrão. Estas configurações já foram feitas, os passos abaixo demonstram quais são e suas funções, para caso de adaptações específicas.

    + __Variáveis de ambiente__:

        Variável que desabilita o carregamento de todas as dags e demais recursos de exemplos dentro do Airflow:
        ```yaml   
        AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
        ```

        Variável que indica o caminho para o volume do conteiner do Airflow-Worker com os scripts externos que compõem as tasks:
        ```yaml 
        PYTHONPATH: :$PYTHONPATH
        ```

        Variável habilita o botão e a funcionalidade de testar a conexão com um DB ou outro serviço quando configurada pela Webserver UI:
        ```yaml 
        AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'
        ```

        Variável para reestabelecer conexão permanentemente entre o Worker (Celery) e Broker (Redis):
        ```yaml 
        BROKER_CONNECTION_RETRY_ON_STARTUP: 'true'
        ```

    + Confira novamente o bind das portas utilizadas:
        ```yaml
        ports:
          - "8888:8080"
        healthcheck:
          test: ["CMD", "curl", "--fail", "http://177.105.35.223/:8888/health"]
        
        airflow-scheduler:
          <<: *airflow-common
          command: scheduler
          healthcheck:
            test: ["CMD", "curl", "--fail", "http://177.105.35.223/:8974/health"]
        ```
        Em caso de dúvida retorne ao item referente as configurações padrão

7. __Utilizando a imagem customizada__

A imagem customizada `airflow_dev` que já foi produzida, ou seja, está com build pronto para ser utilizada, então para utiliza-la é preciso fazer o fetch da mesma no registry deste respositório e estabelece-la como imagem padrão do seu projeto.

Para fazer o fetch utilize o comando a seguir:

  ```bash
  docker run registry.gitlab.com/gabrielviterbo.ti/airflow_dev:latest
  ```

Após fazer o fetch da imagem no repositório de trabalho, redirecione ela como a imagem padrão do seu projeto no `docker-compose.ymal`

  ```yml
  image: registry.gitlab.com/gabrielviterbo.ti/airflow_dev
  ```

  Inicie os conteineres novamente.

   + __Erro de importação de DAG__

  Após inicializar, na UI do Webserver será exibido um erro de importação da DAG, referente a inexistência da conexão p3m_conn.
  Isso ocorrerá devido a DAG padrão carregada no projeto referente a outro projeto P3M.

  Não é intenção que se utilize essa DAG, ela foi utilizada para testes e para carregar o diversos códigos de tasks que a compõe para ser reaproveitados em demais projeto. 
  
  Logo para teste de inicialização de DAG construa um modelo simples utilizando alguma task existente.

8. __Configurações das DAGs__
O bom funcionamento das DAGs a serem desenvolvidas nesta imagem estão condicionadas a uma série de configurações. Abaixo, listam-se algumas configurações importantes:

   + __Adicionar o argumento que contém o diretório dos arquivos SQL__

    Caso sua DAG contenha códigos SQL, é necessário adicionar o diretório onde se localizam os arquivos SQL deste código como argumento no construtor da DAG, da seguinte maneira:

    ```python
    etl_dag = DAG (
        ...
        template_searchpath = '/opt/airflow/includes/sql',
        ...
    )
    ```

9. __Configurações em Servidor para uso do Broker__

Ao iniciar o serviço, durante o log de início no terminal, o Redis que é o Broker utilizado por essa configuração para mensageria, pode exibir o seguinte Warning:

```bash
WARNING Memory overcommit must be enabled! Without it, a background save or replication may fail under low memory condition. Being disabled, it can also cause failures without low memory condition, see https://github.com/jemalloc/jemalloc/issues/1328. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.
```
A mensagem de aviso que você está relacionada à configuração do parâmetro vm.overcommit_memory no sistema operacional, e ela é emitida pelo Redis. Esse aviso indica que o Redis recomenda que o vm.overcommit_memory seja configurado como 1 no seu sistema para evitar possíveis problemas de falta de memória durante operações de gravação em disco ou replicação, mesmo sob condições de baixa memória.

O vm.overcommit_memory é um parâmetro do kernel que controla como a alocação de memória é tratada pelo sistema. Quando configurado como 1, ele permite a alocação de memória mesmo que a quantidade total de memória física disponível e swap se esgote. Isso pode ser útil em sistemas onde a alocação de memória é frequentemente subestimada.

  + __Para configurar o vm.overcommit_memory como 1__

    Edite o arquivo /etc/sysctl.conf com um editor de texto, e editie o parâmetro:

    ```bash
        sudo nano /etc/sysctl.conf

        vm.overcommit_memory = 1

    ```
Depois de fazer uma das configurações acima, você não precisará reiniciar o sistema. O novo valor do vm.overcommit_memory terá efeito imediatamente. Certifique-se de que isso não cause problemas em outros aplicativos que executam no sistema, pois isso afetará como o kernel gerencia a memória.

Lembre-se de que essa configuração é específica para o Redis e é usada para garantir a estabilidade das operações de armazenamento em cache e persistência de dados do Redis, mesmo em condições de baixa memória.

  + __Para entender melhor a mudança__

        https://serverfault.com/questions/606185/how-does-vm-overcommit-memory-work


10. __Fazer um novo build__

Caso o projeto em que você pretende utilizar o airflow possua muitas especifidades nas pipelines e que não são atendidas pela imagem gerada neste repositório, você pode criar uma nova extensão da imagem docker airflow, sendo possível ainda reaproveitar as pré configurações do dockerfile já existente.

Para isso o primeiro passo é seguir as intruções contidas no próprio docker-compose.yaml no qual se pede para comentar a indicação direta da imagem e descomentar a parte do build.

```yaml
 &airflow-common
  # In order to add custom dependencies or upgrade provider packages you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
  #image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.2}
   build: .
```

O proximo passo é utilizar o arquivo dockerfile já existente. O modelo construtor já possui  grande parte das dependencias de sistema (linux dist-bullsey) e python (libs geo/cd) e banco de dados para trabalhar com dados Geo.

É possível adicionar outros repositórios e instalar novos pacotes e configurações no sistema, sob a estrutura do `User root`.

Para adicionar novos pacotes em python, adicione-os no arquivo requirements.txt, mas caso precise fazer alguma outra configuração específica do python, utiliza o bloco com o `User airflow`.

Faça o build da nova imagem:
  
  ```bash
  docker build -t nome.da.imagem
  ```
 Após o sucesso do build, verifique as configurações dos passos 7  a 10 desse documento.

## Add your files
Apenas para desenvolvedores autorizados no projeto.

```
cd existing_repo
git remote add origin https://gitlab.com/gabrielviterbo.ti/airflow_dev.git
git branch -M main
git push -uf origin main
```






