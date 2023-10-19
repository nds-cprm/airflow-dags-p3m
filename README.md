# airflow_dev
Projeto Airflow dockerizado.

O objetivo deste projeto foi construir um ambiente Docker para o Airflow com uma imagem customizada padrão, `airflow-geo`, configurada com as bibliotecas de Ciência de Dados e Geoprocessamento, que possa ser replicado a demais projetos.

Este projeto foi construído com base nos códigos utilizados para o projeto P3M-SGB  e deve ser adaptado aos demais projetos que forem utilizar a imagem `airflow-geo` como padrão. 

## Inicializando

1. __Faça o clone do repositório__

2. __Faça o fetch do repositório da base original__

    Antes de inicializar o projeto com a imagem customizada, é necessário estabelecer a estrutura inicial do Airflow, a partir da imagem. Isso devido ao processo de criação da imagem customizada se tratar da extensão de uma imagem Docker já existente.

    ```bash
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.2/docker-compose.yaml'
    ```
3. __Inicialize a estrutura padrão__

    Para criação da estrutura padrão é preciso estabelecer os diretórios para responsividade entre o host e os contêineres, como bind dos volumes, como a seguir:

    ```bash
    mkdir -p ./dags ./logs ./plugins ./config ./includes
    ```

    O diretório includes deve ser adicionado como um volume extra no `docker-compose.yaml`, como a seguir:

    ```yaml
    volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - ${AIRFLOW_PROJ_DIR:-.}/includes:/opt/airflow/includes
    ```

    Outra configuração necessária é estabelecer a variavel de ambiente do UID:

    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
    __Confirmar o endereço das portas utilizadas:__

    O Airflow por padrão possui portas de serviços já indicadas em sua configuração, se acaso estiver operando em um servidor ou mesmo uma máquina com outros serviços já alocados nessas portas é necessário fazer o bind das mesmas para evitar conflitos e garantir o funcionamento.

    Verifique as seções indicadas abaixo e altere o padrão para o endereço que te atenda.Em caso de exposição do serviço o LocalHost deve ser substituído pelo endereço da máquina.

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

    Após isso, deve-se inicializar o Airflow com a migração e estabelecimento do banco de metadados e a criação de usuários:

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

4. __Verifique se a inicialização ocorreu normalmente inicializando os conteneres__

    ```bash
    docker compose up
    ```

    Acesse, o endereço do Aiflow Webserve UI configurado, e para verificar se os serviços funcionaram, o endereço padrão é:

    http://localhost/:8080

    + __Em caso de dúvidos sobre a inicialização padrão acesse o seguinte link__:

        https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

5. __Prepare o projeto para imagem `airflow-geo`__:

    O `docker-compose.yaml` atual do seu projeto corresponde a versão da imagem padrão, para que haja correspondência com a imagem `airflow-geo` é necessário utilizar o novo arquivo disponível no repositório que será clonado, para isso siga os passos a seguir, antes de clonar o respositório.

    Derrube os serviços do Airflow no docker:

    ```bash
    docker compose down
    ```

    Apague o arquivo `docker-compose.yaml`

6. __Clonar o repositório `airflow_dev`__:

    Faça o clone do projeto no repositório que contém a estrutura para a imagem airflow-geo. Vale lembrar que a estrutura foi construida tendo como base o projeto P3M, sendo assim irá conter uma dag principal e scripts de tasks, os quais podem ser reaproveitados para o seu projeto.


7. __Confirmar as configurações do novo `docker-compose.yaml`__

    Para utilização de uma imagem extendida/customizada do Airflow no Docker, o arquivo `docker-compose.yaml` precisa ser adicionado de determinadas configurações para seu correto funcionamento e garantia de funcionalidades que são interessantes porém podem estar desabilitadas por padrão.

    + __Variáveis de ambiente__:

        Variável que desabilita o carregamento de todas as dags e demais recursos de exemplos dentro do Airflow:
        ```yaml   
        AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
        ```

        Variável que indica o caminho para o volume do conteiner do Airflow-Worker com os scripts externos que compõem as tasks:
        ```yaml 
        PYTHONPATH: airflow/includes$PYTHONPATH
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

9. __Configurações do Servidor/Máquina para uso do Broker__

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

10. __Identificação da estrutura do projeto__

Diretórios padrão do airflow e diretório de scripts:
  ./config ./dags ./logs ./plugins ./includes (Devem ser mergeados por padrão durante/após o clone do repositório com aqueles existentes e vazios em seu diretório de uso do projeto.)

Docker-compose.yaml
  Configurado para operacionalização da imagem extendida airflow-geo 

dockerfile
  Composição da imagem airflow-geo com as devidas configurações, em caso de necessidades específicas para determinado projeto em que está em uso pode ser modificado.

requirements.txt
  Bibliotecas python padrão para operacionalização de métodos geo e cd, pode ser modificado em caso de necessidades específicas.


## Add your files
Apenas para desenvolvedores autorizados no projeto.

```
cd existing_repo
git remote add origin https://gitlab.com/gabrielviterbo.ti/airflow_dev.git
git branch -M main
git push -uf origin main
```

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thank you to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README
Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.





