# Projeto de Arquitetura On-Premise

**Em desenvolvimento 🚧**

Este projeto tem como objetivo a implementação de uma pipeline de dados fim a fim em uma arquitetura **on-premise**. A pipeline utiliza diversas ferramentas integradas para garantir uma automação robusta e eficiente no processo de **ETL (Extração, Transformação e Carga)** de dados. O foco é organizar e preparar os dados para análises posteriores utilizando a arquitetura **medalhão** com camadas **Bronze**, **Silver** e **Gold**.

## Arquitetura da Pipeline

A pipeline foi projetada com as seguintes ferramentas e tecnologias:

### 🛠 **Airflow – Orquestração e Agendamento**
O **Apache Airflow** será utilizado para orquestrar todo o fluxo de trabalho da pipeline, incluindo o agendamento das tarefas e a gestão das dependências entre elas. Ele permite a automação e escalabilidade do processo, executando o ETL conforme a frequência necessária.

### 🐍 **Python – ETL (Extração, Transformação e Carga)**
O **Python** será responsável pela extração dos dados a partir de uma página web HTTP, que contém o dataset **Financial Sample** da Microsoft (formato CSV). Após a extração, o Python irá transformar os dados e carregá-los para o banco de dados, facilitando a manipulação e organização dos dados.

### 📦 **MinIO – Armazenamento de Dados**
**MinIO** será utilizado como solução de armazenamento para os dados na camada **Bronze** da arquitetura medalhão. A camada Bronze armazena os dados brutos, antes de passar pelas etapas de transformação.

### 🗄️ **SQLite – Carregamento de Dados**
O banco de dados **SQLite** será utilizado para armazenar os dados após as transformações. Ele serve para armazenar e manipular os dados de forma leve e eficiente, sem a necessidade de um servidor de banco de dados complexo.

### 📊 **Power BI – Visualização de Dados**
O **Power BI** será utilizado para criar dashboards interativos e relatórios, permitindo visualizações dinâmicas e acessíveis sobre os dados processados.

### 🐳 **Docker Compose – Gerenciamento de Contêineres**
**Docker Compose** será utilizado para orquestrar e gerenciar os contêineres necessários para o projeto, incluindo o **Airflow**, **MinIO** e **SQLite**. O Docker Compose garante que todos os componentes da pipeline sejam inicializados corretamente, funcionando de maneira coesa e isolada.
