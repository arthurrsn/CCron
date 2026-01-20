# Ccron - Refatoração Arquitetural (Hexagonal)

Este projeto é uma refatoração do sistema **Ccron**, migrando de um script monolítico em **Streamlit** para uma **Arquitetura Hexagonal (Ports and Adapters)** desacoplada.

## 🎯 Objetivo e Funcionalidade
O sistema tem como função processar e retornar todas as informações de validação de uma obra. O foco da refatoração foi isolar a lógica de negócio da interface, permitindo que o motor de validação funcione de forma independente, sem persistência em banco de dados.

## 🛠️ Tecnologias
* **Linguagem:** Python
* **Framework:** FastAPI
* **Servidor:** Gunicorn
* **Frontend:** Streamlit (atuando como client)
* **Arquitetura:** Hexagonal (Ports and Adapters)

## 💼 Contexto
Projeto desenvolvido individualmente como estagiário para a **MRV**, dentro do ecossistema **MRV/DTI**. A refatoração buscou alinhar o sistema aos padrões arquiteturais de backend, garantindo maior testabilidade e facilidade de manutenção.

---
**Desenvolvido por [Arthur Ribeiro](https://www.linkedin.com/in/arthurrsdn)**