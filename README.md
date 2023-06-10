<a name="readme-top"></a>

# Open Data Lakehouse Stack

Project to get data and store it in a modern data lakehouse and show it in a data viz platform.

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

## About The Project
This project aims to implement a complete stack, from data extraction to presentation, using free tools, with an open datalakehouse concept and state-of-the-art technologies.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Built With
![](https://github.com/dantonbertuol/open-datalakehouse-stack/blob/237be105fdc44375498425bc1246a352cf159bf3/docs/Data%20Stack%20Project.png)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started
Some software and libraries are needed to run the project.

### Prerequisites
1. Docker
2. Python > 3.8
### Installation
1. Python - https://www.python.org/downloads/
2. Docker - https://docs.docker.com/get-docker/
3. Airbyte - https://docs.airbyte.io/
4. Minio - https://min.io/docs/minio/container/index.html
    ``` sh
    cd config/containers/minio
    docker-compose up -d
    ```
5. MySQL
    ``` sh
    cd config/containers/mysql
    docker-compose up -d
    ```
6. Airflow - https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ## Usage 

<p align="right">(<a href="#readme-top">back to top</a>)</p> 

## Roadmap

<p align="right">(<a href="#readme-top">back to top</a>)</p> -->

## Contact
Danton Bertuol - dantonjb@gmail.com
<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Fonts
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

https://docs.airbyte.com/deploying-airbyte/local-deployment

https://min.io/docs/minio/container/index.html

<p align="right">(<a href="#readme-top">back to top</a>)</p>