https://airflow.apache.org/docs/apache-airflow/2.5.1/docker-compose.yaml

AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2 

AIRFLOW_UID=50000

docker-compose up -d 

1. Create a folder materials in your Documents
2. In this folder, download the following file: docker compose file
   https://airflow.apache.org/docs/apache-airflow/2.5.1/docker-compose.yaml
3. If you right-click on the file and save it, you will end up with docker-compose.yaml.txt. Remove the .txt and keep docker-compose.yaml
4. Open your terminal or CMD and go into Documents/materials
5. Open Visual Studio Code by typing the command: code .
6. You should have something like this  
￼
7. 
8. Right click below docker-compose.yml and create a new file .env (don't forget the dot before env)
9. In this file add the following lines
    * 		AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
    * 		AIRFLOW_UID=50000
10.    and save the file 
￼
11. 
12. Go at the top bar of Visual Studio Code -> Terminal -> New Terminal 
￼
13. 
14. In your new terminal at the bottom of Visual Studio Code, type the command docker-compose up -d and hit ENTER 
￼
15. 
16. You will see many lines scrolled, wait until it's done. Docker is downloading Airflow to run it. It can take up to 5 mins depending on your connection. If Docker raises an error saying it can't download the docker image, ensure you are not behind a proxy/vpn or corporate network. You may need to use your personal connection to make it work. At the end, you should end up with something like this: 
￼
17. 
Well done, you've just installed Apache Airflow with Docker! 🎉

Open your web browser and go to localhost:8080
￼
Troubleshoots
-> If you don't see this page, make sure you have nothing already running on the port 8080
Also, go back to your terminal on Visual Studio Code and check your application with docker-compose ps
All of your "containers" should be healthy as follow:
￼
If a container is not healthy. You can check the logs with docker logs materials_name_of_the_container
Try to spot the error; once you fix it, restart Airflow with docker-compose down then docker-compose up -d
and wait until your container states move from starting to healthy.
-> If you see this error
￼
remove your volumes with docker volume prune and run docker-compose up -d again
-> If you see that airflow-init docker container has exited, that's normal :)
If you still have trouble, reach me on the Q/A with your error.
