# Production Mode Implementation README for HW4
This is description of my vision and it's implementation of the production mode for the code from HW2. 

## Project Structure Overview
Directory is structured as follows:
```
C:.
│   .dockerignore
│   .env
│   config.json
│   docker-compose.yaml
│   Dockerfile
│   README.md
│   requirements.txt
│   
└───src
        helper_functions.py
        task_1.py
        task_2.py
        task_3.py
```
## File Descriptions

- **.dockerignore**: contains directories and files that wil be ignored by the docker engine upon building the image. It is pretty standard, where all dot files will be ignored, this README, docker specific files and all __pycache__ dirs will be ignored.

- **.env**: : Just like in the development mode, .env contains environment variables:
   - **REMOTE_HOST_ADDRESS**
   - **USERNAME**
   - **PASSWORD**
   
Like I said in the other README, the difference can't be seen since I have one username and password to use to connect to remote server, but I see application in real world scenario, where we'd distinguish between dev and prod user accounts and/or databases.

- **config.json**: is used as replacement for command line arguments needed for each python script. If user sends ----config_file and --task, the script reads the configuration for the specific task. Therefore, this json contains defualt configuration, both common and task specific, which enables us to run the tasks with lesser number of arguments.

## Dockerfile
Dockerfile is used for building the docker image that will be used by containers that will run the tasks. 

Dockerfile differs from the one used for building image used in development phase. Since in production, even more focus should be on complete and efficient image and its build, and since I saw multi-stage build is the standard in builing production mode Dockerfiles, this production mode Dockerfile looks like: it is basedon full python:3.11.8,but since multi-stage build approach was used for efficiency, it only builds a Python environment using python:3.11.8, installs dependencies, and prepares executable Python scripts. Then, it uses a slim version of the Python image to create a lightweight final image by copying only the necessary dependencies and scripts for running the tasks. 

My belief is that former method results in more optimized and smaller image, suitable for production.

## docker-compose.yaml

docker-compose.yaml file is lightweight, as it contains: 
   - **version**: specifies the version of the Docker  Compose file format. 3.8 is a version compatible with Docker Engine 19.03.0+. 
   - **build**: build context is the same for each of the 3 services, and it is /dev folder.
   - **env_file**: path to the .env file containing the environment variables
   - **container_name**: name of the container
   - **restart**: container restarts automatically if it stops running.

docker-compose is more lightweight as it doesn't create 3 containers, doesn't bind local dir with the container dir (no need to save data locally), doesn't listen to any port and doesnt have default commands to run. Usage will be different due to those changes. 
The only addition is 'restart' parameter, which ensures high availability of the service.

## Running Commands

Entrypoint in the Dockerfile is python, so the commands to run the task are as follows:
```bash
      docker-compose run production-code  {TASK_NUM}.py --task {TASK_NUM} --config_file config.json
```

if user wishes to use CLI and specify the arguments directly, command is:
```bash
   docker-compose run task{ID} task_{ID}.py  --file_save_path PATH --remote_file_path PATH ...
```

Running command will print out help for CLI commands usage for specific task:
```bash
   docker-compose run task{ID} task_{ID}.py --help
```

## Interactive mode
Since entrypoint is python, to run container in interactive mode, with perhaps bash, you need to run : 
```bash
   docker-compose run --entrypoint /bin/bash -it production-code
```

## Commentary :  differences between development and production mode

I will enumerate differences:
   1. production Dockerfile has multi.stage build, ensuring copying only necessary libraries into lighter python image, resulting in lighter image size.
   2. production Dockerfile copies more specifically files, whereas development Dockerfile would collect all files in the dev dir
   3. production docker-compose.yaml is lightweight, while development one defines 3 different services (three isloated containers running one task each), ports are bind to local ports from docker container, data is stored locally as well for each task
   4.log-level is set to DEBUG in development mode, and to WARNING in production mode (more verbose logging in the development mode)
   5. zip/tar files downloaded from remote host are saved in development mode

