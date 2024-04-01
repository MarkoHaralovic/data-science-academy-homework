# Development Mode Implementation README for HW4
This is description of my vision and it's implementation of the development mode for the code from HW2. 

## Project structure overview
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

## File descriptions

- **.dockerignore**: contains directories and files that will be ignored by the docker engine upon building the image. It is pretty standard, where all dot files will be ignored, as well as this README, docker specific files and all __pycache__ dirs.

- **.env**: contains environment variables. In this particular case, I believe that the variables that should be environmental are variables used to connect to remote server:
   - **REMOTE_HOST_ADDRESS**
   - **USERNAME**
   - **PASSWORD**

#### Commentary on .env
Since I have only one triplet of those, .env file looks the same in the production environment, but in scenario where certain user could only have limited functionality, it would make sense to distinguish between .env files, or if we would have a smaller database for development phase.

- **config.json**: used as replacement for command line arguments needed for each python script. If user sends ----config_file and --task, the script reads the configuration for the specific task. Therefore, this json contains defualt configuration, both common and task specific, which enables us to run the tasks with lesser number of command line arguments, which will be shown later in the file.

## Dockerfile
 Dockerfile is used for building the docker image that will be used by containers that will run the tasks. 

Dockerfile creates a lightweight Python environment based on Python 3.11.8-slim, installs project dependencies from requirements.txt, copies project files into the /app directory, makes Python scripts executable, and sets python as the default command, enabling runtime customization through command-line arguments.

I chose python:3.11.8-slim due to having run the code with that python version,  and the slim variant specifically as it contains the necessary libraries for running Python applications without the additional overhead of the full image.

## docker-compose.yaml
Docker Compose file sets up a multi-container environment for running three different Python tasks (task1, task2, and task3). I will explain each decision:

   - **version**: specifies the version of the Docker  Compose file format. 3.8 is a version compatible with Docker Engine 19.03.0+. 
   - **build**: build context is the same for each of the 3 services, and it is /dev folder.
   - **env_file**: path to the .env file containing the environment variables
   - **container_name**: name of the container
   - **volumes**:  mapping the data created on specific location in the container to local folder. This should be used with caution, as the path where to save the data in the dcoker container is set in the config,json as well, and should match the second argument of the mapping in volumes.
   - **ports**:  each of the task can be accessed on local port,mapped from the port in container where the task is running.
   - **command**: command to run for each task.

## Task isolation
I have 3 docker containers, each having one functionality: to run a specific task.
I did that to isolate tasks. Each container runs in isolation, ensuring that dependencies or environment configurations for one task don't interfere with others, and for possible parallel approach to running and debugging tasks.

## Default commands 
Defualt commands are defined in the docker-compose.yaml, where after running docker-compose build, each task can be run using commands:
```bash
   docker-compose run task1
   docker-compose run task2
   docker-compose run task3
```

In each case, since the entrypoint in the Dockerfile is python, what will be run is:
```bash
   python {TASK_NUM}.py --task {TASK_NUM} --config_file config.json
```
Which assumes that in /app folder in docker container will be python scripts and config.json file.

## Alternative (not defualt) command execution
If, for any reason, one does not wish to use the json file, each task can be manually run using commands:
```bash
docker-compose run task{ID} task_{ID}.py  --file_save_path PATH --remote_file_path PATH ...
```
### Help/usage command
Same goes for all 3 tasks, where all the arguments that could be sent can be checked by running the command: 
```bash
docker-compose run task{ID} task_{ID}.py --help
```
## Interactive mode
To access an interactive mode (e.g., bash) within a task's container:
```bash
docker-compose run --entrypoint /bin/bash -it {TASK_NUM}
```

## Comments
The code is mounted in the container, data will be stored locally as well, logging is set as low as DEBUG to be more verbose. Python scripts are placed within dev directory due to their small size. If bigger, context would either not be in dev folder or I'd restructure dev and prod directory into one and rename Docker specific files.
   

   