## Requirements
Ensure you can run Python from the command line:
```sh
py --version
```
Ensure you can run pip from the command line:
```sh
py -m pip --version
```
Ensure you can run Spark from the command line:
```sh
spark-submit --version
```
Install the requirements:
```sh
make init
```

## Installing the package locally
```sh
make dev
```

## Running the tests
```sh
make test
```

## Running events processing analysis
```sh
make run
```
This command may take some time to finish executing.


## Analysis results.
You can find the analysis results CSV file in the folder analysis_results.
