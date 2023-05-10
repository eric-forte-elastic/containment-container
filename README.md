# containment-container
A simple FastAPI app for local contained storage of BLOB data via sqlite.


## Usage
* To build:

    ```docker build -t containment-container .```

* To run (change port as desired):

    ``` docker run -p 80:80 containment-container:latest```