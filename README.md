# change-data-capture-app

Run the App
1. ` mvn clean install`
2. `docker build -f Dockerfile -t cdc-app .`
3. `docker run -p 8080:8080 -t cdc-app`

## Week 2