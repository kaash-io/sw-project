# sw-project
Analyze nasa access log to generate metrics

## How to use it?

1. Build using maven

From project directory run,

```
mvn clean install
```

It will generate **nasa-log-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar** file at target directory.

This jar accepts two positional arguments -
* url (default value is ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)
* number of top host/urls to show (default value is 5)

After jar is build run command like this (to show top 8 for example)

```
java -jar nasa-log-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 8
```

Above command will generate output at Stdout.

*Note - File from url is loaded at current directory*

2. Use Docker image

Pull docker image kaashio/nasa-log-analyzer from docker hub

https://hub.docker.com/repository/docker/kaashio/nasa-log-analyzer

Run docker container like below (change last number as needed) -

```
docker run -it --rm kaashio/nasa-log-analyzer ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 7
```

Last two arguments are optional. 