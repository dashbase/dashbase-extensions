#### Dashbase Zeppelin Interpreter

[Apache Zeppelin](https://zeppelin.apache.org/) is a web-based notebook that enables interactive data analytics.

Dashbase Zeppelin Interpreter is a plugin interpreter that allows users to interface with Dashbase via Zeppelin

##### Install Zeppelin

Download Zeppelin from [here](https://zeppelin.apache.org/download.html). Make sure to download the `zeppelin-<VERSION>-bin-netinst.tgz` to avoid pulling all the interpreters.

Extract the *tgz* file and call the extracted directory: `ZEPPELIN_HOME`.

Edit the `ZEPPELIN_HOME/conf/interpreter-list` file, and add the line:

~~~~
dashbase        io.dashbase:dashbase-zeppelin:0.0.1-SNAPSHOT            Dashbase interpreter
~~~~

More details on Zeppelin installation [here](https://zeppelin.apache.org/docs/0.7.0/install/install.html).
	
##### Build and Install Dashbase Interpreter

Build: In dashbase-zeppelin:
~~~~
mvn clean install
~~~~

Install:
~~~~
export ZEPPELIN_HOME=`ZEPPELIN_DOWNLOAD_DIRECTORY`
~~~~
~~~~
./bin/install.sh
~~~~

##### Bind Dashbase Interpreter

Follow the instructions [here](https://zeppelin.apache.org/docs/0.7.0/manual/interpreterinstallation.html#3rd-party-interpreters) to bind the Dashbase interpreter.

##### Configuring Dashbase Interpreter

Configuration properties:

* dashbase.url - This This should be set against your Dashbase server. Default: localhost:7888
* dashbase.connection.timeout - Connection timeout in ms. Default: 10000
* dashbase.socket.timeout - Socket timeout in ms. Default: 60000

##### Using Dashbase Interpreter

Load Dashbase interpreter:
~~~~
%dashbase
~~~~

Help:
~~~~
Shows help message
~~~~

Get schema:
~~~~
info
~~~~

Full-text search:
~~~~
search `query_string`
~~~~

SQL query:
~~~~
sql `select_statement`
~~~~
