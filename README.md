# oracle-jdbc-tester

[![Maven Build](https://github.com/a-p-z/oracle-jdbc-tester/actions/workflows/maven.yml/badge.svg)](https://github.com/a-p-z/oracle-jdbc-tester/actions/workflows/maven.yml)

A simple command line application to test JDBC connection to Oracle Database.

## How to run

Clone this repository and then run below Maven command to build the executable JAR file.

```
mvn clean package
```

Alternatively, download the JAR file from [release](https://github.com/aimtiaz11/jdbc-tester/releases) page. 

Execute the JAR file with the following 3 parameters with schema name, password and JDBC connection string:

```sh
java -jar target/oracle-jdbc-tester-2.0-jar-with-dependencies.jar --user <user> --password <password> --url jdbc:oracle:thin:@//<host>:<port>/<service-name> --stop-after-delay 15
```

### Secure your credentials

When running this tool ad-hoc, a good security practice would be read the DB username and password into a variable by using `read` command in Linux (or similar) and then execute the JAR file.

This prevents DB credentials being stored in `~/.bash_history`.


```sh
java -jar target/oracle-jdbc-tester-2.0-jar-with-dependencies.jar --user $DB_USER" --password "$DB_PASS" --url jdbc:oracle:thin:@//<host>:<port>/<service-name> --stop-after-delay 15
```

## How it works

The application connects to the Oracle database and executes a single SQL query: `select sysdate from dual` and prints the output. 

If it cannot connect for whatever reason, it will fail by logging an error message.

## License

(The MIT License)

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
