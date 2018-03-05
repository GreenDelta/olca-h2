# olca-h2
`olca-h2` provides an implementation of the openLCA `IDatabase` interface using
an in-memory instance of an [H2](http://www.h2database.com) database. In import
into `olca-h2` is ~twice as fast compared to an import into a file based Derby
database in openLCA (on a modern SSD drive). 

## Usage
Install the [openLCA core modules](https://github.com/GreenDelta/olca-modules)
and this project via [Maven](https://maven.apache.org/) (you may have to check
the version of the dependencies):

```bash
git clone <path to repo>
cd olca-h2  # check the version in the pom.xml
mvn install
``` 

After this you can use `olca-h2` e.g. as plain Maven dependency:

```xml
...
<properties>
    <olca.version>1.7.0</olca.version>
</properties>
...
<dependencies>
    <dependency>
        <groupId>org.openlca</groupId>
        <artifactId>olca-core</artifactId>
        <version>${olca.version}</version>
        <exclusions>
            <!-- Exclude Derby because we are using H2 -->
            <exclusion>
                <groupId>org.apache.derby</groupId>
                <artifactId>derby</artifactId>
                </exclusion>
            </exclusions>
    </dependency>
    <dependency>
        <groupId>org.openlca</groupId>
        <artifactId>olca-h2</artifactId>
        <version>${olca.version}</version>
    </dependency>
</dependencies>
```

```java
import org.openlca.core.database.IDatabase;
import org.openlca.h2.DB;

IDatabase db = DB.empty();
// use it like a standard openLCA database
```

## Updating
`olca-h2` has its own `schema.sql` and `persistence.xml` files. So if there
are database schema or model updates in openLCA these files need to be checked.
The persistence unit is configured to directly take a
[HikariCP](https://github.com/brettwooldridge/HikariCP) connection pool (
`transaction-type="JTA"`)
