rm -rf target/ #delete old generated files
mvn install package  # generate new jar files
java -jar target/odyssean-pnl-1.0-SNAPSHOT-jar-with-dependencies.jar  # execution