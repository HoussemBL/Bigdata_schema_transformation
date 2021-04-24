mvn -B clean validate -P build-version
mvn install -P jenkins
cp target/dist/lib/spark-schema-transformer-*.jar  target/dist/lib/spark-schema-transformer.jar
