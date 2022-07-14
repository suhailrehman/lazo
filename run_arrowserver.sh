./gradlew clean
./gradlew uberJAR
java -jar build/libs/lazo-0.1.0-uber.jar --add-opens=java.base/java.nio=ALL-UNNAMED