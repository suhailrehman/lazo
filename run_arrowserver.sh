./gradlew clean
./gradlew uberJAR
java --add-opens=java.base/java.nio=ALL-UNNAMED -jar build/libs/lazo-0.1.0-uber.jar 