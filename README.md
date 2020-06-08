# Cottontail DB Examples

This repository contains some simple examples as to how to use Cottontail DB using either a Kotlin or Java client. Since Cottontail DB uses [gRPC](https://grpc.io/) to communicate with its clients, other technologies can be used as well. See [gRPC List of Languages](https://grpc.io/docs/languages/) for a full list of supported platforms.

## Prerequites

You need a running instance of Cottontail DB running either on localhost or some remote host of your choice. In case you're not running Cottontail DB on the same host, make sure to adjust hostname and IP address in example script. Same holds true if you're running Cottontail DB on a different port than **1865**.

Furthermore, you will need **Java 8** or newer on the machine you're running the example project on. The project comes with a Gradle Wrapper, so no need to have Gradle installed locally.

## How to use

Checkout the repository to your local machine by using the following command.

``git clone --recurse-submodules https://github.com/vitrivr/cottontaildb-examples.git``

Make sure, that the **cottontaildb-proto** submodule was checked out too! Then generate the Java code for the ProtoBuf definitions by calling from within the project directory.

``./gradlew clean generateProto`` 

To run the Kotlin example, use the following command.

``./gradlew runKotlinExample`` 

To run the Java example, use the following command.

``./gradlew runJavaExample`` 

Of course it is also possible to open the project in your favourite IDE and use it as a Gradle project from there.

## Words on the example data

The example data was taken from the [YLI feature corpus](https://multimediacommons.wordpress.com/features/). Namely, we're using LIRE features here. You can download the full dataset for the YFCC100M collection from [here](http://multimedia-commons.s3-website-us-west-2.amazonaws.com/).