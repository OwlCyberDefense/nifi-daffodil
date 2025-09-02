# NiFi Daffodil Processors

## Overview

This repository contains the source for two NiFi processors which use
[Apache Daffodil](https://daffodil.apache.org),
an open source implementation of the [Data Format Description Language
(DFDL)](https://www.ogf.org/ogf/doku.php/standards/dfdl/dfdl) to parse/unparse
data to/from an XML infoset. The two processor included are:

* DaffodilParse: Reads a FlowFile and parses the data into an XML infoset
* DaffodilUnparse: Reads a FlowFile, in the form of an XML infoset, and
  unparses the infoset to the original file format

## Build Instructions

This repository uses the maven build environment. To create a nar file for use
in Apache NiFi, run

    mvn install

This command will create a nar file in `nifi-daffodil-nar/target/`, which can
be copied to the Apache NiFi lib directory to install into the Apache NiFi
environment.

## NiFi Compatibility

The minimum supported NiFi version is v1.14.0.

Support for NiFi 2.x is available beginning with NiFi Daffodil v1.19.

##  Daffodil Compatibility

Each version of the NiFi Daffodil processor depends on and is compatible with a
specific version of Daffodil. Although most schemas should work across
different versions of Daffodil, some functionality may differ. Additionally, if
the "Pre-compiled Schema" property is true, the provided saved parsers must be
built using the same version of Daffodil as supported by the NiFi Daffodil
processor. The list of NiFi Daffodil processor versions the version of Daffodil
they are compatible with are listed below.

|NiFi Daffodil version |Daffodil Version |
|----------------------|-----------------|
|1.21                  |3.11.0           |
|1.20                  |3.10.0           |
|1.19                  |3.9.0            |
|1.18                  |3.9.0            |
|1.17                  |3.8.0            |
|1.16                  |3.7.0            |
|1.15                  |3.6.0            |
|1.14                  |3.6.0            |
|1.13                  |3.5.0            |
|1.12                  |3.5.0            |
|1.11                  |3.4.0            |
|1.10                  |3.4.0            |
|1.9                   |3.4.0            |
|1.8                   |3.3.0            |
|1.7                   |3.1.0            |
|1.6                   |3.0.0            |
|1.5                   |2.5.0            |
|1.4                   |2.4.0            |
|1.3                   |2.3.0            |
|1.2                   |2.2.0            |
|1.1                   |2.1.0            |
|1.0                   |2.0.0            |

## License

NiFi Daffodil Processors is licensed under the Apache Software License v2.
