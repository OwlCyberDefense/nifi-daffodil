# NiFi Daffodil Processors

## Overview

This repository contains the source for two NiFi processors which use
[Apache Daffodil (incubating)](https://daffodil.apache.org),
an open source implementation of the [Data Format Description Language
(DFDL)](https://www.ogf.org/ogf/doku.php/standards/dfdl/dfdl) to parse/unparse
data to/from NiFi Records, which is then transformed into an Infoset based on the supplied NiFi Controller.
The two processors included are:

* DaffodilParse: Reads a FlowFile and parses the data into a NiFi Record,
which is then converted into an Infoset by a NiFi RecordSetWriter component.
* DaffodilUnparse: Reads a FlowFile containing an infoset in some form, reads it using the correct NiFi RecordReader
component and converts it into Records, and then unparses these Records to the original file format.

## Processor Properties

Each Processor has a number of configurable properties intended to be analogous
to the [CLI options](https://daffodil.apache.org/cli/) for the Daffodil tool.
Here are is a note about the __Stream__ option:

- __Stream Mode:__ This mode is disabled by default, but when enabled parsing will continue in the situation
that there is leftover data rather than routing to failure; it is simply repeated, generating a Record per parse.
If they are all successful, a Set of Records will be generated.
When using this mode for the XML Reader and Writer components, the Writer component must be configured with a
name for the Root Tag, and the Reader component must be configured with "Expect Records as Array" set to true.

And here is a note about __Tunables__ and __External Variables__:

- To add External Variables to the Processor, simply add custom key/value pairs as custom Properties when
configuring the Processor.  To add Tunables, do the same thing but add a "+" character in front of the name
of the tunable variable; i.e. +maxOccursCount would be the key and something like 10 would be the value.

## Note about the Controllers

Currently, when using the DaffodilReader and DaffodilWriter Controllers in this project, unparsing from XML is not fully supported due to the fact that the NiFi XMLReader Controller ignores empty XML elements.  Unparsing from XML is only supported for XML infosets that do not contain any empty XML elements.  However, unparsing via JSON is fully supported.

## Build Instructions

**This is a specific step for the development branch**:

Because this project depends on a snapshot version of Daffodil, in order to run `mvn install` you
must first clone the latest version of [Apache Daffodil](https://github.com/apache/incubator-daffodil)
and run 

    sbt publishM2

This step will not be necessary once Daffodil 3.0.0 is released.

Then, the following should work as expected:

This repository uses the maven build environment. To create a nar file for use
in Apache NiFi, run

    mvn install

This command will create a nar file in `nifi-daffodil-nar/target/`, which can
be copied to the Apache NiFi lib directory to install into the Apache NiFi
environment.

## License

NiFi Daffodil Processors is licensed under the Apache Software License v2.
