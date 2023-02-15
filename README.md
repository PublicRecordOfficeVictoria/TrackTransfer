# Track Transfer

Copyright Public Records Office Victoria 2023

License CC BY 4.0

## What is Track Transfer?

Track Transfer helps digital archivists keep track of very large transfers (thousands
of files) received in multiple tranches. It is not a workflow, but a documentation tool
that could be used with a workflow.

**Important** Track Transfer is very much a draft tool under development. We would be
very happy to hear of any comments or suggestions for improvement. Please contact
andrew.waugh@prov.vic.gov.au

## How is Track Transfer used?

The purpose of Track Transfer is to ultimately generate a report listing every file
received in a transfer and its fate (successfully ingested into a digital repository, still being processed, or abandoned). Subsidiary reports can be generated listing only
those files that have been ingested, or abandoned, etc.

The basic process of using Track Transfer is:
* A new transfer is created in Track Transfer
* A delivery of files is received and registered into Track Transfer. Delivery of files is considered an event in the history of the processing of the file.
* Some processing is done on the files and this is documented in Track Transfer. The processing is an event. The documentation can either be a short textual description, or updating a status. Files to which this documentation are applied are either selected by being in a specific directory, or by being listed in a CSV or TSV file.
* More deliveries of files are received. The files in these subsequent deliveries can supplement, supersede, or be duplicates of already received files.
* At any time a report can be produced giving the event history of the files received in the transfer

## More information

There is a simple user manual (Track Transfer Toolset Procedure V0.1 AW 20230208).

## Java dependencies

The tools are written in Java 8.0.

## The Libraries

The tools depend on the following public domain libraries:
* h2 JDBC database(version 2.1.214)

JAR files containing these libraries can be found in ./srclib, together with the relevant license.

## The built JAR files and JavaDoc

The built JAR files and the JavaDoc can be found in ./dist (TrackTransfer.jar)

## Executing Track Transfer

Track Transfer is intended to be run as a program from the command line.

A sample BAT file can be found in the root directory (TT.bat).
