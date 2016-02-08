
# Environment (APP_ENV)

A variety of run environment variables are setup in the common/settings/__init__.py
module. Specifically an environment varaible called "APP_ENV" tells
common/settings/__init__.py which file to load (example: production or staging).
The local variables from that module are then jammed into the globals.


# Making calls to the REST API by python

Use common/apiv1 to make calls to the api. This deals with auth headers,
retries, error handling, etc.


# Building and Deploying Docker Containers

The minimum Docker version required is 1.5 since that is the first version to support the -f flag.

As a convention, containers are built using their subdirectory as the "project" name and
are tagged using their git committish.  So an API image might look like:

    mousera/api:35102df

Building and deploying Docker images can be done manually.
Or a script for building and deploying can be used:

    .ve/bin/python -m common.build\_and\_deploy -b -d -g api production

