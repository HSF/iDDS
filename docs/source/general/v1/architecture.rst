Architecture
==============

The iDDS is implemented in a distributed architecture. It composed of Daemons Agents,
RESTful serivces, User Interface and External Plugins.

.. image:: ../../images/v1/architecture.png
      :alt: iDDS Architecture

Layers
~~~~~~

The iDDS is designed with abstract layers to hide the complexity of different logics
and every layer concentrates on one type of operations. It simplifies the logic of 
every layer and smooths the development and maintenance. The iDDS layers are composed of:

* ORM (Oject-Relational Mapping)
* Core layer
* API
* Restful services

Plugins
~~~~~~~

The iDDS plugin architecture is another experiment agnostic design to support new emerging
workflows. In iDDS, a base plugin class is designed and external plugins can inherit from
it to implement new workflow functions. With this structure, iDDS can support different
transforms and different data management systems.

Daemons
~~~~~~~
The iDDS daemons are active agents that orchestrate the collaborative work of the
whole system. They are for example:

* Request Daemon (Clerk) - in charge of handling requests
* Transform daemon (Transformer) - in charge of handling transforms
* Collection daemon (Transporter) - in charge of input and output collection handling
* Processing daemon (Carrier) - in charge of handling processings which do the real data transformation.
* Message daemon (Conductor) - in charge of delivering messages to ActiveMQ, which will be consumed by following workflow.

Client
~~~~~~

The client is the user interface for users to communicate the RESTful service.

* Request client
* Catalog client
* HPO(HyperParameterOptimization) client
