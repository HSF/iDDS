Schematic View
==============

.. image:: ../../images/v1/schematic_view.png
      :alt: iDDS Schematic View

The iDDS is designed as a standalone experiment agnostic service. It consists of
a general Restful service to receive requests from WFMS and several running agents
in a daemon mode to process the requests.

In this model, the Restful service is used to register and query requests. It also
provides a catalog service for users to retrieve required collections or contents.
In the daemon mode, an agent Transporter works to find the input replicas from DDMs
and another agent Transformer works to transform the expensive input replicas to
desired format. When the output data is available, the agent Conductor works in a
fine-grained approach to notify consumers to process the new transformed data.
