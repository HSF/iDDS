Directed Graph(DG) and Directed Acyclic Graph(DAG)
===================================================

.. image:: ../../images/v2/dag.png
         :alt: iDDS Directed Acyclic Graph(DAG)

The DG (Directed Graph) workflow management in iDDS not only supports DAG (Directed Acyclic Graph),
but also supports graphs with cycles. A DG is represented as a Workflow object which is composed of
multiple Work template objects and their relationship with condition branches, as shown below.
A Work template is a placeholder to generate new Work objects by assigning values for pre-defined parameters.
When a Work is terminated, all associated Condition branches will be evaluated and
new Work objects can be generated from their following Work templates,
with newly assigned values for pre-defined parameters.
