Active Learning(AL)
===================

Active Learning is an usecase of iDDS. The purpose of iDDS AL is to use iDDS to run some 'active learning' process to tell production system whether to continue some process.

iDDS AL  workflow
^^^^^^^^^^^^^^^^^

.. image:: ../images/v2/activelearning.png
         :alt: iDDS ActiveLearning

ActiveLearning employs iDDS DAG workflow management to define tasks.

1. It uses processing template and learning template to define the processing workflow.
2. It uses a Condition branch to control the workflow.
3. When executing, the processing template will generate a PanDA task.
4. When the PanDA task finishes, the learning template will generate a learning task which will run in iDDS internally condor cluster, to analyse the outputs of the PanDA task. The result of the learning task will decide whether to generate new PanDA tasks or to terminate.

The AL example
--------------

See examples in "User Documents" -> "iDDS RESTful client: Examples"
