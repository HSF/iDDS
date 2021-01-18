Active Learning(AL)
===================

Active Learning is an usecase of iDDS. The purpose of iDDS AL is to use iDDS to run some 'active learning' process to tell production system whether to continue some process.

iDDS AL  workflow
^^^^^^^^^^^^^^^^^

1. User creates a panda processing task, say task1.
2. User defines a learning task and submitted it to iDDS (run in iDDS local cluster), say task2.
3. User defines the contion between task1 -> task2: When task1 is terminated, the condition function will be called. If it returns True, the next task task2 will be started.
4. User defines the contion between task2 -> task1: When task2 is terminated, this condition function will be called.
5. To trigger next task, if the current task returns parameters, these parameters will be used as inputs to trigger the next task.

The AL example
--------------

See examples in "User Documents" -> "iDDS RESTful client: Examples"
