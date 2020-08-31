Workflow
==============

The iDDS has implemented a workflow architecture to support new use cases. The Workflow
consists of Work(Transformation) and Condition, where Condition can be used to implement
DAG support. New use cases can be implemented with inherited Work class with developed
hook functions.

.. image:: ../../images/v2/architecture_daemon_flow.png
      :alt: iDDS Architecture

DictClass
~~~~~~~~~

DictClass is designed to automatically convert all attributes of the class to a json dictionary
and convert it back from the json dictionary to the class. The Workflow and Work classes are
inherited from this class. In this way, the Workflow and Work can be transmitted from client to
Rest service and be saved to database, in a json format. When reading, this json format can be
converted back to Workflow or Work instance, which simplies the handings in the following step.

Workflow
~~~~~~~~

A Workflow is designed to manage multiple Works(Transformations). It's a sub-class of DictClass.
With Condition supports, the Workflow can support DAG management.

Work
~~~~

A Work, a sub-class of DictClass, is a transformation. New use cases can be implemented by
inherited Work class.

1. Functions need to be overwritten for Transformer: 

        a. get_input_collections: poll DDM to get the status and metadata of the collections.
        b. get_new_input_output_maps(registered_input_output_maps): registered_input_output_maps is provided
           by iDDS with contents registered in iDDS db. This function should return maps between new inputs
           to outputs.
        c. create_processing(input_output_maps): Creating a processing with maps between inputs and outputs.
        d. syn_work_status(registered_input_output_maps): registered_input_output_maps is provided
           by iDDS with contents registered in iDDS db. It works to update the Work.status to be Transforming,
           Finished, SubFinished or Failed based on all outputs' status in registered_input_output_maps.

2. Functions need to be overwritten for Carrier: 

        a. submit_processing: Implement functions how to submit the processing.
        b. poll_processing_updates: Implement functions how to poll the processing status.


WorkflowManager
~~~~~~~~~~~~~~~

It works to automatically convert a workflow to an iDDS request(The workflow will be converted to a json dictionary
by DictClass) and send the request to iDDS Restful service.
