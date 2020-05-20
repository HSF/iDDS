Data Carousel
=============

Data Carousel is an usecase of iDDS. The purpose of iDDS Data Carousel is to improve the job releasing and improve the error handling.

iDDS data carousel workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Prodsys2 creates a Rucio stagein rule. Prodsys2 sends a request to iDDS with dataset scope, dataset name, rule id (optional), RSE storage src and RSE storage dest (optional).
2. iDDS propagates the request to create a transforms.

    a. If a rule id is not given, iDDS will trigger to create a rule with RSE storage src and RSE storage dest; otherwise the rule id provided by prodsys2 will be used.
    b. Monitor the file staging status of the rule.
    c. Notify panda when a file is staged-in through ActiveMQ. 


Improve job releaseing
----------------------

With current model, production system will not release jobs until 90% of the files are available in destionation site. With iDDS, files availablity will be notified to panda in very short delay in file level (instead of dataset level). So the jobs can be released as soon as the file is available.


Improve error handling
----------------------

iDDS is also developing to logics to improving the error handling. For example, when a file is stuck for long time, iDDS will trigger to transfer the file from other sites.


iDDS ATLAS data carousel status monitor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Monitor: https://bigpanda.cern.ch/idds/

1. transform_status:

    a. Finished: All files are processed.
    b. Transforming: Some files are still under processing.

2. in_status: Input dataset status. If the input dataset is not closed, it means it's still possible that some other system will add more files to the input dataset. So iDDS will monitor whehter there are new files added to this input dataset.
3. in_total_files: Total number of files in the input dataset.
4. in_processed_files: Total number of files handled by iDDS(Files that are used as input in an iDDS transformation).
5. out_status: The status of the output dataset. It will be closed(the transform will be finished) when:

    a. All input files are processed and the input dataset is closed.
    b. All output files are processed.

6. out_total_files: Total number of files in the output dataset.
7. output_processed_files: Total number of processed files in the output dataset.
