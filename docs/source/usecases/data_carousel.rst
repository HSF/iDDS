Data Carousel
=============

Data Carousel is a use-case of iDDS. The purpose of Data Carousel with iDDS is to release tasks quickly enough,
avoid redundant job attempts, and improve error handling.

Workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Prodsys2 submits tasks with *inputPreStaging* and *toStaging* parameters to JEDI.
2. Tasks go to *staging* state in JEDI.
3. Prodsys2 creates rules in Rucio to stage-in data from tape to disk by taking into account global share,
   the number of requests in each FTS channel, and so on, and immediately sends notifications to JEDI
   before rules are completed.
4. For each task, JEDI finds input dataset scope/name and the corresponding rule and sends a request to iDDS.
5. iDDS creates a transform object for each request to monitor the rule. Note that in this use-case, tape and disk
   file replicas are regarded as input and output collection, respectively, and thus data are not really transformed.
6. iDDS periodically checks the rule and notifies JEDI via ActiveMQ when files become available on disk
   (or disk buffer of the tape system).
7. JEDI generates jobs using only input files which have disk replicas, assign the jobs, and submits them to PanDA.
8. PanDA creates rules in Rucio to transfer input files if jobs are assigned to satellites where input data are
   unavailable.
9. Jobs get started when input files are or become available.


Advantages
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Without iDDS, JEDI is not aware of which files are on disk while input data are being staged-in,
and generates jobs using input files even if some of them have not had disk replicas yet, where there
are some issues.

* Jobs create other redundant rules in Rucio and staging requests in FTS to stage-in input files from tape to disk
  when they are available only on tape.

* Those jobs tend to silently sit in assigned state and occupy queues until they eventually time-out.
  They may prevent new jobs from being assigned to the queues, leading to unbalanced job distribution.

A naive solution is to not generate jobs until 90% of the input files become available on disk, i.e.,
not to release tasks very quickly. iDDS has solved those issues by letting JEDI use only the files with disk
replicas.


Future improvements to shorten the tails on completing tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

iDDS gives JEDI knowledge of problematic files which means that iDDS and/or JEDI can take actions if necessary.
For example, when files are stuck for long time, iDDS would make new rules to transfer them from other sites.
Possible actions and conditions to trigger them to be defined.


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
