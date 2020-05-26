Functions and Example Workflows
===============================

iDDS(intelligent Data Delivery Service) is an intelligent Data Delivery Service. It's designed
to intelligently transform and deliver the needed data to the processing workflow in a
fine-grained approach for High Energy Physics workloads. It will not only reduce the need for
replicas, but also enable benefits such as decreasing the time period of caching transient data,
transforming expensive replicas to cheaper format data at remote sites and only cache cheaper
new data for processing. iDDS will also work to orchestrate the WorkFlow Management System (WFMS)
and the Distributed Data Management (DDM) systems to trigger them to process the data as soon as
possible. In addition, iDDS will have intelligent algorithms to adjust the lifetime of cache,
the format transformation and the delivery destination. The iDDS will increase the efficiency of
data usage, reduce storage usage for processing and speed up the processing workflow.

Functions
~~~~~~~~~

iDDS is proposed to intelligently transform and deliver the needed data to a processing workflow
in a high granularity. Here are(or will be) the main functions of iDDS:

1. Transformation on demand: Transform expensive data on demand to the format needed for processing
on a remote site and only deliver the needed data to the following processing steps. At first,
transformation on demand will avoid producing unused data. Secondly the storage-side transformation
will minimize the network load. Thirdly, instead of delivering expensive complete replicas, only 
cheaper transformed data will be delivered and cached, which will reduce local replicas or cache usage.
Last but not least, we can apply data locality knowledge and intelligence in the caching process to
promote the cache reuse.

2. Fine-grained delivery: Coordinate with the following processing steps to process data and to remove
data in a fine-grained way, without waiting for all data to be cached. It will reduce the replica usage
or cache usage and speed up the processing workflow.

3. Orchestration: Orchestration between WFMS and DDM for optimal usage of limited resources for workflows
that intersects the boundaries of data management and workflow management.

4. Intelligent: To develop intelligent algorithms as a brain to improve the scheduling in iDDS, which will
apply data locality knowledge and processing requests to trigger on-demand transformations, fine-grained
delivery and cache management to optimize the processing workflow and promote the cache reuse.


Example Workflows
~~~~~~~~~~~~~~~~~

Several workflows are proposed. Here are some examples:

1. For data carousel, instead of waiting to release jobs until all files of a dataset have been
staged-in, we can process the file that is already staged-in and remove it after it’s processe
. In this fine-grained way, we can speed up the file processing and reduce the stage-in pool usage.

2. For some analysis format data, such as DAOD, in the current computing model they are centrally
produced and stored for a long time. Some of the produced data may never be used and some of them
are used just a few times in a short period and are never touched after that. It occupies a lot of
storage space. If we can produce these analysis format data on demand with adjustable lifetime
based on the data usage frequency, we may be able to reduce the storage used by these data.

3. For the HL-LHC, we will produce a lot more data and it will be difficult for users to download
all data to local storage. However, some analysis methods such as machine learning require to load
all events and then evaluate them in many iterations. As a result, a preparation step to skim/slim
data and only download required event information to local storage is needed. If we can standardize
this step and make it reusable, we will not only make the life of physicists easier, but may also
reduce the CPU time used today to skim/slim the data many times for every analysis.

4. Some existing services developed with accumulated requirements are housed in inappropriate
components, for example, dynamic data placement service is mixed in the job management system
PanDA and the data management system Rucio, which increases the difficulty to maintain them and to
optimize the workflow.
