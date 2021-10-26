Monitor API: Examples
=============================

iDDS provides a group of monitor APIs which returns JSON outputs.

Request information
~~~~~~~~~~~~~~~~~~~~~~~~

It returns a summary information monthly(accumulated and not accumulated).

.. code-block:: python

    curl https://hostname:443/idds/monitor_request/<request_id>/<workload_id>
    curl https://hostname:443/idds/monitor_request/null/null

    {
    "month_acc_status": {
        "Finished": {
            "2021-05": 1,
            "2021-06": 1,
            "2021-07": 1,
            "2021-08": 13,
            "2021-09": 13,
            "2021-10": 28
        },
        "Total": {
            "2021-05": 8,
            "2021-06": 8,
            "2021-07": 8,
            "2021-08": 52,
            "2021-09": 54,
            "2021-10": 80
        },
    },
    "month_status": {
        "Finished": {
            "2021-05": 1,
            "2021-06": 0,
            "2021-07": 0,
            "2021-08": 12,
            "2021-09": 0,
            "2021-10": 15
        },
        "SubFinished": {
            "2021-05": 0,
            "2021-06": 0,
            "2021-07": 0,
            "2021-08": 15,
            "2021-09": 2,
            "2021-10": 1
        },
        "Total": {
            "2021-05": 8,
            "2021-06": 0,
            "2021-07": 0,
            "2021-08": 44,
            "2021-09": 2,
            "2021-10": 26
        },
    },
    "total": 80
    }


Here it returns the detail of requests.

.. code-block:: python

    curl https://hostname:443/idds/monitor/<request_id>/<workload_id>/true/false/false
    curl https://hostname:443/idds/monitor/null/null/true/false/false
    curl https://hostname:443/idds/monitor/210/null/true/false/false
    [
    {
        "created_at": "Fri, 22 Oct 2021 22:17:42 UTC",
        "input_coll_bytes": 3,
        "input_processed_files": 14,
        "input_processing_files": 0,
        "input_total_files": 14,
        "output_coll_bytes": 14,
        "output_processed_files": 14,
        "output_processing_files": 0,
        "output_total_files": 14,
        "request_id": 210,
        "status": "Finished",
        "transforms": {
            "Finished": 3
        },
        "updated_at": "Fri, 22 Oct 2021 23:55:13 UTC",
        "workload_id": 1634941059
    }
    ]

Transform information
~~~~~~~~~~~~~~~~~~~~~~~

It returns a summary information monthly(accumulated and not accumulated).

.. code-block:: python

    curl https://hostname:443/idds/monitor_transform/<request_id>/<workload_id>
    curl https://hostname:443/idds/monitor_transform/null/null
    curl https://hostname:443/idds/monitor_transform/210/null
    {
    "month_acc_processed_bytes": {
        "Finished": {
            "2021-10": 3
        },
        "Total": {
            "2021-10": 3
        }
    },
    "month_acc_processed_bytes_by_type": {
        "Processing": {
            "Finished": {
                "2021-10": 3
            },
            "Total": {
                "2021-10": 3
            }
        }
    },
    "month_acc_processed_files": {
        "Finished": {
            "2021-10": 14
        },
        "Total": {
            "2021-10": 14
        }
    },
    "month_acc_processed_files_by_type": {
        "Processing": {
            "Finished": {
                "2021-10": 14
            },
            "Total": {
                "2021-10": 14
            }
        }
    },
    "month_acc_status": {
        "Finished": {
            "2021-10": 3
        },
        "Total": {
            "2021-10": 3
        }
    },
    "month_acc_status_dict_by_type": {
        "Processing": {
            "Finished": {
                "2021-10": 3
            },
            "Total": {
                "2021-10": 3
            }
        }
    },
    "month_processed_bytes": {
        "Finished": {
            "2021-10": 3
        },
        "Total": {
            "2021-10": 3
        }
    },
    "month_processed_bytes_by_type": {
        "Processing": {
            "Finished": {
                "2021-10": 3
            },
            "Total": {
                "2021-10": 3
            }
        }
    },
    "month_processed_files": {
        "Finished": {
            "2021-10": 14
        },
        "Total": {
            "2021-10": 14
        }
    },
    "month_processed_files_by_type": {
        "Processing": {
            "Finished": {
                "2021-10": 14
            },
            "Total": {
                "2021-10": 14
            }
        }
    },
    "month_status": {
        "Finished": {
            "2021-10": 3
        },
        "Total": {
            "2021-10": 3
        }
    },
    "month_status_dict_by_type": {
        "Processing": {
            "Finished": {
                "2021-10": 3
            },
            "Total": {
                "2021-10": 3
            }
        }
    },
    "total": 3,
    "total_bytes": 17,
    "total_files": 14
    }

Here it returns the list of detailed transforms.

.. code-block:: python

    curl https://hostname:443/idds/monitor/<request_id>/<workload_id>/false/true/false
    curl https://hostname:443/idds/monitor/null/null/false/true/false
    curl https://hostname:443/idds/monitor/210/null/false/true/false
    [
    {
        "errors": {
            "msg": ""
        },
        "input_coll_bytes": 1,
        "input_processed_files": 3,
        "input_processing_files": 0,
        "input_total_files": 3,
        "output_coll_bytes": 3,
        "output_coll_name": "pseudo_output_collection#2",
        "output_coll_scope": "pseudo_dataset",
        "output_processed_files": 3,
        "output_processing_files": 0,
        "output_total_files": 3,
        "request_id": 210,
        "transform_created_at": "Fri, 22 Oct 2021 22:50:16 UTC",
        "transform_finished_at": "Fri, 22 Oct 2021 23:20:43 UTC",
        "transform_id": 2445,
        "transform_status": "Finished",
        "transform_type": "Processing",
        "transform_updated_at": "Fri, 22 Oct 2021 23:20:43 UTC",
        "transform_workload_id": 7169,
        "workload_id": 1634941059
    },
    {
        "errors": {
            "msg": ""
        },
        "input_coll_bytes": 1,
        "input_processed_files": 6,
        "input_processing_files": 0,
        "input_total_files": 6,
        "output_coll_bytes": 6,
        "output_coll_name": "pseudo_output_collection#1",
        "output_coll_scope": "pseudo_dataset",
        "output_processed_files": 6,
        "output_processing_files": 0,
        "output_total_files": 6,
        "request_id": 210,
        "transform_created_at": "Fri, 22 Oct 2021 22:17:45 UTC",
        "transform_finished_at": "Fri, 22 Oct 2021 22:46:15 UTC",
        "transform_id": 2444,
        "transform_status": "Finished",
        "transform_type": "Processing",
        "transform_updated_at": "Fri, 22 Oct 2021 22:46:15 UTC",
        "transform_workload_id": 7168,
        "workload_id": 1634941059
    },
    {
        "errors": {
            "msg": ""
        },
        "input_coll_bytes": 1,
        "input_processed_files": 5,
        "input_processing_files": 0,
        "input_total_files": 5,
        "output_coll_bytes": 5,
        "output_coll_name": "pseudo_output_collection#3",
        "output_coll_scope": "pseudo_dataset",
        "output_processed_files": 5,
        "output_processing_files": 0,
        "output_total_files": 5,
        "request_id": 210,
        "transform_created_at": "Fri, 22 Oct 2021 23:24:46 UTC",
        "transform_finished_at": "Fri, 22 Oct 2021 23:53:15 UTC",
        "transform_id": 2446,
        "transform_status": "Finished",
        "transform_type": "Processing",
        "transform_updated_at": "Fri, 22 Oct 2021 23:53:15 UTC",
        "transform_workload_id": 7170,
        "workload_id": 1634941059
    }
    ]

Processing information
~~~~~~~~~~~~~~~~~~~~~~

Here it returns a summary information monthly(accumulated and not accumulated).

.. code-block:: python

    curl https://hostname:443/idds/monitor_processing/<request_id>/<workload_id>
    curl https://hostname:443/idds/monitor_processing/null/null
    curl https://hostname:443/idds/monitor_processing/210/null
    {
    "month_acc_status": {
        "Finished": {
            "2021-10": 3
        },
        "Total": {
            "2021-10": 3
        }
    },
    "month_status": {
        "Finished": {
            "2021-10": 3
        },
        "Total": {
            "2021-10": 3
        }
    },
    "total": 3
    }

.. code-block:: python

    curl https://hostname:443/idds/monitor/<request_id>/<workload_id>/false/false/true
    curl https://hostname:443/idds/monitor/null/null/false/false/true
    curl https://hostname:443/idds/monitor/210/null/false/false/true
    [
    {
        "processing_created_at": "Fri, 22 Oct 2021 23:24:50 UTC",
        "processing_finished_at": "Fri, 22 Oct 2021 23:53:10 UTC",
        "processing_id": 1230,
        "processing_status": "Finished",
        "processing_updated_at": "Fri, 22 Oct 2021 23:53:10 UTC",
        "request_id": 210,
        "workload_id": 7170
    },
    {
        "processing_created_at": "Fri, 22 Oct 2021 22:50:20 UTC",
        "processing_finished_at": "Fri, 22 Oct 2021 23:18:40 UTC",
        "processing_id": 1229,
        "processing_status": "Finished",
        "processing_updated_at": "Fri, 22 Oct 2021 23:18:40 UTC",
        "request_id": 210,
        "workload_id": 7169
    },
    {
        "processing_created_at": "Fri, 22 Oct 2021 22:17:49 UTC",
        "processing_finished_at": "Fri, 22 Oct 2021 22:46:05 UTC",
        "processing_id": 1228,
        "processing_status": "Finished",
        "processing_updated_at": "Fri, 22 Oct 2021 22:46:05 UTC",
        "request_id": 210,
        "workload_id": 7168
    }
    ]

DAG relationships
~~~~~~~~~~~~~~~~~~~~~

Here it returns the request information with dag relationships.
It returns a list of works. For every work, it returns "work" for work data and "next_works" for its followings(if existing).

.. code-block:: python

     curl https://hostname:443/idds/monitor_request_relation/<request_id>/<workload_id>
     curl https://hostname:443/idds/monitor_request_relation/212/null

     [{
        ......
        "relation_map": [
            {
                "next_works": [
                    {
                        "work": {
                            "external_id": null,
                            "workload_id": 7175
                        }
                    }
                ],
                "work": {
                    "external_id": null,
                    "workload_id": 7174
                }
            }
        ],
        ......
     }]

If there is a loop workflow or a sub loop wookflow. The returned format will be:

.. code-block:: python
    [{
        ......
        "relation_map": [
            {
                "next_works": [
                    {
                        "work": {
                            "external_id": null,
                            "workload_id": 7175
                        },
                        "next_works": [
                            {"1": [{"work": <>, "next_works": <>}, ...],   # the first loop for a loop workflow.
                             "2": <>                                       # the second loop for a loop workflow.
                            }
                        ]
                    }
                ],
                "work": {
                    "external_id": null,
                    "workload_id": 7174
                }
            }
        ],
        ......
     }]
