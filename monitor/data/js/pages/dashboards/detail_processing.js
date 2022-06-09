/*
Template Name: Admin Pro Admin
Author: Wrappixel
Email: niravjoshi87@gmail.com
File: js
*/
$(function () {
    "use strict";

    $(document).ready(function() {
        // $.each(appConfig, function(key, value) {
        //     alert(key);
        // });

        $("#wait_message").text("Please wait for data loading");

        var iddsAPI_processing = appConfig.iddsAPI_processing_detail;
        // iddsAPI_processing = "https://aipanda160.cern.ch:443/idds/monitor/72505/null/false/true";
        // iddsAPI_processing = "https://aipanda160.cern.ch:443/idds/monitor/null/null/false/true";

        $.getJSON(iddsAPI_processing, function(data){
            // $("#transforms_table_body").empty();
            $.each(data, function(i) {
                var row = data[i];

                var item_number = i + 1;
                var new_item = "<tr>";
                new_item += "<td>" + item_number + "</td>";
                new_item += '<td class="txt-oflo">' + row.request_id + '</td>';
                new_item += '<td>' + row.workload_id + '</td>';
                new_item += '<td class="txt-oflo">' + row.processing_id + '</td>';
                new_item += '<td>' + row.processing_status + '</td>';
                new_item += '<td class="txt-oflo">' + row.processing_created_at + '</td>';
                new_item += '<td>' + row.processing_updated_at + '</td>';
                new_item += '<td class="txt-oflo">' + row.processing_finished_at + '</td>';
                new_item += "</tr>";

                $("#transforms_table_body").append(new_item);
            });
            $('table.display').DataTable();
            $("#wait_message").text("");
        });

    });

});


