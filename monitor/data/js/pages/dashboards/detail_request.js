/*
Template Name: Admin Pro Admin
Author: Wrappixel
Email: niravjoshi87@gmail.com
File: js
*/
$(function () {
    "use strict";

    function objToString (obj) {
        var str = '';
        for (var p in obj) {
            str += p + '(' + obj[p] + ')';
        }
        return str;
    }

    $(document).ready(function() {
        //$.each(appConfig, function(key, value) {
        //    alert(key);
        //});
        $("#wait_message").text("Please wait for data loading");

        var iddsAPI_request_detail = appConfig.iddsAPI_request_detail;
        // iddsAPI_transform = "https://aipanda160.cern.ch:443/idds/monitor/72505/null/true/false";
        // iddsAPI_transform = "https://aipanda160.cern.ch:443/idds/monitor/null/null/true/false";

        $.getJSON(iddsAPI_request_detail, function(data){
            // $("#transforms_table_body").empty();
            $.each(data, function(i) {
                var row = data[i];

                var item_number = i + 1;
                var new_item = "<tr>";
                new_item += "<td>" + item_number + "</td>";
                new_item += '<td class="txt-oflo">' + row.request_id + '</td>';
                new_item += '<td>' + row.status + '</td>';
                // new_item += '<td class="txt-oflo">' + row.transforms + '</td>';
                new_item += '<td class="txt-oflo">' + objToString(row.transforms) + '</td>';

                new_item += '<td>' + row.created_at + '</td>';
                new_item += '<td class="txt-oflo">' + row.updated_at + '</td>';
                new_item += '<td>' + row.input_total_files + '</td>';
                new_item += '<td class="txt-oflo">' + row.input_processed_files + '</td>';
                new_item += '<td>' + row.output_total_files + '</td>';
                new_item += '<td class="txt-oflo">' + row.output_processed_files + '</td>';
                new_item += "</tr>";

                $("#requests_table_body").append(new_item);
            });
            $('table.display').DataTable();
            $("#wait_message").text("");
        });

    });

});


