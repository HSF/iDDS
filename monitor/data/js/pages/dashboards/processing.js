/*
Template Name: Admin Pro Admin
Author: Wrappixel
Email: niravjoshi87@gmail.com
File: js
*/
$(function () {
    "use strict";
    // ============================================================== 
    // Newsletter
    // ============================================================== 

    function draw_chartist(group_types, group_types_name, chartist_name, labels, num_monthly) {

        $(group_types_name).empty();
        $.each(group_types, function(i) {
            var new_li = '<li class="ps-3"><h5><i class="fa fa-circle me-1 ';
            new_li += 'view_chartist_lable_' + i + '"></i>' + group_types[i]+ '</h5>';
            new_li += '</li>';
            $(group_types_name).append(new_li);
        });

        new Chartist.Line(chartist_name, {
            labels: labels,
            series: num_monthly
            },
            {top: 0,
             low: 1,
             showPoint: true,
             fullWidth: true,
             plugins: [
                 Chartist.plugins.tooltip()
             ],
            axisY: {
                labelInterpolationFnc: function (value) {
                    // return (value / 1) + 'k';
                    if (value > 1000000000000000) {
                        return (value / 1000000000000000) + 'P';
                    } else if (value > 1000000000000) {
                        return (value / 1000000000000) + 'T';
                    } else if (value > 1000000000) {
                        return (value / 1000000000) + 'G';
                    } else if (value > 1000000) {
                        return (value / 1000000) + 'M';
                    } else if (value > 1000) {
                        return (value / 1000) + 'K';
                    } else {
                        return value;
                    }
                }
            },
            showArea: false
        });
    }

    function draw_chartist1(labels, data, group_types_name, chartist_name, group_types_select) {
        var g_types = Object.keys(data);

        var select_option = $(group_types_select + " option").filter(':selected').text();
        select_option = $.trim(select_option);

        $(group_types_select).empty();
        if (select_option === 'All'){
            var all_option = '<option value="All" selected="selected">All</option>';
            $(group_types_select).append(all_option);
        } else {
            var all_option = '<option value="All">All</option>';
            $(group_types_select).append(all_option);
        }
        $.each(g_types, function(i) {
            if (select_option === g_types[i]){
                var new_opt = '<option value="' + g_types[i] + '" selected="selected">' + g_types[i] + '</option>';
                $(group_types_select).append(new_opt);
            } else {
                var new_opt = '<option value="' + g_types[i] + '">' + g_types[i] + '</option>';
                $(group_types_select).append(new_opt);
            }
        });

        if (select_option === 'All'){
            var num_monthly = Object.keys(data).map(function(key){
                var mnum_monthly_status = Object.keys(data[key]).map(function(key1){
                    return data[key][key1];
                });
                return mnum_monthly_status;
            });
            draw_chartist(g_types, group_types_name, chartist_name, labels, num_monthly);
        } else {
            var num_monthly = Object.keys(data[select_option]).map(function(key){
                return data[select_option][key];
            });
            num_monthly = [num_monthly];
            draw_chartist([select_option], group_types_name, chartist_name, labels, num_monthly);
        }
    }


    function draw_chartist_status(data) {

        // alert(Object.keys(data.month_acc_status.Total));
        var labels = Object.keys(data.month_acc_status.Total);
        draw_chartist1(labels, data.month_acc_status, '#view_chartist_labels', '#view_chartist', "#view_chartist_labels_select");
   }

    var sparklineLogin = function () {
        var iddsAPI_request = appConfig.iddsAPI_request;
        var iddsAPI_transform = appConfig.iddsAPI_transform;
        var iddsAPI_processing = appConfig.iddsAPI_processing;

        $.getJSON(iddsAPI_processing, function(data){
            $('#totalprocessings span').text(data.total);
            draw_chartist_status(data);

            // alert(Object.keys(data.month_status.Total));
            var month_requests = Object.keys(data.month_status.Total).map(function(key){
                return data.month_status.Total[key];
            });
            // alert(month_requests);
            $('#totalprocessingslinedash').sparkline(month_requests, {
                type: 'bar',
                height: '30',
                barWidth: '4',
                resize: true,
                barSpacing: '5',
                barColor: '#7ace4c'
            });
        });
    }

    var sparkResize;
    $(window).on("resize", function (e) {
        clearTimeout(sparkResize);
        sparkResize = setTimeout(sparklineLogin, 500);
    });
    sparklineLogin();

    $("select").on("change", function() {
        // alert( "Handler for .change() called." );
        sparklineLogin();
    });

});


