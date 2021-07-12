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
                    // return value;
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

    function draw_chartist2(labels, data, group_types_name, chartist_name, group_types_select) {
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
                var mnum_monthly_status = Object.keys(data[key].Total).map(function(key1){
                    return data[key].Total[key1];
                });
                return mnum_monthly_status;
            });
            draw_chartist(g_types, group_types_name, chartist_name, labels, num_monthly);
        } else {
            var num_monthly = Object.keys(data[select_option].Total).map(function(key){
                return data[select_option].Total[key];
            });
            num_monthly = [num_monthly];
            draw_chartist([select_option], group_types_name, chartist_name, labels, num_monthly);
        }
    }

    function draw_chartist_status(data) {

        // alert(Object.keys(data.month_acc_status.Total));
        var labels = Object.keys(data.month_acc_status.Total);

        draw_chartist1(labels, data.month_acc_status, '#view_chartist_labels_status', '#view_chartist_status', "#view_chartist_labels_status_select");
    }

    function draw_chartist_files(data) {

        var labels = Object.keys(data.month_acc_processed_files.Total);
        draw_chartist1(labels, data.month_acc_processed_files, '#view_chartist_labels_files', '#view_chartist_files', "#view_chartist_labels_files_select");
    }

    function draw_chartist_bytes(data) {

        var labels = Object.keys(data.month_acc_processed_bytes.Total);
        draw_chartist1(labels, data.month_acc_processed_bytes, '#view_chartist_labels_bytes', '#view_chartist_bytes', "#view_chartist_labels_bytes_select");
    }

    function draw_chartist_status_type(data) {

        var labels = Object.keys(data.month_acc_status.Total);

        draw_chartist2(labels, data.month_acc_status_dict_by_type, '#view_chartist_labels_type', '#view_chartist_type', "#view_chartist_labels_type_select");
    }

    function draw_chartist_files_type(data) {

        var labels = Object.keys(data.month_acc_status.Total);
        draw_chartist2(labels, data.month_acc_processed_files_by_type, '#view_chartist_labels_files_type', '#view_chartist_files_type', "#view_chartist_labels_files_type_select");
    }

    function draw_chartist_bytes_type(data) {

        var labels = Object.keys(data.month_acc_status.Total);
        draw_chartist2(labels, data.month_acc_processed_files_by_type, '#view_chartist_labels_bytes_type', '#view_chartist_bytes_type', "#view_chartist_labels_bytes_type_select");
    }


    var sparklineLogin = function () {
        var iddsAPI_request = appConfig.iddsAPI_request;
        var iddsAPI_transform = appConfig.iddsAPI_transform;
        var iddsAPI_processing = appConfig.iddsAPI_processing;

        $.getJSON(iddsAPI_transform, function(data){ 
            $('#totaltransforms span').text(data.total);
            var month_transforms = Object.keys(data.month_status.Total).map(function(key){
                return data.month_status.Total[key];
            });
            $('#totaltransformslinedash').sparkline(month_transforms, {
                type: 'bar',
                height: '30',
                barWidth: '4',
                resize: true,
                barSpacing: '5',
                barColor: '#7460ee'
            });

            $('#totalfiles span').text(data.total_files);
            var month_files = Object.keys(data.month_processed_files.Total).map(function(key){
                return data.month_processed_files.Total[key];
            });
            $('#totalfileslinedash').sparkline(month_files, {
                type: 'bar',
                height: '30',
                barWidth: '4',
                resize: true,
                barSpacing: '5',
                barColor: '#7460ee'
            });

            $('#totalbytes span').text(Math.round(data.total_bytes/1000/1000/1000/1000));
            var month_bytes = Object.keys(data.month_processed_bytes.Total).map(function(key){
                return data.month_processed_bytes.Total[key];
            });
            $('#totalbyteslinedash').sparkline(month_bytes, {
                type: 'bar',
                height: '30',
                barWidth: '4',
                resize: true,
                barSpacing: '5',
                barColor: '#7460ee'
            });

            draw_chartist_status(data);
            draw_chartist_files(data);
            draw_chartist_bytes(data);
            draw_chartist_status_type(data);
            draw_chartist_files_type(data);
            draw_chartist_bytes_type(data);
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


