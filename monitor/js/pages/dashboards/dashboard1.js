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

    //ct-visits
    new Chartist.Line('#ct-visits', {
        labels: ['2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015'],
        series: [
            [5, 2, 7, 4, 5, 3, 5, 4]
            , [2, 5, 2, 6, 2, 5, 2, 4]
            , [2, 5, 2, 6, 2, 5, 3, 8]
        ]
    }, {
        top: 0,
        low: 1,
        showPoint: true,
        fullWidth: true,
        plugins: [
            Chartist.plugins.tooltip()
        ],
        axisY: {
            labelInterpolationFnc: function (value) {
                return (value / 1) + 'k';
            }
        },
        showArea: true
    });


    var chart = [chart];

    var get_idds_data = function () {
        var iddsAPI = "https://aipanda160.cern.ch:443/idds/monitor/null/null";
        $.getJSON( iddsAPI, function(data) {console.log(data);})
            .done(function( data ) {return data})
            .fail(function() {window.alert("err");});
    }

    function get_json(url){
        var data = $.getJSON(url, function(data) { return data;});
        // $.each(data, function(i) {alert(i)});
        return data;
    }

    function draw_chartist(data) {

        // alert(Object.keys(data.month_acc_status.Total));
        var labels = Object.keys(data.month_acc_status.Total);
        var statuses = Object.keys(data.month_acc_status);
        var num_monthly = Object.keys(data.month_acc_status).map(function(key){
            var mnum_monthly_status = Object.keys(data.month_acc_status[key]).map(function(key1){
                return data.month_acc_status[key][key1];
            });
            return mnum_monthly_status;
        });
        // alert(labels);
        // alert(num_monthly);
        var month_requests = Object.keys(data.month_status.Total).map(function(key){
            return data.month_status.Total[key];
        });

        // alert(statuses);
        $.each(statuses, function(i) {
            // alert(i);
            // alert(statuses[i])
            var new_li = '<li class="ps-3"><h5><i class="fa fa-circle me-1 ';
            new_li += 'view_chartist_lable_' + i + '"></i>' + statuses[i]+ '</h5>';
            new_li += '</li>';
            $('#view_chartist_labels').append(new_li);
        });

        new Chartist.Line('#view_chartist', {
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
                    return value;
                }
            },
            showArea: false
        });
    }

    var sparklineLogin = function () {
        var iddsAPI_request = appConfig.iddsAPI_request;
        var iddsAPI_transform = appConfig.iddsAPI_transform;
        var iddsAPI_processing = appConfig.iddsAPI_processing;

        $.getJSON(iddsAPI_request, function(data){
            $('#totalrequests span').text(data.total);
            draw_chartist(data);

            // alert(Object.keys(data.month_status.Total));
            var month_requests = Object.keys(data.month_status.Total).map(function(key){
                return data.month_status.Total[key];
            });
            // alert(month_requests);
            $('#totalrequestslinedash').sparkline(month_requests, {
                type: 'bar',
                height: '30',
                barWidth: '4',
                resize: true,
                barSpacing: '5',
                barColor: '#7ace4c'
            });
        });

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
        });

        $.getJSON(iddsAPI_processing, function(data){ 
            $('#totalprocessings span').text(data.total);
            var month_processings = Object.keys(data.month_status.Total).map(function(key){
                return data.month_status.Total[key];
            });
            $('#totalprocessingslinedash').sparkline(month_processings, {
                type: 'bar',
                height: '30',
                barWidth: '4',
                resize: true,
                barSpacing: '5',
                barColor: '#11a0f8'
            });
        });
    }

    var sparkResize;
    $(window).on("resize", function (e) {
        clearTimeout(sparkResize);
        sparkResize = setTimeout(sparklineLogin, 500);
    });
    sparklineLogin();

});


