/*
jQuery(document).ready(function(){
    callAPI();
});
*/
var timeZoneOffset = getTimezoneName();
function getTimezoneName() {
    var d = new Date();
    return d.getTimezoneOffset();
}

jQuery.noConflict();
var isLive = false;
var interval;
var d1h = d2h = d3h = minuesd1h = plusd1h = false;
const regexEndTime = /^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01]) (?:[01]\d|2[0123]):(?:[012345]\d):(?:[012345]\d)$/;
const regexMoveinTime = /[-+]\d+(\.\d+)?hr/;
jQuery("#livebutton").click(function () {

	if (!isLive) {
		jQuery("#livestatus").addClass("has-text-danger");
		isLive = true;
		interval = setInterval(function () {
			var today = new Date();
			var date = today.getFullYear() + '-' + (today.getMonth() + 1) + '-' + today.getDate() + ' ' + today.getHours() + ':' + today.getMinutes() + ':' + today.getSeconds();
			
			document.getElementById('currenttime').innerHTML = date;
            
            callAPI();
		}, 5000);
	} else {
		jQuery("#livestatus").removeClass("has-text-danger");
		isLive = false;
		clearInterval(interval);
	}
});

jQuery("#d1h").click(function () {

	if (!d1h) {
		jQuery("#d1h").addClass("has-background-info has-text-white");
		d1h = true;
		jQuery("#hr-input").val("1");
		if (d2h) {
			jQuery("#d2h").removeClass("has-background-info has-text-white");
			d2h = false;
		}
		if (d3h) {
			jQuery("#d3h").removeClass("has-background-info has-text-white");
			d3h = false;
		}
	} else {
		jQuery("#d1h").removeClass("has-background-info has-text-white");
		d1h = false;
	}
});

jQuery("#d2h").click(function () {

	if (!d2h) {
		jQuery("#d2h").addClass("has-background-info has-text-white");
		d2h = true;
		jQuery("#hr-input").val("2");
		if (d1h) {
			jQuery("#d1h").removeClass("has-background-info has-text-white");
			d1h = false;
		}
		if (d3h) {
			jQuery("#d3h").removeClass("has-background-info has-text-white");
			d3h = false;
		}
	} else {
		jQuery("#d2h").removeClass("has-background-info has-text-white");
		d2h = false;
	}
});

jQuery("#d3h").click(function () {

	if (!d3h) {
		jQuery("#d3h").addClass("has-background-info has-text-white");
		d3h = true;
		jQuery("#hr-input").val("3");
		if (d1h) {
			jQuery("#d1h").removeClass("has-background-info has-text-white");
			d1h = false;
		}
		if (d2h) {
			jQuery("#d2h").removeClass("has-background-info has-text-white");
			d2h = false;
		}		
	} else {
		jQuery("#d3h").removeClass("has-background-info has-text-white");
		d3h = false;
	}
});

jQuery("#endtime-button").click(function () {

	var input = document.getElementById("endtime").value;
	// Check if input in valid yyyy-mm-dd hh:mm:ss format
	if (!input.match(regexEndTime)) {
		alert("End time must be in the format of yyyy-mm-dd hh:mm:ss");
		return;
	}
	var localEndTime = input + "Z";
	// convert input format to RFC3339: yyyy-mm-ddThh:mm:ss
	// e.g. 2019-11-30T12:49:32Z
	localEndTime = new Date(localEndTime.slice(0, 10) + "T" + localEndTime.slice(11));

	// Convert time difference
	console.log(localEndTime)
	UTCEndTime = new Date(localEndTime.getTime() + timeZoneOffset * 60 * 1000)
	console.log(UTCEndTime.toISOString())
	startEndTimeEndpoint(UTCEndTime);
});


document.getElementById("minuesd1h").onclick = function () {
	moveinTime("-", 1)
};

document.getElementById("plusd1h").onclick = function () {
	moveinTime("+", 1)
};

jQuery("#moveintime-button").click(function () {
	var input = document.getElementById("moveintime-input").value;
	// Check if input in a valid format
	if (!input.match(regexMoveinTime)) {
		alert("Move in time must be in the format of +/-(number)hr");
		return false;
	}
	sign = input.slice(0, 1);
	hr = Number(input.slice(1, -2));
	moveinTime(sign, hr);
});

function moveinTime(sign, hr) {
	var input = document.getElementById("endtime").value;

	// Check if input in valid yyyy-mm-dd hh:mm:ss format
	if (!input.match(regexEndTime)) {
		alert("End time must be in the format of yyyy-mm-dd hh:mm:ss");
		return false;
	}
	var endTime = input + "Z";
	// convert input format to RFC3339: yyyy-mm-ddThh:mm:ss
	// e.g. 2019-11-30T12:49:32
	endTime = endTime.slice(0, 10) + "T" + endTime.slice(11);

	var endTimeDate = new Date(endTime);
	var endTimeMills = endTimeDate.getTime();
	if (sign == "+") {
		newEndTimeDate = new Date(endTimeMills + hr * 60 * 60 * 1000);
	} else {
		newEndTimeDate = new Date(endTimeMills - hr * 60 * 60 * 1000);
	}
	if (startEndTimeEndpoint(newEndTimeDate)){
		newEndTimeDateStr = newEndTimeDate.toISOString()
		newEndTimeDateStr = newEndTimeDateStr.slice(0,10) + " " + newEndTimeDateStr.slice(11, 19)
		jQuery("#endtime").val(newEndTimeDateStr);
	}
}

function startEndTimeEndpoint(endTimeDate) {

	var hours = document.getElementById("hr-input").value;
	// if the duration input is empty or not in a valid number format
	if (!/\S/.test(hours) || isNaN(hours)) {
		alert("Duration hour is not a valid number");
		return false;
	} else{
		var hours = parseInt(hours);
		var milliseconds = hours * 60 * 60 * 1000;
	}
	
	var endTimeMills = endTimeDate.getTime();
	var endTime = endTimeDate.toISOString();
	var startTime = (new Date(endTimeMills - milliseconds)).toISOString();

	console.log('https://anand-bq-test-2.appspot.com/_ah/get-handlers/v1/jobs/' + startTime + '/' + endTime)
	jQuery.ajax({
		type: 'GET',
		url: 'https://anand-bq-test-2.appspot.com/_ah/get-handlers/v1/jobs/' + startTime + '/' + endTime,
		data: { get_param: 'value' },
		dataType: 'json',
		success: function (data) {
			console.log(data);
			data = data["data"];
			// calculat slot usage
			for (i in data) {
				rowData = data[i]
				const length = rowData["activeunits"].length
				rowData["slotUsage"] = new Array(length);
				if (length == 0) {
					rowData["slotUsage"].push(0);
				} else {
					for (j = 0; j < length; j++) {
						rowData["slotUsage"][j] = rowData["slotmillis"][j] * 1000000 / rowData["elapsed"][j];
					}
				}
			}
			// Load the Visualization API and the package and
			// set a callback to run when the Google Visualization API is loaded.
			google.charts.load('current', {
				'callback': function () {
					drawReservationChart(data);
					jobList(data);
				},
				'packages': ['treemap', 'corechart']
			});
		}
	});
	return true;
}

function callAPI() {
	jQuery.ajax({
		type: 'GET',
        //url: '/_ah/get-handlers/v1/jobs',
        
        url: 'https://anand-bq-test-2.appspot.com/_ah/get-handlers/v1/jobs',
		data: { get_param: 'value' },
		dataType: 'json',
		success: function (data) {
			data = data["data"];
			// calculat slot usage
			for (i in data) {
				rowData = data[i]
				const length = rowData["activeunits"].length
				rowData["slotUsage"] = new Array(length);
				if (length == 0) {
					rowData["slotUsage"].push(0);
				} else {
					for (j = 0; j < length; j++) {
						rowData["slotUsage"][j] = rowData["slotmillis"][j] * 1000000 / rowData["elapsed"][j];
					}
				}
			}
			console.log(data)
			// Load the Visualization API and the package and
			// set a callback to run when the Google Visualization API is loaded.
			google.charts.load('current', {
				'callback': function () {
					drawReservationChart(data);
					jobList(data);
				},
				'packages': ['treemap', 'corechart']
			});
		}
	});
}


function drawReservationChart(jsonData) {
	var data = google.visualization.arrayToDataTable(reservationUsage(jsonData));
	tree = new google.visualization.TreeMap(document.getElementById('chart_div'));
	tree.draw(data, {
		minColor: '#007000',
		midColor: '#FFBF00',
		maxColor: '#D2222D',
		headerHeight: 25,
		fontColor: 'black'
		, showScale: true
	});

	google.visualization.events.addListener(tree, 'select', function () {
		var selectedItem = tree.getSelection()[0];
		var size = 10;
		var value = 20;
		if (selectedItem) {
			var row = selectedItem.row;
			//alert('The user selected ' + value);
			/*
			alert(
	'' + data.getValue(row, 0) +
	', ' + data.getValue(row, 1) + 
	', ' + data.getValue(row, 2) +
	', ' + data.getValue(row, 3) + 
	', ' +
	'Datatable row: ' + row + 
	', ' + data.getColumnLabel(2) +
	' (total value of this cell and its children): ' + size + 
	', ' + data.getColumnLabel(3) + 
	': ' + value );
	*/
		}
	});
}

// reservation usage section
function reservationUsage(jsonData) {
	// initialization
	var arr = [];
	arr.push(['Level', 'Parent', 'Reserved slot (size)', 'Slot usage (color)']);
	arr.push(["all", null, 0, 0]);

	// group by reservation id
	var groupbyReservationId = groupBy(jsonData, "reservationid");
	for (var reservationId in groupbyReservationId) {
		// var slotsbyReservation = sum(groupbyReservationId[reservationId], "slots");
		var slotsbyReservation = findSlot(jsonData, reservationId);
		var slotUsagebyReservation = 0;

		var groupbyProject = groupBy(groupbyReservationId[reservationId], 'projectid');
		var slotsbyProject = slotsbyReservation / Object.keys(groupbyProject).length;

		for (var projectId in groupbyProject) {
			var groupbyUser = groupBy(groupbyProject[projectId], 'useremail');
			var slotsbyUser = slotsbyProject / Object.keys(groupbyUser).length;
			var slotUsagebyProject = 0;

			for (var email in groupbyUser) {
				var slotUsagebyUser = sum(groupbyUser[email], "slotUsage");
				slotUsagebyProject += slotUsagebyUser;
				if (slotsbyUser != 0) {
					slotUsagebyUser /= slotsbyUser;
				}
				arr.push([{v: projectId + "/" + email, f: email + 
					" (Reserved slots: " + slotsbyUser.toFixed(2) + "; slotUsage: " + 
					(slotUsagebyUser * 100).toFixed(2) + "%; number of jobs: " + 
					groupbyUser[email].length + ")"}, 
					projectId, slotsbyUser, slotUsagebyUser]);
			}			
			
			arr.push([{v: projectId, f: projectId + 
				" (Reserved slots: " + slotsbyProject.toFixed(2) + "; slotUsage: " + 
				(slotUsagebyProject / slotsbyProject * 100).toFixed(2) + "%; number of users: " + 
				Object.keys(groupbyUser).length + ")"}, 
				reservationId, 0, 0]);

			slotUsagebyReservation += slotUsagebyProject;
		}

		arr.push([{v: reservationId, f: reservationId + 
			" (Reserved slots: " + slotsbyReservation.toFixed(2) + "; slotUsage: " + 
			(slotUsagebyReservation / slotsbyReservation * 100).toFixed(2) + "%; number of projects: " + 
			Object.keys(groupbyProject).length + ")"}, 
			"all", 0, 0]);
	}
	console.log(arr)
	return arr;
}

//helper function for reservationUsage
function sum(arr, key) {
	var total = 0;
	if (key == "slotUsage") {
		for (var i = 0; i < arr.length; i++) {
			//console.log(arr);
			row = arr[i];
			// add the last slot average usage to total
			total += row[key][row[key].length - 1];
		}
	} else {
		for (var i = 0; i < arr.length; i++) {
			row = arr[i];
			total += row[key];
		}
	}
	return total;
}

//helper function for reservationUsage
function groupBy(xs, key) {
	return xs.reduce(function (rv, x) {
		(rv[x[key]] = rv[x[key]] || []).push(x);
		return rv;
	}, {});
}

function findSlot(data, reservationId) {
	for (var i = 0; i < data.length; i++) {
		row = data[i];
		if (row["reservationid"] == reservationId) {
			return row["slots"];
		}
	}
}

// job list section
function jobList(data) {
    jQuery('#job-table').DataTable().destroy();
	jQuery('#job-table').DataTable({
        "scrollX": true,
        "scrollY": "800px",
        "scrollCollapse": true,
        "paging":         false,
		"data": data,
		"columns": [
			{
				"data": "jobid",
				'createdCell': function (td, cellData, rowData, row, col) {
					jQuery(td).html('<a>' + cellData + '</a>');
					jQuery(td).click(
						function () {
							jQuery(".modal").addClass("is-active");
							drawChartLine(rowData);
						}
					);
				}
			},
			{ "data": "useremail" },
			{ "data": "projectid" },
			{ "data": "reservationid" },
			{ "data": "slotUsage",
			  "createdCell": function(td,cellData, rowData, row, col){
				slotUsage = rowData["slotUsage"]
				jQuery(td).html((slotUsage[slotUsage.length - 1]).toString());
			  }  
		  },
            { "data": "state", 
              "createdCell": function(td,cellData, rowData, row, col){
                var activeunits = rowData["activeunits"]
                , completedunits = rowData["completedunits"]
                , pendingunits = rowData["pendingunits"]
                , slotUsage = rowData["slotUsage"]
                , elapsed = rowData["elapsed"]
                , starttime = new Date(rowData["starttime"])
                , jobId = rowData["jobid"]
                , projectId = rowData["projectid"]
                , query = rowData["query"];
                var completedValue = completedunits[completedunits.length - 1];
                var totalValue = activeunits[activeunits.length - 1] 
                                + pendingunits[pendingunits.length - 1] 
                                + completedunits[completedunits.length - 1];
                  jQuery(td).html('<progress class="progress" value="'+ completedValue 
                  + '" max="' + totalValue + '">50%</progress>');
              }
           
            },
            { "data": "starttime",
              "createdCell": function(td,cellData, rowData, row, col){
                
                
                var starttime = new Date(rowData["starttime"]); 
                
                  jQuery(td).html(starttime.toLocaleString());
              }  
            },
            { "data": "slotmillis", 
              "createdCell": function(td,cellData, rowData, row, col){
                  var slotmillis = rowData["slotmillis"];
                  var fmtslotsmillis = slotmillis[slotmillis.length - 1];
                  var seconds = Math.floor((fmtslotsmillis / 1000) % 60);
                  var minutes = Math.floor((fmtslotsmillis / (1000 * 60)) % 60);
                  var hours = Math.floor((fmtslotsmillis / (1000 * 60 * 60)) % 24);
                  var days = Math.floor((fmtslotsmillis / (1000 * 60 * 60 * 24)) );
                    jQuery(td).html(days + ":" + hours + ":" + minutes + "." + seconds);
                } 
            },
		]
	});
	jQuery("#modal-close").click(function () {
		jQuery(".modal").removeClass("is-active");
	});
}

function drawChartLine(rowData) {
	var activeunits = rowData["activeunits"]
		, completedunits = rowData["completedunits"]
		, pendingunits = rowData["pendingunits"]
		, slotUsage = rowData["slotUsage"]
		, elapsed = rowData["elapsed"]
		, starttime = new Date(rowData["starttime"])
		, jobId = rowData["jobid"]
        , projectId = rowData["projectid"]
        , location = rowData["location"]
		, query = rowData["query"];


	const length = rowData["activeunits"].length
	// number of milliseconds since 1 January 1970 00:00:00
	starttime = starttime.getTime();

	for (i = 0; i < length; i++) {
		// 1 milliseconds = 1000000 Nanoseconds 
		rowData["elapsed"][i] = new Date(starttime + rowData["elapsed"][i] / 1000000)
	}

	var dataArray = [['elapsed', 'activeunits', 'pendingunits', 'completedunits', 'slotUsage']];

	if (activeunits === undefined || slotUsage === undefined ||
		pendingunits === undefined || completedunits === undefined ||
		elapsed === undefined) {
		return;
	}

	for (var n = 0; n < activeunits.length; n++) {
		dataArray.push([elapsed[n], activeunits[n], pendingunits[n], completedunits[n], slotUsage[n]]);
	}

	var data = new google.visualization.arrayToDataTable(dataArray);
   
    
	var options = {
		title: "Job performance",
		pointSize: 2,
		curveType: 'function',
		legend: 'top',
		height: 800,
		width: 1000,
		chartArea: { 'width': '70%', 'height': '70%' },
		// Gives each series an axis that matches the vAxes number below.
		series: {
			0: { targetAxisIndex: 0 },
			3: { targetAxisIndex: 3 }
		},
		hAxis: {
			title: 'Timeline (UTC-6)',
			format: 'M/d/yy hh:mm:ss',
			slantedText: true,
			gridlines: {
				count: 11
			}
		},
		vAxes: {
			// Adds titles to each axis.
			0: {
				title: 'Work Units'
			},
			3: {
				title: 'Slots utilized'
			}
		}
	};

	var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));

    chart.draw(data, options);
 
    


    document.getElementById("jobDetailId").innerHTML = jobId;
    document.getElementById("jobDetailQuery").innerHTML = query;
    document.getElementById("jobDetailProjectId").innerHTML = projectId;
    document.getElementById("jobKillCommand").innerHTML = "bq --location=" + location + " cancel " + jobId;
}


//TODO: 
// 1. Remove the hardcoding of the API service URL
// 2. Comment the CORS setting in app.yaml
// 3. Comment the CORS settings in main.go
